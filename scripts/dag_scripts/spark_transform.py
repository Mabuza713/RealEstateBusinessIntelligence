"""Spark: clean -> staging (parquet). Bez wymiarow i faktow - tylko przygotowanie danych."""

import os
from functools import reduce
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType, IntegerType

POI_FILES = [
    ("cafe", "all_cafes.csv"),
    ("parking", "all_parkings.csv"),
    ("bus_stop", "all_bus_stops.csv"),
]

APT_AMENITY_COLS = [
    "hasParkingSpace",
    "hasBalcony",
    "hasElevator",
    "hasSecurity",
    "hasStorageRoom",
]

APT_TEXT_COLS = ["buildingMaterial", "condition", "ownership", "type"]


# --- helpers -----------------------------------------------------------------
def _root() -> str:
    candidates = (
        os.environ.get("DATA_ROOT"),
        "/opt/airflow/data",
        str(Path(__file__).resolve().parents[2] / "data"),
    )
    for path in candidates:
        if path and Path(path).exists():
            return path
    raise FileNotFoundError("Brak katalogu data")


def _union(dfs):
    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), dfs)


def _csv(spark, path, sep=","):
    return spark.read.option("header", True).option("sep", sep).csv(path)


def _glob_one(folder, pattern):
    return str(next(Path(folder).glob(pattern)))


def _write(df, path):
    df.write.mode("overwrite").parquet(path)


def _norm_city(col):
    lowered = F.lower(F.trim(col))
    return F.translate(lowered, "ąćęłńóśźż", "acelnoszz")


def _missing_label(col, label="brak informacji"):
    empty = col.isNull() | (F.trim(col.cast("string")) == "")
    return F.when(empty, F.lit(label)).otherwise(F.trim(col.cast("string")))


def _round_num(col, scale=2):
    c = F.col(col) if isinstance(col, str) else col
    return F.round(c.cast("double"), scale)


# --- staging: apartments (Dim_Lokal, Dim_Budynek, Dim_Czas) ------------------
def _get_latest_month_from_db(spark):
    pg_user = os.environ.get("POSTGRES_USER", "postgres")
    pg_password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    pg_db = os.environ.get("POSTGRES_DB", "postgres")
    pg_host = os.environ.get("POSTGRES_HOST", "postgres")
    pg_port = os.environ.get("POSTGRES_PORT", "5432")

    url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    # Sprawdzamy prod.dim_czas
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", "(SELECT MAX(source_date) as max_date FROM prod.dim_czas) as tmp") \
            .option("user", pg_user) \
            .option("password", pg_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        res = df.collect()
        if res and res[0]["max_date"]:
            return str(res[0]["max_date"])[:7]
    except Exception:
        pass

    # Sprawdzamy stg.apartments
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", "(SELECT MAX(source_date) as max_date FROM stg.apartments) as tmp") \
            .option("user", pg_user) \
            .option("password", pg_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        res = df.collect()
        if res and res[0]["max_date"]:
            return str(res[0]["max_date"])[:7]
    except Exception:
        pass

    return None


def _stage_apartments(spark, src_dir):
    sources = [
        _csv(spark, f"{src_dir}/all_apartments_{kind}.csv").withColumn("listing_type", F.lit(kind))
        for kind in ("sell", "rent")
    ]
    df = _union(sources)

    df = (
        df
        .withColumn("city_norm", _norm_city(F.col("city")))
        .withColumn("source_date", F.to_date("source_date", "yyyy-MM"))
    )

    # Filtracja przyrostowa na poziomie transformacji
    force_full_load = os.environ.get("FORCE_FULL_LOAD", "false").lower() == "true"
    if not force_full_load:
        latest_month = _get_latest_month_from_db(spark)
        if latest_month:
            print(f"[Transform] Znaleziono ostatni miesiąc w bazie: {latest_month}. Filtruję tylko nowsze wiersze.")
            df = df.filter(F.col("source_date") > F.to_date(F.lit(f"{latest_month}-01")))

    df = (
        df
        .withColumn("source_year", F.year("source_date"))
        .withColumn("source_month", F.month("source_date"))
        .withColumn("square_meters", F.col("squareMeters").cast(DecimalType(10, 2)))
        .withColumn("price", F.col("price").cast(DecimalType(15, 2)))
        .withColumn(
            "price_per_sqm",
            F.when(F.col("squareMeters") > 0, F.col("price") / F.col("squareMeters")).otherwise(F.lit(None)),
        )
        .withColumn("floor", F.coalesce(F.col("floor").cast(IntegerType()), F.lit(-1)))
        .withColumn("build_year", F.col("buildYear").cast(IntegerType()))
        .withColumn("rooms", F.col("rooms").cast(IntegerType()))
        .withColumn("floorCount", F.col("floorCount").cast(IntegerType()))
        .withColumn("poiCount", F.col("poiCount").cast(IntegerType()))
        .withColumn("latitude", _round_num("latitude", 6))
        .withColumn("longitude", _round_num("longitude", 6))
        .withColumn("centre_distance", _round_num("centreDistance"))
    )

    for col in APT_TEXT_COLS:
        if col in df.columns:
            df = df.withColumn(col, _missing_label(F.col(col)))

    for col in APT_AMENITY_COLS:
        if col in df.columns:
            df = df.withColumn(col, _missing_label(F.col(col)))

    distance_cols = [c for c in df.columns if c.endswith("Distance")]
    for col in distance_cols:
        df = df.withColumn(col, _round_num(col))

    return (
        df
        .filter(
            F.col("id").isNotNull() &
            F.col("city").isNotNull() &
            F.col("squareMeters").isNotNull() & (F.col("squareMeters") >= 10) & (F.col("squareMeters") <= 300) &
            F.col("price").isNotNull() & (F.col("price") > 0) &
            F.col("latitude").isNotNull() &
            F.col("longitude").isNotNull() &
            F.col("source_date").isNotNull()
        )
    )


# --- staging: demografia (Dim_Demografia) ------------------------------------
def _stage_demografia(spark, src_dir):
    return (
        _csv(spark, _glob_one(src_dir, "baza_bi_miasta_*.csv"))
        .withColumn("city_norm", _norm_city("Glowne_Miasto"))
        .withColumn("data_date", F.to_date("Data"))
        .withColumn("populacja_ogolna", F.col("Populacja_Ogolna").cast(IntegerType()))
        .withColumn("populacja_mezczyzni", F.col("Populacja_Mezczyzni").cast(IntegerType()))
        .withColumn("populacja_kobiety", F.col("Populacja_Kobiety").cast(IntegerType()))
        .withColumn("Zarejestrowani_Bezrobotni", F.col("Zarejestrowani_Bezrobotni").cast(IntegerType()))
        .withColumn("Przecietne_Wynagrodzenie_Brutto", F.col("Przecietne_Wynagrodzenie_Brutto").cast(DecimalType(10, 2)))
        .withColumn("Dochody_Wlasne_JST", F.col("Dochody_Wlasne_JST").cast(DecimalType(10, 2)))
        .filter(
            F.col("Miasto_GUS").isNotNull() & (F.trim(F.col("Miasto_GUS")) != "") &
            F.col("Glowne_Miasto").isNotNull() & (F.trim(F.col("Glowne_Miasto")) != "") &
            F.col("Data").isNotNull() &
            (
                (F.col("populacja_ogolna").isNotNull() & (F.col("populacja_ogolna") > 0)) |
                F.col("Przecietne_Wynagrodzenie_Brutto").isNotNull() |
                F.col("Dochody_Wlasne_JST").isNotNull() |
                F.col("Zarejestrowani_Bezrobotni").isNotNull()
            )
        )
        .dropDuplicates(["Miasto_GUS", "Data"])
    )


# --- staging: POI (źródło Dim_Infrastruktura) --------------------------------
def _stage_poi(spark, src_dir):
    sources = [
        _csv(spark, f"{src_dir}/{filename}", ";")
        .withColumn("poi_type", F.lit(poi_type))
        for poi_type, filename in POI_FILES
    ]
    df = _union(sources)

    return (
        df
        .withColumn("city_norm", _norm_city("City"))
        .withColumn("poi_name", _missing_label(F.col("Name")))
        .withColumn("street", _missing_label(F.col("Street")))
        .withColumn("street_number", _missing_label(F.col("Number")))
        .withColumn("latitude", _round_num("LAT", 6))
        .withColumn("longitude", _round_num("LON", 6))
        .filter(
            F.col("City").isNotNull() & (F.trim(F.col("City")) != "") &
            F.col("poi_type").isNotNull() &
            F.col("latitude").isNotNull() & (F.col("latitude") >= 0) &
            F.col("longitude").isNotNull() & (F.col("longitude") >= 0)
        )
    )

# --- database ----------------------------------------------------------------
_DB_COLUMNS = {
    "apartments": [
        "id", "city", "type", "squaremeters", "rooms", "floor", "floorcount", "buildyear",
        "latitude", "longitude", "centredistance", "poicount", "schooldistance", "clinicdistance",
        "postofficedistance", "kindergartendistance", "restaurantdistance", "collegedistance",
        "pharmacydistance", "ownership", "buildingmaterial", "condition", "hasparkingspace",
        "hasbalcony", "haselevator", "hassecurity", "hasstorageroom", "price", "source_date",
        "listing_type"
    ],
    "demografia": [
        "miasto_gus", "glowne_miasto", "data", "populacja_ogolna",
        "populacja_mezczyzni", "populacja_kobiety", "zarejestrowani_bezrobotni",
        "przecietne_wynagrodzenie_brutto", "dochody_wlasne_jst"
    ],
    "poi": [
        "city", "name", "street", "number", "lat", "lon", "poi_type"
    ],
}


def _write_postgres(df, table_name):
    pg_user = os.environ.get("POSTGRES_USER", "postgres")
    pg_password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    pg_db = os.environ.get("POSTGRES_DB", "postgres")
    pg_host = os.environ.get("POSTGRES_HOST", "postgres")
    pg_port = os.environ.get("POSTGRES_PORT", "5432")

    url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    # Rename clean columns to match Postgres schema column names and resolve ambiguity
    if table_name == "apartments":
        df = df.drop("squareMeters", "buildYear", "centreDistance")
        df = df.withColumnRenamed("square_meters", "squaremeters") \
               .withColumnRenamed("build_year", "buildyear") \
               .withColumnRenamed("centre_distance", "centredistance")
    elif table_name == "demografia":
        df = df.drop("Populacja_Ogolna", "Populacja_Mezczyzni", "Populacja_Kobiety")
    elif table_name == "poi":
        df = df.drop("Name", "Street", "Number", "LAT", "LON")
        df = df.withColumnRenamed("poi_name", "name") \
               .withColumnRenamed("street_number", "number") \
               .withColumnRenamed("latitude", "lat") \
               .withColumnRenamed("longitude", "lon")

    # Lowercase all DataFrame column names to match the case-sensitive lowercase Postgres tables
    df = df.toDF(*[c.lower() for c in df.columns])

    columns = _DB_COLUMNS.get(table_name)
    if columns:
        df = df.select(*columns)

    # Cast source_date to string to prevent JDBC formatting timezone extensions (e.g. '2024-04-01 +00')
    if "source_date" in df.columns:
        df = df.withColumn("source_date", F.col("source_date").cast("string"))

    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", f"stg.{table_name}") \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("driver", "org.postgresql.Driver") \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()


# --- main --------------------------------------------------------------------
def main():
    raw_dir = f"{_root()}/raw"

    spark = (
        SparkSession.builder.appName("ETL_Staging")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Load and clean tables directly from raw (Spark filters apply)
    tables = {
        "apartments": _stage_apartments(spark, raw_dir),
        "demografia": _stage_demografia(spark, raw_dir),
        "poi": _stage_poi(spark, raw_dir),
    }

    # Write clean records to PostgreSQL
    for name, df in tables.items():
        _write_postgres(df, name)
        print(f"PostgreSQL OK: stg.{name} ({df.count()} wierszy)")

    spark.stop()


if __name__ == "__main__":
    main()
