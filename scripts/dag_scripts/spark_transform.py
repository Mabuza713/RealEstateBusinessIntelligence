"""Spark: clean → staging (parquet). Bez wymiarów i faktów — tylko przygotowanie danych."""

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
    """Ujednolicenie nazw miejscowości: małe litery, bez polskich znaków (PDF sl. 9)."""
    lowered = F.lower(F.trim(col))
    return F.translate(lowered, "ąćęłńóśźż", "acelnoszz")


def _missing_label(col, label="brak informacji"):
    empty = col.isNull() | (F.trim(col.cast("string")) == "")
    return F.when(empty, F.lit(label)).otherwise(F.trim(col.cast("string")))


def _round_num(col, scale=2):
    return F.round(col.cast("double"), scale)


# --- staging: apartments (Dim_Lokal, Dim_Budynek, Dim_Czas) ------------------

def _stage_apartments(spark, clean_dir):
    sources = [
        _csv(spark, f"{clean_dir}/all_apartments_{kind}.csv").withColumn("listing_type", F.lit(kind))
        for kind in ("sell", "rent")
    ]
    df = _union(sources)

    df = (
        df
        .withColumn("city_norm", _norm_city(F.col("city")))
        .withColumn("source_date", F.to_date("source_date", "yyyy-MM"))
        .withColumn("source_year", F.year("source_date"))
        .withColumn("source_month", F.month("source_date"))
        .withColumn("square_meters", F.col("squareMeters").cast(DecimalType(10, 2)))
        .withColumn(
            "price_per_sqm",
            F.when(F.col("squareMeters") > 0, F.col("price") / F.col("squareMeters")).otherwise(F.lit(None)),
        )
        .withColumn("floor", F.coalesce(F.col("floor").cast(IntegerType()), F.lit(-1)))
        .withColumn("build_year", F.col("buildYear").cast(IntegerType()))
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

    # Kontrola jakości Fact_Oferta (PDF sl. 17) — na stagingu
    return (
        df
        .filter(F.col("squareMeters").isNotNull())
        .filter((F.col("squareMeters") >= 10) & (F.col("squareMeters") <= 300))
        .filter(F.col("price") > 0)
    )


# --- staging: demografia (Dim_Demografia) ------------------------------------

def _stage_demografia(spark, clean_dir):
    return (
        _csv(spark, _glob_one(clean_dir, "baza_bi_miasta_*.csv"))
        .withColumn("city_norm", _norm_city("Glowne_Miasto"))
        .withColumn("data_date", F.to_date("Data"))
        .withColumn("populacja_ogolna", F.col("Populacja_Ogolna").cast(IntegerType()))
        .withColumn("populacja_mezczyzni", F.col("Populacja_Mezczyzni").cast(IntegerType()))
        .withColumn("populacja_kobiety", F.col("Populacja_Kobiety").cast(IntegerType()))
        .filter(F.col("Miasto_GUS").isNotNull() & (F.trim(F.col("Miasto_GUS")) != ""))
        .filter(F.col("populacja_ogolna").isNotNull() & (F.col("populacja_ogolna") > 0))
        .dropDuplicates(["Miasto_GUS", "Data"])
    )


# --- staging: POI (źródło Dim_Infrastruktura) --------------------------------

def _stage_poi(spark, clean_dir):
    sources = [
        _csv(spark, f"{clean_dir}/{filename}", ";")
        .withColumn("poi_type", F.lit(poi_type))
        for poi_type, filename in POI_FILES
    ]
    df = _union(sources)

    return (
        df
        .withColumn("city_norm", _norm_city("City"))
        .withColumn("poi_name", _missing_label(F.col("Name")))
        .withColumn("street", _missing_label(F.col("Street")))
        .withColumn("street_number", _missing_label(F.col("Number"), "brak"))
        .withColumn("latitude", _round_num("LAT", 6))
        .withColumn("longitude", _round_num("LON", 6))
        .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
        .filter((F.col("latitude") >= 0) & (F.col("longitude") >= 0))
    )


# --- staging: macro (wskaźniki makro) ----------------------------------------

def _stage_macro(spark, clean_dir):
    return (
        _csv(spark, _glob_one(clean_dir, "poland_real_estate_monthly*.csv"), ";")
        .withColumn("data_date", F.to_date("Date"))
        .withColumn("source_year", F.year("data_date"))
        .withColumn("source_month", F.month("data_date"))
        .filter(F.col("data_date").isNotNull())
    )


# --- main --------------------------------------------------------------------

def main():
    data_root = _root()
    clean_dir = f"{data_root}/clean"
    staging_dir = f"{data_root}/staging"

    spark = SparkSession.builder.appName("ETL_Staging").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    tables = {
        "apartments": _stage_apartments(spark, clean_dir),
        "demografia": _stage_demografia(spark, clean_dir),
        "poi": _stage_poi(spark, clean_dir),
        "macro": _stage_macro(spark, clean_dir),
    }

    for name, df in tables.items():
        path = f"{staging_dir}/{name}"
        _write(df, path)
        print(f"OK: {path} ({df.count()} wierszy)")

    spark.stop()


if __name__ == "__main__":
    main()
