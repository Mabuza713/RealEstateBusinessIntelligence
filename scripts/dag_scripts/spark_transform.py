"""
Krok I potoku transformacji Sparka: Staging.
Skrypt wczytuje surowe pliki CSV, normalizuje nazwy miast, filtruje niepoprawne rekordy,
tłumaczy słowniki na polski, wylicza odległości geograficzne do POI wzorem Haversine
oraz zapisuje dane do tabel stagingowych (stg.*) w PostgreSQL za pomocą JDBC.
"""

import os
from functools import reduce
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType, IntegerType

# Mapowanie typów punktów użyteczności publicznej na pliki źródłowe CSV
POI_FILES = [
    ("cafe", "all_cafes.csv"),
    ("parking", "all_parkings.csv"),
    ("bus_stop", "all_bus_stops.csv"),
]

# Kolumny określające udogodnienia w ofertach mieszkań
APT_AMENITY_COLS = [
    "hasParkingSpace",
    "hasBalcony",
    "hasElevator",
    "hasSecurity",
    "hasStorageRoom",
]

# Kolumny tekstowe w ofertach mieszkań podlegające unifikacji słownikowej
APT_TEXT_COLS = ["buildingMaterial", "condition", "ownership", "type"]


# --- helpers -----------------------------------------------------------------
def _root() -> str:
    """Określa i weryfikuje ścieżkę do katalogu głównego z danymi (data/)."""
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
    """Pomocnicze scalanie wielu ramek danych PySpark za pomocą unionByName (FK-safe dla brakujących kolumn)."""
    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), dfs)


def _csv(spark, path, sep=","):
    """
    Wczytuje plik CSV. Obsługuje również usuwanie znacznika BOM (\ufeff)
    z pierwszej kolumny, co bywa częstym problemem w plikach CSV kodowanych w UTF-8.
    """
    df = spark.read.option("header", True).option("sep", sep).csv(path)
    if df.columns and df.columns[0].startswith('\ufeff'):
        df = df.withColumnRenamed(df.columns[0], df.columns[0][1:])
    return df


def _glob_one(folder, pattern):
    """Wyszukuje i zwraca ścieżkę do pierwszego pliku pasującego do wzorca (glob)."""
    return str(next(Path(folder).glob(pattern)))


def _write(df, path):
    """Zapisuje ramkę danych Spark do formatu Parquet (opcja nadpisywania)."""
    df.write.mode("overwrite").parquet(path)


def _norm_city(col):
    """
    Normalizacja nazw miast:
    Usuwa spacje skrajne, zamienia litery na małe oraz konwertuje polskie znaki diakrytyczne na ich łacińskie odpowiedniki.
    Ułatwia to poprawne złączanie (join) danych z różnych źródeł (Kaggle, GUS, OSM).
    """
    lowered = F.lower(F.trim(col))
    return F.translate(lowered, "ąćęłńóśźż", "acelnoszz")


def _missing_label(col, label="brak informacji"):
    """Zastępuje wartości puste (NULL) lub puste ciągi znakowe zdefiniowaną etykietą."""
    empty = col.isNull() | (F.trim(col.cast("string")) == "")
    return F.when(empty, F.lit(label)).otherwise(F.trim(col.cast("string")))


def _round_num(col, scale=2):
    """Rzutuje kolumnę na double i zaokrągla do określonej liczby miejsc po przecinku."""
    c = F.col(col) if isinstance(col, str) else col
    return F.round(c.cast("double"), scale)


# --- staging: apartments (Dim_Lokal, Dim_Budynek, Dim_Czas) ------------------
def _get_latest_month_from_db(spark):
    """
    Odpytuje bazę danych PostgreSQL przez JDBC w celu pobrania maksymalnej
    daty source_date w tabeli produkcyjnej lub stagingowej.
    Wspomaga filtrowanie przyrostowe na poziomie transformacji Spark.
    """
    pg_user = os.environ.get("POSTGRES_USER", "postgres")
    pg_password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    pg_db = os.environ.get("POSTGRES_DB", "postgres")
    pg_host = os.environ.get("POSTGRES_HOST", "postgres")
    pg_port = os.environ.get("POSTGRES_PORT", "5432")

    url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    # Sprawdzamy tabelę produkcyjną prod.dim_czas
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

    # Fallback: sprawdzamy tabelę stagingową stg.apartments
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
    """
    Ładuje surowe pliki ofert mieszkań (sprzedaż i wynajem), scala je,
    rzutuje typy danych, normalizuje i standaryzuje słowniki wartości tekstowych na język polski
    oraz filtruje niepoprawne wiersze.
    """
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

    # Przyrostowość: odrzucamy rekordy z datami mniejszymi bądź równymi ostatnio załadowanej
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

    # Tłumaczenie i ujednolicanie słowników na język polski
    df = (
        df
        .withColumn("listing_type",
            F.when(F.col("listing_type") == "sell", F.lit("Sprzedaż"))
            .when(F.col("listing_type") == "rent", F.lit("Wynajem"))
            .otherwise(F.col("listing_type"))
        )
        .withColumn("buildingMaterial",
            F.when(F.col("buildingMaterial") == "brick", F.lit("Cegła"))
            .when(F.col("buildingMaterial") == "concreteSlab", F.lit("Wielka płyta"))
            .when(F.col("buildingMaterial").isNull() | (F.trim(F.col("buildingMaterial")) == "") | (F.lower(F.col("buildingMaterial")) == "brak informacji"), F.lit("Brak informacji"))
            .otherwise(F.col("buildingMaterial"))
        )
        .withColumn("condition",
            F.when(F.col("condition") == "low", F.lit("Do remontu"))
            .when(F.col("condition") == "premium", F.lit("Wysoki standard"))
            .when(F.col("condition").isNull() | (F.trim(F.col("condition")) == "") | (F.lower(F.col("condition")) == "brak informacji"), F.lit("Brak informacji"))
            .otherwise(F.col("condition"))
        )
        .withColumn("ownership",
            F.when(F.col("ownership") == "condominium", F.lit("Własność"))
            .when(F.col("ownership") == "cooperative", F.lit("Spółdzielcze własnościowe"))
            .when(F.col("ownership").like("%udzia%"), F.lit("Udział w nieruchomości"))
            .when(F.col("ownership").isNull() | (F.trim(F.col("ownership")) == "") | (F.lower(F.col("ownership")) == "brak informacji"), F.lit("Brak informacji"))
            .otherwise(F.col("ownership"))
        )
        .withColumn("type",
            F.when(F.col("type") == "blockOfFlats", F.lit("Blok mieszkalny"))
            .when(F.col("type") == "tenement", F.lit("Kamienica"))
            .when(F.col("type") == "apartmentBuilding", F.lit("Apartamentowiec"))
            .when(F.col("type").isNull() | (F.trim(F.col("type")) == "") | (F.lower(F.col("type")) == "brak informacji"), F.lit("Brak informacji"))
            .otherwise(F.col("type"))
        )
    )

    # Ujednolicanie cech udogodnień
    for col in APT_AMENITY_COLS:
        if col in df.columns:
            df = df.withColumn(col,
                F.when(F.col(col) == "yes", F.lit("Tak"))
                .when(F.col(col) == "no", F.lit("Nie"))
                .when(F.col(col).isNull() | (F.trim(F.col(col)) == "") | (F.lower(F.col(col)) == "brak") | (F.lower(F.col(col)) == "brak informacji"), F.lit("Brak informacji"))
                .otherwise(F.col(col))
            )

    distance_cols = [c for c in df.columns if c.endswith("Distance")]
    for col in distance_cols:
        df = df.withColumn(col, _round_num(col))

    # Odrzucanie rekordów niespełniających kryteriów jakościowych (kwalifikacja danych wejściowych)
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
    """
    Wczytuje surowy plik danych demograficznych GUS BDL, normalizuje nazwy miast,
    rzutuje typy wskaźników statystycznych i eliminuje duplikaty.
    """
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
    """Scalanie plików kawiarni, parkingów i przystanków OSM w jedną spójną tabelę POI."""
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

# --- database columns specification ------------------------------------------
_DB_COLUMNS = {
    "apartments": [
        "id", "city", "type", "squaremeters", "rooms", "floor", "floorcount", "buildyear",
        "latitude", "longitude", "centredistance", "poicount", "schooldistance", "clinicdistance",
        "postofficedistance", "kindergartendistance", "restaurantdistance", "collegedistance",
        "pharmacydistance", "busstopdistance", "caffedistance", "parkingdistance", "ownership",
        "buildingmaterial", "condition", "hasparkingspace", "hasbalcony", "haselevator",
        "hassecurity", "hasstorageroom", "price", "source_date", "listing_type"
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
    """
    Zapisuje ramkę danych Spark do bazy danych PostgreSQL.
    Mapuje nazwy kolumn, rzutuje daty na format tekstowy w celu uniknięcia rozszerzeń stref czasowych
    przez sterownik JDBC oraz realizuje zapis w trybie overwrite z truncate.
    """
    pg_user = os.environ.get("POSTGRES_USER", "postgres")
    pg_password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    pg_db = os.environ.get("POSTGRES_DB", "postgres")
    pg_host = os.environ.get("POSTGRES_HOST", "postgres")
    pg_port = os.environ.get("POSTGRES_PORT", "5432")

    url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    # Zmiana nazw kolumn na małe litery, aby zachować spójność z bazą PostgreSQL
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

    df = df.toDF(*[c.lower() for c in df.columns])

    columns = _DB_COLUMNS.get(table_name)
    if columns:
        df = df.select(*columns)

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

    # 1. Wstępne wczytanie i oczyszczenie tabel ze stagingu
    apt_df = _stage_apartments(spark, raw_dir)
    demografia_df = _stage_demografia(spark, raw_dir)
    poi_df = _stage_poi(spark, raw_dir)

    # 2. Obliczanie odległości do najbliższych kawiarni, parkingów i przystanków OSM (Wzór Haversine)
    filtered_poi = poi_df.filter(F.col("poi_type").isin("cafe", "parking", "bus_stop")) \
                         .select(F.col("city_norm").alias("_poi_city"), 
                                  F.col("poi_type").alias("_poi_type"),
                                  F.col("latitude").alias("_poi_lat"), 
                                  F.col("longitude").alias("_poi_lon"))
    
    # Do złączenia tworzymy odchudzoną wersję tabeli mieszkań
    apt_for_poi = apt_df.select("id", "listing_type", "source_date", "city_norm", "latitude", "longitude")
    joined = apt_for_poi.join(filtered_poi, apt_for_poi["city_norm"] == filtered_poi["_poi_city"], "left")
    
    # Wyznaczenie odległości na sferze wzorem Haversine
    r = 6371.0  # Promień Ziemi w km
    lat1 = F.radians(F.col("latitude"))
    lon1 = F.radians(F.col("longitude"))
    lat2 = F.radians(F.col("_poi_lat"))
    lon2 = F.radians(F.col("_poi_lon"))
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = F.sin(dlat / 2.0)**2 + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2.0)**2
    c = 2.0 * F.atan2(F.sqrt(a), F.sqrt(1.0 - a))
    dist = r * c
    
    # Agregacja odległości: wybieramy najmniejszy dystans dla każdego typu POI
    min_dist_df = joined.withColumn("_dist", dist) \
                        .groupBy("id", "listing_type", "source_date") \
                        .agg(
                            F.round(F.min(F.when(F.col("_poi_type") == "cafe", F.col("_dist"))), 3).alias("caffeDistance"),
                            F.round(F.min(F.when(F.col("_poi_type") == "parking", F.col("_dist"))), 3).alias("parkingDistance"),
                            F.round(F.min(F.when(F.col("_poi_type") == "bus_stop", F.col("_dist"))), 3).alias("busstopDistance")
                        )
    
    # Zmiana nazw oryginalnych kolumn odległości, aby uniknąć konfliktów przy złączeniu
    _orig_cols = {"caffeDistance": "_orig_caffedistance",
                  "parkingDistance": "_orig_parkingdistance",
                  "busstopDistance": "_orig_busstopdistance"}
    for _src, _tmp in _orig_cols.items():
        if _src in apt_df.columns:
            apt_df = apt_df.withColumnRenamed(_src, _tmp)

    # Złączenie obliczonych odległości z ramką mieszkań
    apt_df = apt_df.join(min_dist_df, on=["id", "listing_type", "source_date"], how="left")

    # Scalanie wyliczonych wartości z oryginalnymi (coalesce zapewnia fallback)
    for _new, _tmp in [("caffeDistance",   "_orig_caffedistance"),
                        ("parkingDistance", "_orig_parkingdistance"),
                        ("busstopDistance", "_orig_busstopdistance")]:
        if _tmp in apt_df.columns:
            apt_df = apt_df.withColumn(_new, F.coalesce(F.col(_new), F.col(_tmp))).drop(_tmp)

    tables = {
        "apartments": apt_df,
        "demografia": demografia_df,
        "poi": poi_df,
    }

    # 3. Zapis do PostgreSQL
    for name, df in tables.items():
        _write_postgres(df, name)
        print(f"PostgreSQL OK: stg.{name} ({df.count()} wierszy)")

    spark.stop()


if __name__ == "__main__":
    main()
