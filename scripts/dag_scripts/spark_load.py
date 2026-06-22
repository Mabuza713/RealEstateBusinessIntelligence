"""
Krok IV potoku transformacji Sparka: Ładowanie (Load).
Skrypt wczytuje dane ze stagingu PostgreSQL (tabelę mieszkań, demografii GUS i wyliczone miary),
buduje produkcyjny schemat gwiazdy w bazie (prod.*) zachowując prawidłową kolejność FK-safe,
generuje sztuczne klucze główne (Surrogate Keys) oraz buduje i zasila tabelę faktów.
"""

import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType, IntegerType


def _pg_url():
    """Zwraca adres URL do PostgreSQL wraz z flagą optymalizacji reWriteBatchedInserts do masowych insertów."""
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db   = os.environ.get("POSTGRES_DB",   "postgres")
    return f"jdbc:postgresql://{host}:{port}/{db}?reWriteBatchedInserts=true"


def _pg_opts():
    """Zwraca dane uwierzytelniające dla sterownika JDBC PostgreSQL."""
    return {
        "user":     os.environ.get("POSTGRES_USER",     "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
        "driver":   "org.postgresql.Driver",
    }


def _read(spark, table):
    """Wczytuje tabelę ze stagingu (stg.*) w PostgreSQL."""
    opts = _pg_opts()
    return (
        spark.read.format("jdbc")
        .option("url", _pg_url())
        .option("dbtable", f"stg.{table}")
        .option("user",     opts["user"])
        .option("password", opts["password"])
        .option("driver",   opts["driver"])
        .load()
    )


def _read_prod(spark, table):
    """Wczytuje tabelę produkcyjną (prod.*) w PostgreSQL."""
    opts = _pg_opts()
    return (
        spark.read.format("jdbc")
        .option("url", _pg_url())
        .option("dbtable", f"prod.{table}")
        .option("user",     opts["user"])
        .option("password", opts["password"])
        .option("driver",   opts["driver"])
        .load()
    )


def _truncate_prod_tables(spark):
    """
    Wykonuje zapytanie TRUNCATE TABLE ... CASCADE bezpośrednio na PostgreSQL przy użyciu
    bramki JVM Sparka. Służy do czyszczenia całej bazy przed pełnym ładowaniem (Full Load).
    """
    opts = _pg_opts()
    jvm = spark.sparkContext._gateway.jvm
    jvm.java.lang.Class.forName("org.postgresql.Driver")
    conn = jvm.java.sql.DriverManager.getConnection(_pg_url(), opts["user"], opts["password"])
    try:
        # CASCADE automatycznie czyści powiązane tabele zależne (w tym tabelę faktów)
        stmt = conn.createStatement()
        stmt.execute(
            "TRUNCATE TABLE prod.Fact_Oferta_Nieruchomosci, prod.Dim_Czas, prod.Dim_Lokal,"
            " prod.Dim_Budynek, prod.Dim_Infrastruktura, prod.Dim_Demografia CASCADE;"
        )
        stmt.close()
        print("TRUNCATE prod.* CASCADE OK")
    finally:
        conn.close()


def _write(df, table):
    """Zapisuje ramkę danych Spark do bazy PostgreSQL w trybie append ze zdefiniowanym rozmiarem batcha."""
    opts = _pg_opts()
    df.write \
        .format("jdbc") \
        .option("url",      _pg_url()) \
        .option("dbtable",  f"prod.{table}") \
        .option("user",     opts["user"]) \
        .option("password", opts["password"]) \
        .option("driver",   opts["driver"]) \
        .option("batchsize", "20000") \
        .mode("append") \
        .save()


def _add_sk_incremental(df, col_name, start_id):
    """
    Dodaje sztuczny klucz zastępczy (Surrogate Key) przy użyciu monotonically_increasing_id().
    Wywołanie coalesce(1) chroni przed generowaniem dużych offsetów (dziur w sekwencji ID) na wielu partycjach,
    co mogłoby przekroczyć pojemność typu INTEGER w bazie.
    """
    return df.coalesce(1).withColumn(col_name, F.monotonically_increasing_id() + 1 + start_id)


# ---------------------------------------------------------------------------
# Dim_Czas
# ---------------------------------------------------------------------------
def _load_dim_czas(spark, apt, force_full_load=False):
    """Uzupełnia wymiar czasu unikalnymi datami wyekstrahowanymi z apartments."""
    df = (
        apt.select("source_date").distinct()
        .withColumn("source_date_str", F.col("source_date").cast("string"))
        .withColumn("miesiac",     F.month(F.to_date("source_date_str", "yyyy-MM-dd")))
        .withColumn("rok",         F.year(F.to_date("source_date_str", "yyyy-MM-dd")))
        .withColumn("source_date_m", F.col("miesiac"))
        .withColumn("source_date_y", F.col("rok"))
        .select(
            F.col("source_date_str").alias("source_date"),
            "miesiac", "rok", "source_date_m", "source_date_y",
        )
    )
    if force_full_load:
        df = _add_sk_incremental(df, "ID_Czasu", 0).cache()
        _write(df, "Dim_Czas")
        print(f"Load OK (Full): prod.Dim_Czas ({df.count()} wierszy)")
        return df
    else:
        # Ładowanie przyrostowe: filtrujemy daty już istniejące w bazie (left_anti)
        existing = _read_prod(spark, "Dim_Czas").cache()
        new_df = df.join(existing, on="source_date", how="left_anti")
        max_id = existing.select(F.max("ID_Czasu")).collect()[0][0] or 0
        new_df_with_id = _add_sk_incremental(new_df, "ID_Czasu", max_id).cache()
        if new_df_with_id.count() > 0:
            _write(new_df_with_id, "Dim_Czas")
            print(f"Load OK (Incremental): prod.Dim_Czas (+{new_df_with_id.count()} wierszy)")
        result_df = existing.unionByName(new_df_with_id).cache()
        existing.unpersist()
        new_df_with_id.unpersist()
        return result_df


# ---------------------------------------------------------------------------
# Dim_Lokal
# ---------------------------------------------------------------------------
def _load_dim_lokal(spark, apt, force_full_load=False):
    """Zasila wymiar lokalu parametrami fizycznymi i udogodnieniami mieszkania."""
    max_id = 0
    if not force_full_load:
        existing = _read_prod(spark, "Dim_Lokal")
        max_id = existing.select(F.max("ID_Lokalu")).collect()[0][0] or 0

    df = _add_sk_incremental(
        apt.select(
            F.col("id").alias("source_id"),
            "listing_type", "source_date", "latitude", "longitude", "city",
            F.col("squaremeters").alias("squareMeters"),
            "rooms", "floor",
            F.col("floorcount").alias("floorCount"),
            "condition", "haselevator", "hasparkingspace",
            "hasstorageroom", "hassecurity", "hasbalcony", "price", "ownership",
        ),
        "ID_Lokalu",
        max_id
    ).cache()

    _write(
        df.select(
            "ID_Lokalu", "source_id", "listing_type",
            "latitude", "longitude", "city", "squareMeters",
            "rooms", "floor", "floorCount", "condition",
            F.col("haselevator").alias("hasElevator"),
            F.col("hasparkingspace").alias("hasParkingSpace"),
            F.col("hasstorageroom").alias("hasStorageRoom"),
            F.col("hassecurity").alias("hasSecurity"),
            F.col("hasbalcony").alias("hasBalcony"),
            "price", "ownership",
        ),
        "Dim_Lokal",
    )
    print(f"Load OK: prod.Dim_Lokal ({df.count()} wierszy)")
    return df


# ---------------------------------------------------------------------------
# Dim_Budynek
# ---------------------------------------------------------------------------
def _load_dim_budynek(spark, apt, force_full_load=False):
    """Zasila wymiar budynku rokiem i materiałem budowlanym."""
    max_id = 0
    if not force_full_load:
        existing = _read_prod(spark, "Dim_Budynek")
        max_id = existing.select(F.max("ID_Budynku")).collect()[0][0] or 0

    df = _add_sk_incremental(
        apt.select(
            F.col("id").alias("source_id"),
            "listing_type", "source_date", "city", "type",
            F.col("buildyear").alias("buildYear"),
            F.col("buildingmaterial").alias("buildingMaterial"),
        ),
        "ID_Budynku",
        max_id
    ).cache()

    _write(df.select("ID_Budynku", "source_id", "listing_type",
                     "city", "type", "buildYear", "buildingMaterial"),
           "Dim_Budynek")
    print(f"Load OK: prod.Dim_Budynek ({df.count()} wierszy)")
    return df


# ---------------------------------------------------------------------------
# Dim_Infrastruktura
# ---------------------------------------------------------------------------
def _load_dim_infrastruktura(spark, apt, force_full_load=False):
    """Zasila wymiar infrastruktury odległościami do punktów POI oraz ich łącznym zagęszczeniem (poiCount)."""
    max_id = 0
    if not force_full_load:
        existing = _read_prod(spark, "Dim_Infrastruktura")
        max_id = existing.select(F.max("ID_Infrastruktury")).collect()[0][0] or 0

    dist_cols = [
        "centredistance", "schooldistance", "clinicdistance",
        "postofficedistance", "collegedistance", "kindergartendistance",
        "restaurantdistance", "pharmacydistance", "poicount",
    ]
    available = [c for c in dist_cols if c in [x.lower() for x in apt.columns]]

    sel = [F.col("id").alias("source_id"), "listing_type", "source_date"]
    renames = {
        "centredistance": "centreDistance", "schooldistance": "schoolDistance",
        "clinicdistance": "clinicDistance", "postofficedistance": "postOfficeDistance",
        "collegedistance": "collegeDistance", "kindergartendistance": "kindergartenDistance",
        "restaurantdistance": "restaurantDistance", "pharmacydistance": "pharmacyDistance",
        "poicount": "poiCount",
    }
    for col in available:
        sel.append(F.col(col).alias(renames.get(col, col)))

    # Odległości do POI z OpenStreetMap wyliczone przez spark_transform.py
    for extra in ("busstopDistance", "caffeDistance", "parkingDistance"):
        if extra.lower() in [x.lower() for x in apt.columns]:
            sel.append(F.col(extra.lower()).alias(extra))
        else:
            sel.append(F.lit(None).cast(DecimalType(10, 2)).alias(extra))

    df = _add_sk_incremental(apt.select(*sel), "ID_Infrastruktury", max_id).cache()
    db_cols = [c for c in df.columns if c != "source_date"]
    _write(df.select(*db_cols), "Dim_Infrastruktura")
    print(f"Load OK: prod.Dim_Infrastruktura ({df.count()} wierszy)")
    return df


# ---------------------------------------------------------------------------
# Dim_Demografia
# ---------------------------------------------------------------------------
def _load_dim_demografia(spark, demo, force_full_load=False):
    """Zasila wymiar demografii miast wskaźnikami ekonomiczno-społecznymi z GUS BDL."""
    df = demo.select(
        F.col("miasto_gus").alias("Miasto_GUS"),
        F.col("glowne_miasto").alias("Glowne_Miasto"),
        F.col("data").alias("Data"),
        F.col("populacja_ogolna").cast(IntegerType()).alias("Populacja_Ogolna"),
        F.col("zarejestrowani_bezrobotni").cast(IntegerType()).alias("Zarejestrowani_Bezrobotni"),
        F.col("przecietne_wynagrodzenie_brutto").alias("Przecietne_Wynagrodzenie_Brutto"),
        F.col("dochody_wlasne_jst").alias("Dochody_Wlasne_JST"),
    )

    if force_full_load:
        df = _add_sk_incremental(df, "ID_Demografii", 0).cache()
        _write(df, "Dim_Demografia")
        print(f"Load OK (Full): prod.Dim_Demografii ({df.count()} wierszy)")
        return df
    else:
        existing = _read_prod(spark, "Dim_Demografia").cache()
        new_df = df.join(existing, on=["Miasto_GUS", "Data"], how="left_anti")
        max_id = existing.select(F.max("ID_Demografii")).collect()[0][0] or 0
        new_df_with_id = _add_sk_incremental(new_df, "ID_Demografii", max_id).cache()
        if new_df_with_id.count() > 0:
            _write(new_df_with_id, "Dim_Demografia")
            print(f"Load OK (Incremental): prod.Dim_Demografii (+{new_df_with_id.count()} wierszy)")
        result_df = existing.unionByName(new_df_with_id).cache()
        existing.unpersist()
        new_df_with_id.unpersist()
        return result_df


# ---------------------------------------------------------------------------
# Fact_Oferta_Nieruchomosci
# ---------------------------------------------------------------------------
def _load_fact(apt, dim_lokal, dim_budynek, dim_infra, dim_czas, dim_demo, apt_measures):
    """
    Zasila tabelę faktów (Fact_Oferta_Nieruchomosci).
    Łączy wczytane wymiary z miarami analitycznymi w oparciu o klucze kompozytowe
    (source_id, listing_type, source_date), co skutecznie eliminuje powstawanie
    kosztownych i niepoprawnych logicznie iloczynów kartezjańskich.
    """
    def _norm_city(col):
        return F.translate(F.lower(F.trim(col)), "ąćęłńóśźż", "acelnoszz")

    apt_city_norm = (
        apt.withColumn("_city_norm", _norm_city(F.col("city")))
        .withColumn("_year", F.year(F.to_date(F.col("source_date"), "yyyy-MM-dd")))
    )

    # Przygotowanie kluczy łączeń dla wymiarów
    lokal_map = dim_lokal.select(
        F.col("source_id").alias("_sid"),
        F.col("listing_type").alias("_lt"),
        F.col("source_date").alias("_sd"),
        "ID_Lokalu"
    )
    bud_map = dim_budynek.select(
        F.col("source_id").alias("_sid"),
        F.col("listing_type").alias("_lt"),
        F.col("source_date").alias("_sd"),
        "ID_Budynku"
    )
    infra_map = dim_infra.select(
        F.col("source_id").alias("_sid"),
        F.col("listing_type").alias("_lt"),
        F.col("source_date").alias("_sd"),
        "ID_Infrastruktury"
    )
    czas_map = dim_czas.select(
        F.col("source_date").alias("_sd"), "ID_Czasu"
    )

    # Złączenie z demografią (tylko powiaty miejskie "m.")
    demo_map = (
        dim_demo.filter(F.col("Miasto_GUS").like("%m.%"))
        .withColumn("_city_norm", _norm_city(F.col("Glowne_Miasto")))
        .withColumn("_year", F.year(F.to_date(F.col("Data"), "yyyy-MM-dd")))
        .select("_city_norm", "_year", "ID_Demografii")
    )

    measures_map = apt_measures.select(
        F.col("id").alias("_sid"),
        F.col("listing_type").alias("_lt"),
        F.col("source_date").alias("_sd"),
        "cena_za_m2",
        "odchylenie_procentowe_ceny",
        "stosunek_najmu_do_wynagrodzenia",
        "premia_lokalizacyjna"
    )

    # Wielokrotne złączenia po unikalnych kluczach złożonych
    fact = (
        apt_city_norm
        .join(lokal_map,  (apt_city_norm["id"] == lokal_map["_sid"]) & (apt_city_norm["listing_type"] == lokal_map["_lt"]) & (apt_city_norm["source_date"] == lokal_map["_sd"]),  "left")
        .join(bud_map,    (apt_city_norm["id"] == bud_map["_sid"])   & (apt_city_norm["listing_type"] == bud_map["_lt"]) & (apt_city_norm["source_date"] == bud_map["_sd"]),    "left")
        .join(infra_map,  (apt_city_norm["id"] == infra_map["_sid"]) & (apt_city_norm["listing_type"] == infra_map["_lt"]) & (apt_city_norm["source_date"] == infra_map["_sd"]),  "left")
        .join(czas_map,   apt_city_norm["source_date"] == czas_map["_sd"], "left")
        .join(demo_map,   on=["_city_norm", "_year"], how="left")
        .join(measures_map, (apt_city_norm["id"] == measures_map["_sid"]) & (apt_city_norm["listing_type"] == measures_map["_lt"]) & (apt_city_norm["source_date"] == measures_map["_sd"]), "left")
    )

    # Wybór miar i kluczy obcych; weryfikacja poprawności powiązań (FK not null)
    result = fact.select(
        "ID_Lokalu", "ID_Budynku", "ID_Infrastruktury", "ID_Czasu", "ID_Demografii",
        F.col("price").cast(DecimalType(15, 2)).alias("Cena_Calkowita"),
        F.col("cena_za_m2").cast(DecimalType(10, 2)).alias("Cena_Za_M2"),
        F.col("squaremeters").cast(DecimalType(10, 2)).alias("Powierzchnia_Lokalu"),
        F.col("odchylenie_procentowe_ceny").cast(DecimalType(8, 4)).alias("Odchylenie_Procentowe_Ceny"),
        F.col("stosunek_najmu_do_wynagrodzenia").cast(DecimalType(8, 4)).alias("Stosunek_Najmu_Do_Wynagrodzenia"),
        F.col("premia_lokalizacyjna").cast(DecimalType(10, 2)).alias("Premia_Lokalizacyjna"),
    ).filter(F.col("ID_Lokalu").isNotNull() & F.col("ID_Budynku").isNotNull() &
             F.col("ID_Infrastruktury").isNotNull() & F.col("ID_Czasu").isNotNull()).cache()

    # Zapis tabeli faktów do PostgreSQL
    _write(result, "Fact_Oferta_Nieruchomosci")
    print(f"Load OK: prod.Fact_Oferta_Nieruchomosci ({result.count()} wierszy)")
    result.unpersist()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    spark = (
        SparkSession.builder.appName("ETL_Load")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.caseSensitive", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    
    # 1. Truncate całej bazy produkcyjnej przy Full Load
    force_full_load = os.environ.get("FORCE_FULL_LOAD", "false").lower() == "true"
    if force_full_load:
        _truncate_prod_tables(spark)

    # Keszujemy dane stagingowe, by uniknąć wielokrotnego odczytu przez JDBC
    apt  = _read(spark, "apartments").cache()
    demo = _read(spark, "demografia").cache()
    apt_measures = _read(spark, "apartments_measures").cache()

    # 2. Zasilanie Wymiarów (FK-safe write order: Czas jako pierwszy)
    dim_czas  = _load_dim_czas(spark, apt, force_full_load)
    dim_lokal = _load_dim_lokal(spark, apt, force_full_load)
    dim_bud   = _load_dim_budynek(spark, apt, force_full_load)
    dim_infra = _load_dim_infrastruktura(spark, apt, force_full_load)
    dim_demo  = _load_dim_demografia(spark, demo, force_full_load)

    # 3. Zasilanie Tabeli Faktów na samym końcu
    _load_fact(apt, dim_lokal, dim_bud, dim_infra, dim_czas, dim_demo, apt_measures)

    # Zwolnienie pamięci cache
    dim_czas.unpersist()
    dim_lokal.unpersist()
    dim_bud.unpersist()
    dim_infra.unpersist()
    dim_demo.unpersist()
    apt.unpersist()
    demo.unpersist()
    apt_measures.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
