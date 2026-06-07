"""Spark: load — stg.* → prod.* (schemat gwiazdy).

Kolejność zapisu respektuje FK:
  Dim_Czas → Dim_Lokal, Dim_Budynek, Dim_Infrastruktura, Dim_Demografia → Fact

Surrogate keys (ID_*) generowane przez monotonically_increasing_id().
Miary obliczane inline przed zapisem.
"""

import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType, IntegerType


# ---------------------------------------------------------------------------
def _pg_url():
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db   = os.environ.get("POSTGRES_DB",   "postgres")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def _pg_opts():
    return {
        "user":     os.environ.get("POSTGRES_USER",     "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
        "driver":   "org.postgresql.Driver",
    }


def _read(spark, table):
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


def _truncate_prod_tables(spark):
    pg_user = os.environ.get("POSTGRES_USER", "postgres")
    pg_password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    pg_db = os.environ.get("POSTGRES_DB", "postgres")
    pg_host = os.environ.get("POSTGRES_HOST", "postgres")
    pg_port = os.environ.get("POSTGRES_PORT", "5432")
    url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    jvm = spark.sparkContext._gateway.jvm
    jvm.java.lang.Class.forName("org.postgresql.Driver")
    conn = jvm.java.sql.DriverManager.getConnection(url, pg_user, pg_password)
    try:
        stmt = conn.createStatement()
        # Truncate all tables cascade to reset IDs and clear tables without violating foreign key constraints
        stmt.execute("TRUNCATE TABLE prod.Fact_Oferta_Nieruchomosci, prod.Dim_Czas, prod.Dim_Lokal, prod.Dim_Budynek, prod.Dim_Infrastruktura, prod.Dim_Demografia CASCADE;")
        stmt.close()
        print("TRUNCATE prod.* CASCADE OK")
    finally:
        conn.close()


def _write(df, table):
    opts = _pg_opts()
    df.write \
        .format("jdbc") \
        .option("url",      _pg_url()) \
        .option("dbtable",  f"prod.{table}") \
        .option("user",     opts["user"]) \
        .option("password", opts["password"]) \
        .option("driver",   opts["driver"]) \
        .mode("append") \
        .save()


def _add_sk(df, col_name):
    """Dodaj surrogate key jako sekwencyjny long (1-based)."""
    return df.withColumn(col_name, F.monotonically_increasing_id() + 1)


# ---------------------------------------------------------------------------
# Dim_Czas
# ---------------------------------------------------------------------------
def _load_dim_czas(spark, apt):
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
    df = _add_sk(df, "ID_Czasu")
    _write(df.select("ID_Czasu", "source_date", "miesiac", "rok", "source_date_m", "source_date_y"),
           "Dim_Czas")
    print(f"Load OK: prod.Dim_Czas ({df.count()} wierszy)")
    return df  # zwracamy z ID_Czasu do joinu w Fact


# ---------------------------------------------------------------------------
# Dim_Lokal
# ---------------------------------------------------------------------------
def _load_dim_lokal(apt):
    df = _add_sk(
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
    )
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
def _load_dim_budynek(apt):
    df = _add_sk(
        apt.select(
            F.col("id").alias("source_id"),
            "listing_type", "source_date", "city", "type",
            F.col("buildyear").alias("buildYear"),
            F.col("buildingmaterial").alias("buildingMaterial"),
        ),
        "ID_Budynku",
    )
    _write(df.select("ID_Budynku", "source_id", "listing_type",
                     "city", "type", "buildYear", "buildingMaterial"),
           "Dim_Budynek")
    print(f"Load OK: prod.Dim_Budynek ({df.count()} wierszy)")
    return df


# ---------------------------------------------------------------------------
# Dim_Infrastruktura
# ---------------------------------------------------------------------------
def _load_dim_infrastruktura(apt):
    dist_cols = [
        "centredistance", "schooldistance", "clinicdistance",
        "postofficedistance", "collegedistance", "kindergartendistance",
        "restaurantdistance", "pharmacydistance", "poicount",
    ]
    # kolumny opcjonalne (mogą nie istnieć jeśli nie zostały wyliczone przez clean)
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

    # POI odległości liczone przez spark_clean (busstop/caffe/parking) — jeśli istnieją
    for extra in ("busstopDistance", "caffeDistance", "parkingDistance"):
        if extra.lower() in [x.lower() for x in apt.columns]:
            sel.append(F.col(extra.lower()).alias(extra))
        else:
            sel.append(F.lit(None).cast(DecimalType(10, 2)).alias(extra))

    df = _add_sk(apt.select(*sel), "ID_Infrastruktury")
    db_cols = [c for c in df.columns if c != "source_date"]
    _write(df.select(*db_cols), "Dim_Infrastruktura")
    print(f"Load OK: prod.Dim_Infrastruktura ({df.count()} wierszy)")
    return df


# ---------------------------------------------------------------------------
# Dim_Demografia
# ---------------------------------------------------------------------------
def _load_dim_demografia(demo):
    df = _add_sk(
        demo.select(
            F.col("miasto_gus").alias("Miasto_GUS"),
            F.col("glowne_miasto").alias("Glowne_Miasto"),
            F.col("data").alias("Data"),
            F.col("populacja_ogolna").cast(IntegerType()).alias("Populacja_Ogolna"),
            F.col("zarejestrowani_bezrobotni").cast(IntegerType()).alias("Zarejestrowani_Bezrobotni"),
            F.col("przecietne_wynagrodzenie_brutto").alias("Przecietne_Wynagrodzenie_Brutto"),
            F.col("dochody_wlasne_jst").alias("Dochody_Wlasne_JST"),
        ),
        "ID_Demografii",
    )
    _write(df, "Dim_Demografia")
    print(f"Load OK: prod.Dim_Demografii ({df.count()} wierszy)")
    return df


# ---------------------------------------------------------------------------
# Fact_Oferta_Nieruchomosci
# ---------------------------------------------------------------------------
def _load_fact(apt, dim_lokal, dim_budynek, dim_infra, dim_czas, dim_demo):
    # Normalizacja city do joinu z demografią (małe litery, bez polskich znaków)
    from pyspark.sql.functions import translate, lower, trim
    def _norm(col):
        return translate(lower(trim(col)), "ąćęłńóśźż", "acelnoszz")

    apt_city_norm = (
        apt.withColumn("_city_norm", _norm(F.col("city")))
        .withColumn("_year", F.year(F.to_date(F.col("source_date"), "yyyy-MM-dd")))
    )

    # surrogate key lookup — mapujemy source_id+listing_type+source_date na SK
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

    # demo: join po znormalizowanym mieście (Glowne_Miasto) i roku
    # Filtrujemy tylko powiaty miejskie (zawierające "m."), aby uniknąć duplikacji z powiatami ziemskimi
    demo_map = (
        dim_demo.filter(F.col("Miasto_GUS").like("%m.%"))
        .withColumn("_city_norm", _norm(F.col("Glowne_Miasto")))
        .withColumn("_year", F.year(F.to_date(F.col("Data"), "yyyy-MM-dd")))
        .select("_city_norm", "_year", "ID_Demografii", "Przecietne_Wynagrodzenie_Brutto")
    )

    fact = (
        apt_city_norm
        .join(lokal_map,  (apt_city_norm["id"] == lokal_map["_sid"]) & (apt_city_norm["listing_type"] == lokal_map["_lt"]) & (apt_city_norm["source_date"] == lokal_map["_sd"]),  "left")
        .join(bud_map,    (apt_city_norm["id"] == bud_map["_sid"])   & (apt_city_norm["listing_type"] == bud_map["_lt"])   & (apt_city_norm["source_date"] == bud_map["_sd"]),    "left")
        .join(infra_map,  (apt_city_norm["id"] == infra_map["_sid"]) & (apt_city_norm["listing_type"] == infra_map["_lt"]) & (apt_city_norm["source_date"] == infra_map["_sd"]),  "left")
        .join(czas_map,   apt_city_norm["source_date"] == czas_map["_sd"], "left")
        .join(demo_map,   on=["_city_norm", "_year"], how="left")
    )

    # uproszczone inline (unikamy zależności kołowej):
    fact = fact.withColumn(
        "Cena_Za_M2",
        F.when(F.col("squaremeters").cast("double") > 0,
               F.col("price").cast("double") / F.col("squaremeters").cast("double"))
        .otherwise(F.lit(None))
    )

    # Odchylenie % ceny — wymaga avg per (city, rooms, type) => self-join
    avg_df = fact.groupBy("_city_norm", "rooms", "type").agg(
        F.avg("Cena_Za_M2").alias("_avg_city_rooms_type")
    )
    fact = fact.join(avg_df, on=["_city_norm", "rooms", "type"], how="left")

    fact = fact.withColumn(
        "Odchylenie_Procentowe_Ceny",
        F.when(F.col("_avg_city_rooms_type") > 0,
               (F.col("Cena_Za_M2") - F.col("_avg_city_rooms_type")) / F.col("_avg_city_rooms_type"))
        .otherwise(F.lit(None))
    )

    # Stosunek najmu do wynagrodzenia (KPI 4) — tylko oferty rent
    fact = fact.withColumn(
        "Stosunek_Najmu_Do_Wynagrodzenia",
        F.when(
            (F.col("listing_type") == "rent") & F.col("Przecietne_Wynagrodzenie_Brutto").isNotNull() &
            (F.col("Przecietne_Wynagrodzenie_Brutto") > 0),
            F.col("price").cast("double") / F.col("Przecietne_Wynagrodzenie_Brutto").cast("double")
        ).otherwise(F.lit(None))
    )

    # Premia lokalizacyjna — wysoka vs niska liczba POI (> 15)
    avg_poi_high = fact.filter(F.col("poicount") > 15).agg(F.avg("Cena_Za_M2").alias("_high")).collect()[0]["_high"]
    avg_poi_low  = fact.filter(F.col("poicount") <= 15).agg(F.avg("Cena_Za_M2").alias("_low")).collect()[0]["_low"]
    premia = float(avg_poi_high or 0) - float(avg_poi_low or 0)

    fact = fact.withColumn("Premia_Lokalizacyjna", F.lit(round(premia, 2)))

    result = fact.select(
        "ID_Lokalu", "ID_Budynku", "ID_Infrastruktury", "ID_Czasu", "ID_Demografii",
        F.col("price").cast(DecimalType(15, 2)).alias("Cena_Calkowita"),
        F.round("Cena_Za_M2", 2).cast(DecimalType(10, 2)).alias("Cena_Za_M2"),
        F.col("squaremeters").cast(DecimalType(10, 2)).alias("Powierzchnia_Lokalu"),
        F.round("Odchylenie_Procentowe_Ceny", 4).cast(DecimalType(8, 4)).alias("Odchylenie_Procentowe_Ceny"),
        F.round("Stosunek_Najmu_Do_Wynagrodzenia", 4).cast(DecimalType(8, 4)).alias("Stosunek_Najmu_Do_Wynagrodzenia"),
        F.col("Premia_Lokalizacyjna").cast(DecimalType(10, 2)).alias("Premia_Lokalizacyjna"),
    ).filter(F.col("ID_Lokalu").isNotNull() & F.col("ID_Budynku").isNotNull() &
             F.col("ID_Infrastruktury").isNotNull() & F.col("ID_Czasu").isNotNull())

    _write(result, "Fact_Oferta_Nieruchomosci")
    print(f"Load OK: prod.Fact_Oferta_Nieruchomosci ({result.count()} wierszy)")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    spark = (
        SparkSession.builder.appName("ETL_Load")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.caseSensitive", "false")
        .getOrCreate()
    )
    # Truncate parent and child tables in Postgres to handle foreign key dependencies
    _truncate_prod_tables(spark)

    apt  = _read(spark, "apartments")
    demo = _read(spark, "demografia")

    # Wymiary — kolejność: Czas najpierw (potrzebny do Fact)
    dim_czas  = _load_dim_czas(spark, apt)
    dim_lokal = _load_dim_lokal(apt)
    dim_bud   = _load_dim_budynek(apt)
    dim_infra = _load_dim_infrastruktura(apt)
    dim_demo  = _load_dim_demografia(demo)

    # Fakt — na końcu, po wszystkich wymiarach
    _load_fact(apt, dim_lokal, dim_bud, dim_infra, dim_czas, dim_demo)

    spark.stop()


if __name__ == "__main__":
    main()
