"""Spark: calculate measures and save them to stg.apartments_measures."""

import os

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import DecimalType


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


def _write(df, table):
    opts = _pg_opts()
    df.write \
        .format("jdbc") \
        .option("url",      _pg_url()) \
        .option("dbtable",  f"stg.{table}") \
        .option("user",     opts["user"]) \
        .option("password", opts["password"]) \
        .option("driver",   opts["driver"]) \
        .mode("overwrite") \
        .save()


def _norm_city(col):
    return F.translate(F.lower(F.trim(col)), "ąćęłńóśźż", "acelnoszz")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    spark = (
        SparkSession.builder.appName("ETL_Measures")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.caseSensitive", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    apt  = _read(spark, "apartments")
    demo = _read(spark, "demografia")

    # 1. Przygotowanie danych i normalizacja do złączenia z demografią
    apt_norm = (
        apt.withColumn("_city_norm", _norm_city(F.col("city")))
        .withColumn("_year", F.year(F.to_date(F.col("source_date"), "yyyy-MM-dd")))
    )

    demo_map = (
        demo.filter(F.col("miasto_gus").like("%m.%"))
        .withColumn("_city_norm", _norm_city(F.col("glowne_miasto")))
        .withColumn("_year", F.year(F.to_date(F.col("data"), "yyyy-MM-dd")))
        .select("_city_norm", "_year", F.col("przecietne_wynagrodzenie_brutto").alias("wage"))
    )

    df = apt_norm.join(demo_map, on=["_city_norm", "_year"], how="left")

    # 2. Obliczenie Cena_Za_M2
    df = df.withColumn(
        "cena_za_m2",
        F.when(F.col("squaremeters").cast("double") > 0,
               F.col("price").cast("double") / F.col("squaremeters").cast("double"))
        .otherwise(F.lit(None))
    )

    # 3. Obliczenie Odchylenie_Procentowe_Ceny (Deal Index)
    avg_df = df.groupBy("_city_norm", "rooms", "type", "listing_type").agg(
        F.avg("cena_za_m2").alias("_avg_city_rooms_type")
    )
    df = df.join(avg_df, on=["_city_norm", "rooms", "type", "listing_type"], how="left")

    df = df.withColumn(
        "odchylenie_procentowe_ceny",
        F.when(F.col("_avg_city_rooms_type") > 0,
               (F.col("cena_za_m2") - F.col("_avg_city_rooms_type")) / F.col("_avg_city_rooms_type"))
        .otherwise(F.lit(None))
    )

    # 4. Obliczenie Stosunek_Najmu_Do_Wynagrodzenia (KPI 4)
    df = df.withColumn(
        "stosunek_najmu_do_wynagrodzenia",
        F.when(
            (F.col("listing_type") == "rent") & F.col("wage").isNotNull() & (F.col("wage") > 0),
            F.col("price").cast("double") / F.col("wage").cast("double")
        ).otherwise(F.lit(None))
    )

    # 5. Obliczenie Premia_Lokalizacyjna (KPI 3) — oddzielnie dla 'sell' i 'rent'
    def _calc_premia(fdf):
        rows = fdf.agg(F.avg("cena_za_m2").alias("val")).collect()
        return float(rows[0]["val"] or 0) if rows else 0.0

    sell_df = df.filter(F.col("listing_type") == "sell")
    premia_sell = _calc_premia(sell_df.filter(F.col("poicount") > 15)) - _calc_premia(sell_df.filter(F.col("poicount") <= 15))

    rent_df = df.filter(F.col("listing_type") == "rent")
    premia_rent = _calc_premia(rent_df.filter(F.col("poicount") > 15)) - _calc_premia(rent_df.filter(F.col("poicount") <= 15))

    df = df.withColumn(
        "premia_lokalizacyjna",
        F.when(F.col("listing_type") == "sell", F.lit(round(premia_sell, 2)))
        .otherwise(F.lit(round(premia_rent, 2)))
    )

    # 6. Wybór ostatecznych kolumn i rzutowanie typów
    result = df.select(
        "id", "listing_type", "source_date",
        F.round("cena_za_m2", 2).cast(DecimalType(10, 2)).alias("cena_za_m2"),
        F.round("odchylenie_procentowe_ceny", 4).cast(DecimalType(8, 4)).alias("odchylenie_procentowe_ceny"),
        F.round("stosunek_najmu_do_wynagrodzenia", 4).cast(DecimalType(8, 4)).alias("stosunek_najmu_do_wynagrodzenia"),
        F.col("premia_lokalizacyjna").cast(DecimalType(10, 2)).alias("premia_lokalizacyjna")
    )

    # 7. Zapis do stg.apartments_measures
    _write(result, "apartments_measures")
    print(f"Measures OK: stg.apartments_measures ({result.count()} wierszy)")

    spark.stop()


if __name__ == "__main__":
    main()
