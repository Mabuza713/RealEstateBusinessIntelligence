"""
Krok III potoku transformacji Sparka: Wyliczanie Miar Biznesowych (KPI).
Skrypt wczytuje oczyszczone tabele apartments i demografia ze stagingu PostgreSQL,
oblicza cenę za m2, wskaźnik okazji rynkowych (Deal Index), stosunek ceny najmu do lokalnej płacy
oraz premię lokalizacyjną POI, zapisując wyniki do tabeli stg.apartments_measures.
"""

import os

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import DecimalType


# ---------------------------------------------------------------------------
def _pg_url():
    """Zwraca pełny adres URL połączenia JDBC do bazy PostgreSQL."""
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db   = os.environ.get("POSTGRES_DB",   "postgres")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def _pg_opts():
    """Zwraca parametry uwierzytelnienia JDBC do bazy PostgreSQL."""
    return {
        "user":     os.environ.get("POSTGRES_USER",     "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
        "driver":   "org.postgresql.Driver",
    }


def _read(spark, table):
    """Wczytuje zadaną tabelę stagingową z bazy PostgreSQL."""
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
    """Zapisuje wyniki do wskazanej tabeli stagingowej w bazie PostgreSQL."""
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
    """Ujednolica nazwę miasta do małych liter bez spacji i znaków diakrytycznych."""
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

    # Filtracja powiatów miejskich (unikanie duplikacji z powiatami ziemskimi w GUS)
    demo_map = (
        demo.filter(F.col("miasto_gus").like("%m.%"))
        .withColumn("_city_norm", _norm_city(F.col("glowne_miasto")))
        .withColumn("_year", F.year(F.to_date(F.col("data"), "yyyy-MM-dd")))
        .select("_city_norm", "_year", F.col("przecietne_wynagrodzenie_brutto").alias("wage"))
    )

    df = apt_norm.join(demo_map, on=["_city_norm", "_year"], how="left")

    # 2. Obliczenie ceny za metr kwadratowy (Cena_Za_M2)
    df = df.withColumn(
        "cena_za_m2",
        F.when(F.col("squaremeters").cast("double") > 0,
               F.col("price").cast("double") / F.col("squaremeters").cast("double"))
        .otherwise(F.lit(None))
    )

    # 3. Obliczenie wskaźnika okazji Deal Index (Odchylenie_Procentowe_Ceny)
    # Wyznaczenie średniej ceny za m2 w grupach porównawczych
    avg_df = df.groupBy("_city_norm", "rooms", "type", "listing_type").agg(
        F.avg("cena_za_m2").alias("_avg_city_rooms_type")
    )
    df = df.join(avg_df, on=["_city_norm", "rooms", "type", "listing_type"], how="left")

    # Deal Index: ujemny wskaźnik oznacza, że cena jest niższa od rynkowej średniej
    df = df.withColumn(
        "odchylenie_procentowe_ceny",
        F.when(F.col("_avg_city_rooms_type") > 0,
               (F.col("cena_za_m2") - F.col("_avg_city_rooms_type")) / F.col("_avg_city_rooms_type"))
        .otherwise(F.lit(None))
    )

    # 4. Obliczenie relacji najmu do wynagrodzenia (Stosunek_Najmu_Do_Wynagrodzenia - KPI 4)
    # Obliczany tylko dla ofert Wynajmu w powiązaniu z lokalną średnią płacą brutto
    df = df.withColumn(
        "stosunek_najmu_do_wynagrodzenia",
        F.when(
            (F.col("listing_type") == "Wynajem") & F.col("wage").isNotNull() & (F.col("wage") > 0),
            F.col("price").cast("double") / F.col("wage").cast("double")
        ).otherwise(F.lit(None))
    )

    # 5. Obliczenie Premii Lokalizacyjnej (KPI 3) w oparciu o zagęszczenie POI
    # Dzielimy oferty na te z bogatą infrastrukturą (>15 POI) oraz ubogą (<=15 POI).
    # Wyliczamy wartość średnią pojedynczego punktu POI dla miasta i typu transakcji.
    premia_df = df.groupBy("_city_norm", "listing_type").agg(
        F.avg(F.when(F.col("poicount") > 15, F.col("cena_za_m2"))).alias("_avg_high_price"),
        F.avg(F.when(F.col("poicount") <= 15, F.col("cena_za_m2"))).alias("_avg_low_price"),
        F.avg(F.when(F.col("poicount") > 15, F.col("poicount"))).alias("_avg_high_poi"),
        F.avg(F.when(F.col("poicount") <= 15, F.col("poicount"))).alias("_avg_low_poi")
    ).withColumn(
        "_diff_price",
        F.col("_avg_high_price") - F.col("_avg_low_price")
    ).withColumn(
        "_diff_poi",
        F.col("_avg_high_poi") - F.col("_avg_low_poi")
    ).withColumn(
        "cena_za_singiel_POI",
        F.when(
            (F.col("_diff_price").isNotNull()) & (F.col("_diff_poi").isNotNull()) & (F.col("_diff_poi") > 0),
            F.col("_diff_price") / F.col("_diff_poi")
        ).otherwise(F.lit(0.0))
    ).select("_city_norm", "listing_type", "cena_za_singiel_POI")

    df = df.join(premia_df, on=["_city_norm", "listing_type"], how="left")

    # Premia lokalizacyjna dla konkretnej nieruchomości
    df = df.withColumn(
        "premia_lokalizacyjna",
        F.coalesce(F.round(F.col("poicount") * F.col("cena_za_singiel_POI"), 2), F.lit(0.0))
    )

    # 6. Wybór ostatecznych kolumn i rzutowanie typów do zapisu
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
