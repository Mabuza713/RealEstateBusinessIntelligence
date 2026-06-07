"""Spark: clean — dodatkowe czyszczenie stg.* wg reguł context.md.

Etap wykonywany PO spark_transform.py.
Czyta z stg.* w Postgres, stosuje pozostałe transformacje, nadpisuje stg.*.

Reguły z context.md:
- Dim_Demografia: odrzucenie wierszy gdzie populacja_k + populacja_m ≠ populacja_ogolna
- Dim_Infrastruktura: IDW dla brakujących odległości — grid-bucketing zamiast cross-join
- Dim_Budynek/Lokal: imputacja dominantą per miasto dla buildingMaterial/condition
- Dim_Czas: ostrzeżenie o lukach w datach
- Fact: double-check powierzchnia 10–300 m² i price > 0

IDW przez grid-bucketing:
  Siatka 0.018° (~2km). Każdy wiersz dostaje klucz (grid_lat, grid_lon).
  Join tylko po kluczu siatki i sąsiednich komórkach → O(n * k) zamiast O(n²).
"""

import os
from datetime import datetime

from pyspark.sql import SparkSession, Window, functions as F

_DISTANCE_COLS = [
    "schooldistance", "clinicdistance", "postofficedistance",
    "kindergartendistance", "restaurantdistance", "collegedistance",
    "pharmacydistance", "centredistance",
]

_BUILDING_COLS = ["buildingmaterial", "condition"]

_GRID_SIZE = 0.018   # ~2 km w stopniach
_IDW_K = 10          # max sąsiedzi do uśrednienia


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------

def _pg_opts(spark, table):
    pg_user = os.environ.get("POSTGRES_USER", "postgres")
    pg_password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    pg_db = os.environ.get("POSTGRES_DB", "postgres")
    pg_host = os.environ.get("POSTGRES_HOST", "postgres")
    pg_port = os.environ.get("POSTGRES_PORT", "5432")
    url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", f"stg.{table}")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    ), url, pg_user, pg_password


def _write_postgres(df, table, url, user, password):
    if "source_date" in df.columns:
        df = df.withColumn("source_date", F.col("source_date").cast("string"))
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", f"stg.{table}") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()


# ---------------------------------------------------------------------------
# Dim_Demografia: weryfikacja sumy populacji
# ---------------------------------------------------------------------------

def _clean_demografia(df):
    """Odrzuć wiersze gdzie populacja_k + populacja_m ≠ populacja_ogolna."""
    has_all = (
        F.col("populacja_ogolna").isNotNull() &
        F.col("populacja_mezczyzni").isNotNull() &
        F.col("populacja_kobiety").isNotNull()
    )
    sum_ok = (
        (F.col("populacja_mezczyzni") + F.col("populacja_kobiety")) == F.col("populacja_ogolna")
    )
    return df.filter(~has_all | sum_ok)


# ---------------------------------------------------------------------------
# IDW przez grid-bucketing — O(n * k), brak cross-join
# ---------------------------------------------------------------------------

def _add_grid_key(df, size):
    """Dodaj kolumnę grid_key = (floor(lat/size), floor(lon/size))."""
    return df.withColumn(
        "_grid_lat", F.floor(F.col("latitude").cast("double") / size)
    ).withColumn(
        "_grid_lon", F.floor(F.col("longitude").cast("double") / size)
    )


def _idw_impute(df, col):
    """IDW dla kolumny `col` przez grid-bucketing (sąsiedztwo 3×3 komórek)."""
    lower_cols = [c.lower() for c in df.columns]
    if col not in lower_cols:
        return df

    # Sprawdź, czy są jakieś brakujące wartości. Jeśli nie, pomiń.
    missing_count = df.filter(F.col(col).isNull()).count()
    if missing_count == 0:
        return df

    # Oblicz mediany per miasto z góry na wypadek braku sąsiadów w siatce
    city_medians = (
        df.filter(F.col(col).isNotNull())
        .groupBy("city")
        .agg(F.percentile_approx(F.col(col).cast("double"), 0.5).alias("_city_median"))
    )

    # Podziel na znane i brakujące
    known = (
        _add_grid_key(df.filter(F.col(col).isNotNull()), _GRID_SIZE)
        .select(
            "_grid_lat", "_grid_lon",
            F.col("latitude").alias("_lat_k"),
            F.col("longitude").alias("_lon_k"),
            F.col(col).cast("double").alias("_val_k"),
        )
    )
    missing = _add_grid_key(df.filter(F.col(col).isNull()), _GRID_SIZE)

    # Eksploduj brakujące na 9 kluczy siatki (sąsiedztwo 3×3)
    offsets = F.array(*[
        F.struct(F.lit(dy).alias("dy"), F.lit(dx).alias("dx"))
        for dy in (-1, 0, 1)
        for dx in (-1, 0, 1)
    ])
    missing_exp = (
        missing
        .withColumn("_offset", F.explode(offsets))
        .withColumn("_join_lat", F.col("_grid_lat") + F.col("_offset.dy"))
        .withColumn("_join_lon", F.col("_grid_lon") + F.col("_offset.dx"))
        .drop("_offset")
    )

    # Broadcastuj małą tabelę missing_exp, aby uniknąć shuffle wielkiego known
    joined = (
        F.broadcast(missing_exp).alias("m")
        .join(
            known,
            (F.col("m._join_lat") == known["_grid_lat"]) &
            (F.col("m._join_lon") == known["_grid_lon"]),
            "left",
        )
        .withColumn(
            "_dist",
            F.sqrt(
                F.pow(F.col("m.latitude").cast("double") - F.col("_lat_k"), 2) +
                F.pow(F.col("m.longitude").cast("double") - F.col("_lon_k"), 2)
            ),
        )
        .filter(F.col("_lat_k").isNotNull() & (F.col("_dist") > 0))
    )

    # IDW: top-k najbliższych → ważona średnia
    w = Window.partitionBy("id", "listing_type")
    agg = (
        joined
        .withColumn("_rn", F.row_number().over(w.orderBy("_dist")))
        .filter(F.col("_rn") <= _IDW_K)
        .withColumn("_weight", F.lit(1.0) / F.col("_dist"))
        .groupBy("id", "listing_type")
        .agg(
            (F.sum(F.col("_weight") * F.col("_val_k")) / F.sum("_weight"))
            .alias("_imputed")
        )
    )

    # Deduplikacja missing po eksplozji, join z wyliczonymi wartościami oraz medianami miast
    missing_dedup = missing.dropDuplicates(["id", "listing_type"])
    imputed = (
        F.broadcast(missing_dedup).alias("md")
        .join(F.broadcast(agg).alias("a"), on=["id", "listing_type"], how="left")
        .join(F.broadcast(city_medians).alias("cm"), on="city", how="left")
        .withColumn(
            col,
            F.when(F.col("a._imputed").isNotNull(), F.round(F.col("a._imputed"), 2))
            .otherwise(F.col("cm._city_median"))
        )
        .drop("_imputed", "_city_median", "_grid_lat", "_grid_lon", "_join_lat", "_join_lon")
    )

    return (
        df.filter(F.col(col).isNotNull())
        .union(imputed.select(df.columns))
    )



# ---------------------------------------------------------------------------
# Dim_Budynek/Lokal: dominanta per miasto (bez cross-join)
# ---------------------------------------------------------------------------

def _impute_building_attrs(df, cols):
    """Uzupełnij NULL/'brak informacji' dominantą z tego samego miasta."""
    for col in cols:
        if col not in [c.lower() for c in df.columns]:
            continue

        is_missing = F.col(col).isNull() | (F.trim(F.col(col)) == "brak informacji")

        city_mode = (
            df.filter(~is_missing)
            .groupBy("city", col)
            .count()
            .withColumn(
                "_rn",
                F.row_number().over(
                    Window.partitionBy("city").orderBy(F.col("count").desc())
                ),
            )
            .filter(F.col("_rn") == 1)
            .select(
                F.col("city").alias("_city_m"),
                F.col(col).alias(f"_mode_{col}"),
            )
        )

        df = (
            df.join(city_mode, df["city"] == city_mode["_city_m"], "left")
            .withColumn(col, F.when(is_missing, F.col(f"_mode_{col}")).otherwise(F.col(col)))
            .drop("_city_m", f"_mode_{col}")
        )

    return df


# ---------------------------------------------------------------------------
# Dim_Czas: ostrzeżenie o lukach w datach
# ---------------------------------------------------------------------------

def _warn_date_gaps(df):
    rows = df.select("source_date").distinct().orderBy("source_date").collect()
    parsed = sorted(
        {datetime.strptime(str(r["source_date"])[:7], "%Y-%m") for r in rows if r["source_date"]}
    )
    for i in range(1, len(parsed)):
        gap = (
            (parsed[i].year * 12 + parsed[i].month) -
            (parsed[i - 1].year * 12 + parsed[i - 1].month)
        )
        if gap > 1:
            print(f"[WARN] Luka w datach Dim_Czas: {parsed[i-1]:%Y-%m} → {parsed[i]:%Y-%m}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    spark = (
        SparkSession.builder.appName("ETL_Clean")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.caseSensitive", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- demografia ---
    df_demo, url, user, pwd = _pg_opts(spark, "demografia")
    df_demo = _clean_demografia(df_demo)
    df_demo = df_demo.localCheckpoint()
    _write_postgres(df_demo, "demografia", url, user, pwd)
    print(f"Clean OK: stg.demografia ({df_demo.count()} wierszy)")

    # --- apartments ---
    df_apt, url, user, pwd = _pg_opts(spark, "apartments")

    # IDW per kolumna odległości (grid-bucketing, bez cross-join)
    for col in _DISTANCE_COLS:
        df_apt = _idw_impute(df_apt, col)
        df_apt = df_apt.localCheckpoint()

    # Dominanta per miasto dla cech budynku
    df_apt = _impute_building_attrs(df_apt, _BUILDING_COLS)
    df_apt = df_apt.localCheckpoint()

    # Double-check filtrów jakości
    df_apt = df_apt.filter(
        F.col("squaremeters").cast("double").between(10, 300) &
        (F.col("price").cast("double") > 0)
    )

    _write_postgres(df_apt, "apartments", url, user, pwd)
    print(f"Clean OK: stg.apartments ({df_apt.count()} wierszy)")

    _warn_date_gaps(df_apt)

    spark.stop()


if __name__ == "__main__":
    main()
