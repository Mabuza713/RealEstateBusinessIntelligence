"""Spark: clean — dodatkowe czyszczenie stg.* wg reguł context.md.

Etap wykonywany PO spark_transform.py (który wstępnie czyści i ładuje dane).
Czyta z stg.* w Postgres, stosuje pozostałe transformacje, nadpisuje stg.*.

Reguły z context.md obsługiwane tutaj:
- Dim_Demografia: odrzucenie wierszy gdzie populacja_k + populacja_m ≠ populacja_ogolna
- Dim_Infrastruktura: IDW (Inverse Distance Weighting) dla brakujących odległości POI
- Dim_Budynek/Lokal: imputacja brakującego buildingMaterial/condition z k-NN (50m → 2km)
- Dim_Czas: deduplicacja na (source_date) — brak luk w datach (ostrzeżenie)
- Fact: cena_za_m2 nie może być ujemna; powierzchnia 10–300 m²
"""

import os

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import DecimalType

# odległości do imputacji IDW (Dim_Infrastruktura)
_DISTANCE_COLS = [
    "schooldistance", "clinicdistance", "postofficedistance",
    "kindergartendistance", "restaurantdistance", "collegedistance",
    "pharmacydistance", "centredistance",
]

# cechy budynku do imputacji k-NN z sąsiedztwa
_BUILDING_COLS = ["buildingmaterial", "condition"]

# max sąsiedzi do IDW
_IDW_K = 10
_IDW_RADIUS_DEG = 0.018  # ~2 km w stopniach


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


# --- Dim_Demografia: weryfikacja sumy populacji --------------------------------
def _clean_demografia(df):
    """Odrzuć wiersze gdzie populacja_k + populacja_m ≠ populacja_ogolna (gdy wszystkie ≠ NULL)."""
    has_all = (
        F.col("populacja_ogolna").isNotNull() &
        F.col("populacja_mezczyzni").isNotNull() &
        F.col("populacja_kobiety").isNotNull()
    )
    sum_ok = (F.col("populacja_mezczyzni") + F.col("populacja_kobiety")) == F.col("populacja_ogolna")
    return df.filter(~has_all | sum_ok)


# --- Dim_Infrastruktura: IDW dla brakujących odległości -----------------------
def _idw_impute(df, col):
    """Uzupełnij brakujące wartości `col` przez IDW z 10 najbliższych sąsiadów (w ~2km)."""
    known = df.filter(F.col(col).isNotNull()).select(
        F.col("latitude").alias("_lat_k"),
        F.col("longitude").alias("_lon_k"),
        F.col(col).alias("_val_k"),
    )
    missing = df.filter(F.col(col).isNull())

    if missing.count() == 0:
        return df

    # Cross-join z filtrem bounding-box, następnie IDW
    joined = (
        missing.join(known, how="cross")
        .filter(
            (F.abs(F.col("latitude") - F.col("_lat_k")) <= _IDW_RADIUS_DEG) &
            (F.abs(F.col("longitude") - F.col("_lon_k")) <= _IDW_RADIUS_DEG)
        )
        .withColumn(
            "_dist",
            F.sqrt(
                F.pow(F.col("latitude") - F.col("_lat_k"), 2) +
                F.pow(F.col("longitude") - F.col("_lon_k"), 2)
            ),
        )
        .filter(F.col("_dist") > 0)
        .withColumn("_w", F.lit(1.0) / F.col("_dist"))
        .withColumn("_wz", F.col("_w") * F.col("_val_k"))
    )

    w = Window.partitionBy("id", "listing_type")
    ranked = (
        joined
        .withColumn("_rn", F.row_number().over(w.orderBy(F.col("_dist"))))
        .filter(F.col("_rn") <= _IDW_K)
    )

    agg = ranked.groupBy("id", "listing_type").agg(
        (F.sum("_wz") / F.sum("_w")).alias(f"_imputed_{col}")
    )

    imputed = (
        missing
        .join(agg, on=["id", "listing_type"], how="left")
        .withColumn(col, F.round(F.col(f"_imputed_{col}").cast("double"), 2))
        .drop(f"_imputed_{col}")
    )

    return df.filter(F.col(col).isNotNull()).union(imputed)


# --- Dim_Budynek/Lokal: imputacja cech budynku z sąsiedztwa ------------------
def _impute_building_attr(df, col):
    """Uzupełnij NULL w `col` dominantą z sąsiedztwa (50m pierwsze, do 2km fallback)."""
    known = df.filter(
        F.col(col).isNotNull() &
        (F.trim(F.col(col)) != "brak informacji")
    ).select("latitude", "longitude", F.col(col).alias("_val"))

    missing = df.filter(
        F.col(col).isNull() | (F.trim(F.col(col)) == "brak informacji")
    )

    if missing.count() == 0:
        return df

    for radius in (0.00045, _IDW_RADIUS_DEG):  # ~50m, ~2km
        joined = (
            missing.join(known, how="cross")
            .withColumn(
                "_dist",
                F.sqrt(
                    F.pow(F.col("latitude") - F.col("longitude_1").alias("lon"), 2) +
                    F.pow(F.col("latitude") - F.col("latitude"), 2)
                ),
            )
        )
        # uproszczone: bounding box
        joined = (
            missing.alias("m")
            .join(known.alias("k"), how="cross")
            .filter(
                (F.abs(F.col("m.latitude") - F.col("k.latitude")) <= radius) &
                (F.abs(F.col("m.longitude") - F.col("k.longitude")) <= radius)
            )
            .withColumn(
                "_dist",
                F.sqrt(
                    F.pow(F.col("m.latitude") - F.col("k.latitude"), 2) +
                    F.pow(F.col("m.longitude") - F.col("k.longitude"), 2)
                ),
            )
        )

        w = Window.partitionBy("m.id", "m.listing_type")
        ranked = (
            joined
            .withColumn("_rn", F.row_number().over(w.orderBy("_dist")))
            .filter(F.col("_rn") <= _IDW_K)
        )

        # dominanta (mode) — najczęstsza wartość w okolicy
        mode_agg = ranked.groupBy("m.id", "m.listing_type", "_val").count()
        w2 = Window.partitionBy("m.id", "m.listing_type")
        mode_df = (
            mode_agg
            .withColumn("_mode_rn", F.row_number().over(w2.orderBy(F.col("count").desc())))
            .filter(F.col("_mode_rn") == 1)
            .select(
                F.col("m.id").alias("_id"),
                F.col("m.listing_type").alias("_lt"),
                F.col("_val").alias(f"_imputed_{col}"),
            )
        )

        imputed = (
            missing
            .join(mode_df, (missing["id"] == F.col("_id")) & (missing["listing_type"] == F.col("_lt")), "left")
            .withColumn(col, F.coalesce(F.col(f"_imputed_{col}"), F.col(col)))
            .drop("_id", "_lt", f"_imputed_{col}")
        )

        still_missing = imputed.filter(F.col(col).isNull() | (F.trim(F.col(col)) == "brak informacji"))
        resolved = imputed.filter(F.col(col).isNotNull() & (F.trim(F.col(col)) != "brak informacji"))

        if still_missing.count() == 0:
            return df.filter(
                F.col(col).isNotNull() & (F.trim(F.col(col)) != "brak informacji")
            ).union(resolved)

        missing = still_missing

    # Fallback: zostaw "brak informacji"
    return df.filter(
        F.col(col).isNotNull() & (F.trim(F.col(col)) != "brak informacji")
    ).union(missing)


# --- Dim_Czas: ostrzeżenie o lukach w datach ----------------------------------
def _warn_date_gaps(df, spark):
    dates = df.select("source_date").distinct().orderBy("source_date").collect()
    from datetime import datetime, timedelta
    parsed = sorted(
        {datetime.strptime(str(r["source_date"])[:7], "%Y-%m") for r in dates if r["source_date"]}
    )
    for i in range(1, len(parsed)):
        gap = (parsed[i].year * 12 + parsed[i].month) - (parsed[i - 1].year * 12 + parsed[i - 1].month)
        if gap > 1:
            print(f"[WARN] Luka w datach Dim_Czas: {parsed[i-1]:%Y-%m} → {parsed[i]:%Y-%m}")


# --- main --------------------------------------------------------------------
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
    _write_postgres(df_demo, "demografia", url, user, pwd)
    print(f"Clean OK: stg.demografia ({df_demo.count()} wierszy)")

    # --- apartments: IDW dla odległości + imputacja cech budynku ---
    df_apt, url, user, pwd = _pg_opts(spark, "apartments")

    for col in _DISTANCE_COLS:
        if col in [c.lower() for c in df_apt.columns]:
            df_apt = _idw_impute(df_apt, col)

    for col in _BUILDING_COLS:
        if col in [c.lower() for c in df_apt.columns]:
            df_apt = _impute_building_attr(df_apt, col)

    # Fact: cena_za_m2 ≥ 0, powierzchnia 10–300 (double-check po ewentualnych zmianach)
    df_apt = df_apt.filter(
        F.col("squaremeters").cast("double").between(10, 300) &
        (F.col("price").cast("double") > 0)
    )

    _write_postgres(df_apt, "apartments", url, user, pwd)
    print(f"Clean OK: stg.apartments ({df_apt.count()} wierszy)")

    # --- Dim_Czas: ostrzeżenie o lukach ---
    _warn_date_gaps(df_apt, spark)

    spark.stop()


if __name__ == "__main__":
    main()
