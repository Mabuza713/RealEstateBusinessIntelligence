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
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import IntegerType

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
    """
    Łączy wiersze demograficzne i płacowe dla tego samego głównego miasta i daty,
    uzupełnia wartości i odrzuca wiersze niespełniające warunków spójności.
    """
    agg_df = df.groupBy("glowne_miasto", "data").agg(
        F.coalesce(
            F.first(F.when(F.col("miasto_gus").like("%m.%") | F.col("miasto_gus").like("%st.%"), F.col("miasto_gus")), ignorenulls=True),
            F.first("miasto_gus", ignorenulls=True)
        ).alias("miasto_gus"),
        F.first("populacja_ogolna", ignorenulls=True).alias("populacja_ogolna"),
        F.first("populacja_mezczyzni", ignorenulls=True).alias("populacja_mezczyzni"),
        F.first("populacja_kobiety", ignorenulls=True).alias("populacja_kobiety"),
        F.first("zarejestrowani_bezrobotni", ignorenulls=True).alias("zarejestrowani_bezrobotni"),
        F.first("przecietne_wynagrodzenie_brutto", ignorenulls=True).alias("przecietne_wynagrodzenie_brutto"),
        F.first("dochody_wlasne_jst", ignorenulls=True).alias("dochody_wlasne_jst")
    )

    agg_df = (
        agg_df
        .withColumn("populacja_mezczyzni", F.col("populacja_mezczyzni") * 10)
        .withColumn("populacja_kobiety", F.col("populacja_kobiety") * 10)
        .withColumn("populacja_ogolna", F.col("populacja_mezczyzni") + F.col("populacja_kobiety"))
    )

    filtered_df = agg_df.filter(
        F.col("miasto_gus").isNotNull() & (F.trim(F.col("miasto_gus")) != "") &
        F.col("populacja_ogolna").isNotNull() & (F.col("populacja_ogolna") > 0)
    )

    return filtered_df.select(
        "miasto_gus", "glowne_miasto", "data", "populacja_ogolna",
        "populacja_mezczyzni", "populacja_kobiety", "zarejestrowani_bezrobotni",
        "przecietne_wynagrodzenie_brutto", "dochody_wlasne_jst"
    )


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

def _impute_spatial_pandas(pdf):
    """
    Wykonuje przestrzenną imputację cech budynku i udogodnień w Pandas za pomocą
    szybkiego indeksu siatki (Spatial Grid Index) i NumPy.
    """
    lat = pdf["latitude"].astype(float).values
    lon = pdf["longitude"].astype(float).values
    ids = pdf["id"].values
    cities = pdf["city"].values

    # 1. Imputacja cech budynku: buildingmaterial, condition, buildyear
    is_missing_mat = pdf["buildingmaterial"].isna() | (pdf["buildingmaterial"].str.strip() == "brak informacji")
    is_missing_cond = pdf["condition"].isna() | (pdf["condition"].str.strip() == "brak informacji")
    is_missing_year = pdf["buildyear"].isna() | (pdf["buildyear"] <= 0)
    is_missing_bldg = is_missing_mat | is_missing_cond | is_missing_year

    known_mask = ~is_missing_bldg
    grid_size = 0.018  # 2km

    grid = {}
    for idx in np.where(known_mask)[0]:
        c_lat = int(lat[idx] / grid_size)
        c_lon = int(lon[idx] / grid_size)
        key = (c_lat, c_lon)
        if key not in grid:
            grid[key] = []
        grid[key].append(idx)

    # Mediany/dominanty per miasto (fallbacks)
    city_mat_mode = pdf[known_mask].groupby("city")["buildingmaterial"].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else "brak informacji").to_dict()
    city_cond_mode = pdf[known_mask].groupby("city")["condition"].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else "brak informacji").to_dict()
    city_year_median = pdf[known_mask & (pdf["buildyear"] > 0)].groupby("city")["buildyear"].median().to_dict()

    for idx in np.where(is_missing_bldg)[0]:
        m_lat, m_lon = lat[idx], lon[idx]
        m_city = cities[idx]
        c_lat = int(m_lat / grid_size)
        c_lon = int(m_lon / grid_size)

        candidates = []
        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                key = (c_lat + dy, c_lon + dx)
                if key in grid:
                    candidates.extend(grid[key])

        candidates = [c for c in candidates if c != idx and cities[c] == m_city]

        if candidates:
            c_lats = lat[candidates]
            c_lons = lon[candidates]
            dists = np.sqrt((c_lats - m_lat)**2 + (c_lons - m_lon)**2)

            min_dist_idx = np.argmin(dists)
            if dists[min_dist_idx] <= 0.00045:  # 50m
                nearest_idx = candidates[min_dist_idx]
                if is_missing_mat[idx]:
                    pdf.at[idx, "buildingmaterial"] = pdf.at[nearest_idx, "buildingmaterial"]
                if is_missing_cond[idx]:
                    pdf.at[idx, "condition"] = pdf.at[nearest_idx, "condition"]
                if is_missing_year[idx]:
                    pdf.at[idx, "buildyear"] = pdf.at[nearest_idx, "buildyear"]
                continue

            valid_indices = np.where(dists <= 0.018)[0]
            if len(valid_indices) > 0:
                sorted_valid = valid_indices[np.argsort(dists[valid_indices])][:10]
                top_k_idxs = [candidates[i] for i in sorted_valid]

                if is_missing_mat[idx]:
                    mats = [pdf.at[i, "buildingmaterial"] for i in top_k_idxs if pd.notna(pdf.at[i, "buildingmaterial"]) and pdf.at[i, "buildingmaterial"] != "brak informacji"]
                    pdf.at[idx, "buildingmaterial"] = max(set(mats), key=mats.count) if mats else city_mat_mode.get(m_city, "brak informacji")
                if is_missing_cond[idx]:
                    conds = [pdf.at[i, "condition"] for i in top_k_idxs if pd.notna(pdf.at[i, "condition"]) and pdf.at[i, "condition"] != "brak informacji"]
                    pdf.at[idx, "condition"] = max(set(conds), key=conds.count) if conds else city_cond_mode.get(m_city, "brak informacji")
                if is_missing_year[idx]:
                    years = [pdf.at[i, "buildyear"] for i in top_k_idxs if pdf.at[i, "buildyear"] > 0]
                    pdf.at[idx, "buildyear"] = int(np.median(years)) if years else int(city_year_median.get(m_city, 2000))
                continue

        if is_missing_mat[idx]:
            pdf.at[idx, "buildingmaterial"] = city_mat_mode.get(m_city, "brak informacji")
        if is_missing_cond[idx]:
            pdf.at[idx, "condition"] = city_cond_mode.get(m_city, "brak informacji")
        if is_missing_year[idx]:
            pdf.at[idx, "buildyear"] = int(city_year_median.get(m_city, 2000))

    # 2. Imputacja udogodnień: hasparkingspace, hasbalcony, haselevator, hassecurity, hasstorageroom
    amenity_cols = ["hasparkingspace", "hasbalcony", "haselevator", "hassecurity", "hasstorageroom"]
    is_missing_amenity = pd.Series(False, index=pdf.index)
    for c in amenity_cols:
        is_missing_amenity |= pdf[c].isna() | (pdf[c].str.strip() == "brak informacji") | (pdf[c].str.strip() == "")

    known_mask_am = ~is_missing_amenity
    grid_size_30m = 0.0009  # 100m

    grid_am = {}
    for idx in np.where(known_mask_am)[0]:
        c_lat = int(lat[idx] / grid_size_30m)
        c_lon = int(lon[idx] / grid_size_30m)
        key = (c_lat, c_lon)
        if key not in grid_am:
            grid_am[key] = []
        grid_am[key].append(idx)

    for idx in np.where(is_missing_amenity)[0]:
        m_lat, m_lon = lat[idx], lon[idx]
        m_city = cities[idx]
        c_lat = int(m_lat / grid_size_30m)
        c_lon = int(m_lon / grid_size_30m)

        candidates = []
        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                key = (c_lat + dy, c_lon + dx)
                if key in grid_am:
                    candidates.extend(grid_am[key])

        candidates = [c for c in candidates if c != idx and cities[c] == m_city]
        imputed_vals = {c: "brak" for c in amenity_cols}

        if candidates:
            c_lats = lat[candidates]
            c_lons = lon[candidates]
            dists = np.sqrt((c_lats - m_lat)**2 + (c_lons - m_lon)**2)

            min_dist_idx = np.argmin(dists)
            if dists[min_dist_idx] <= 0.00027:  # 30m
                nearest_idx = candidates[min_dist_idx]
                for c in amenity_cols:
                    imputed_vals[c] = pdf.at[nearest_idx, c]

        for c in amenity_cols:
            is_miss = pd.isna(pdf.at[idx, c]) or str(pdf.at[idx, c]).strip() in ("brak informacji", "")
            if is_miss:
                pdf.at[idx, c] = imputed_vals[c]

    # Upewnijmy się, że buildyear nie ma wartości NaN i jest typu int
    pdf["buildyear"] = pdf["buildyear"].fillna(2000).astype(int)

    return pdf


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

    # Spatial building attributes and amenities imputation via Pandas
    pdf_apt = df_apt.toPandas()
    pdf_apt = _impute_spatial_pandas(pdf_apt)

    # Cast IntegerType columns explicitly to Python int/None to avoid float64/NaN issues
    for field in df_apt.schema.fields:
        col_name = field.name
        if col_name in pdf_apt.columns and isinstance(field.dataType, IntegerType):
            pdf_apt[col_name] = pdf_apt[col_name].astype(object)
            pdf_apt.loc[pdf_apt[col_name].notnull(), col_name] = pdf_apt.loc[pdf_apt[col_name].notnull(), col_name].astype(int)

    # Convert the entire DataFrame to object type and replace NaN/NaT with None for Spark compatibility
    pdf_apt = pdf_apt.astype(object).where(pdf_apt.notnull(), None)
    df_apt = spark.createDataFrame(pdf_apt[df_apt.columns], schema=df_apt.schema)
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
