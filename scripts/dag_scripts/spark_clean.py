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

_IDW_K = 10          # max neighbors to average


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
# Dim_Demografia: verify population sum
# ---------------------------------------------------------------------------

def _clean_demografia(df):
    """
    Merge demographic and wage rows for the same city and date,
    fill in missing values, and drop rows that fail consistency checks.
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

def _idw_impute(df, col):
    if col not in df.columns or df.filter(F.col(col).isNull()).count() == 0:
        return df

    # 1. Podział na rekordy ze znanymi wartościami i szukające wartości
    known = df.filter(F.col(col).isNotNull()).select(
        "city",
        F.col("latitude").alias("_lat_k"),
        F.col("longitude").alias("_lon_k"),
        F.col(col).cast("double").alias("_val_k")
    )
    missing = df.filter(F.col(col).isNull())

    # 2. Złączenie po mieście i obliczenie odległości
    joined = missing.join(known, "city", "left") \
        .withColumn("_dist", F.sqrt(F.pow(F.col("latitude").cast("double") - F.col("_lat_k"), 2) + 
                                    F.pow(F.col("longitude").cast("double") - F.col("_lon_k"), 2))) \
        .filter(F.col("_val_k").isNotNull() & (F.col("_dist") > 0))

    # 3. Obliczenie IDW (średnia ważona odwrotnością odległości dla 10 najbliższych sąsiadów w mieście)
    win = Window.partitionBy("id", "listing_type").orderBy("_dist")
    agg = joined.withColumn("_rn", F.row_number().over(win)) \
                .filter(F.col("_rn") <= _IDW_K) \
                .withColumn("_w", F.lit(1.0) / F.col("_dist")) \
                .groupBy("id", "listing_type") \
                .agg((F.sum(F.col("_w") * F.col("_val_k")) / F.sum("_w")).alias("_imp"))

    # 4. Uzupełnienie wartości (imputacja lub fallback do mediany miasta)
    medians = df.filter(F.col(col).isNotNull()).groupBy("city") \
                .agg(F.percentile_approx(F.col(col).cast("double"), 0.5).alias("_med"))

    imputed = missing.join(agg, ["id", "listing_type"], "left") \
                     .join(medians, "city", "left") \
                     .withColumn(col, F.coalesce(F.round(F.col("_imp"), 2), F.col("_med"))) \
                     .select(df.columns)

    return df.filter(F.col(col).isNotNull()).union(imputed)



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
    """Print a warning for each missing month in the source_date sequence."""
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
            print(f"[WARN] Date gap in Dim_Czas: {parsed[i-1]:%Y-%m} -> {parsed[i]:%Y-%m}")


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

    # IDW per distance column (grid-bucketing, no cross-join)
    for dist_col in _DISTANCE_COLS:
        df_apt = _idw_impute(df_apt, dist_col)
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

    # Final quality check
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
