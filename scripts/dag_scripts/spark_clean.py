"""
Krok II potoku transformacji Sparka: Czyszczenie i Imputacja.
Skrypt odczytuje tabele ze stagingu (stg.*), dokonuje zaawansowanego czyszczenia
demografii oraz dwufazowej imputacji braków przestrzennych dla ofert mieszkań (IDW
oraz Spatial Grid Index) w Pandas/NumPy zrównoleglonym na executorach Sparka.
"""

import os
from datetime import datetime
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import IntegerType, DecimalType

# Lista kolumn odległościowych podlegających imputacji IDW
_DISTANCE_COLS = [
    "schooldistance", "clinicdistance", "postofficedistance",
    "kindergartendistance", "restaurantdistance", "collegedistance",
    "pharmacydistance", "centredistance",
    "busstopdistance", "caffedistance", "parkingdistance",
]

_BUILDING_COLS = ["buildingmaterial", "condition"]

# Maksymalna liczba sąsiadów brana pod uwagę przy uśrednianiu IDW
_IDW_K = 10


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------

def _pg_opts(spark, table):
    """Odczytuje tabelę stagingową z bazy PostgreSQL i zwraca ramkę danych Spark oraz parametry połączenia."""
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
    """Zapisuje wyczyszczoną ramkę danych z powrotem do bazy PostgreSQL w trybie overwrite z truncate."""
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
    Agreguje wskaźniki demograficzne GUS dla tego samego miasta i daty (first ignorenulls),
    koryguje błąd skali populacji GUS (mnożnik 10) i wylicza sumaryczną populację ogólną.
    """
    # Konsolidacja wielu rekordów z tego samego dnia (GUS zwraca osobne wiersze)
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

    # Korekta systematycznego błędu skali płci z API GUS (skala 1:10) i wyliczenie sumy
    agg_df = (
        agg_df
        .withColumn("populacja_mezczyzni", F.col("populacja_mezczyzni") * 10)
        .withColumn("populacja_kobiety", F.col("populacja_kobiety") * 10)
        .withColumn("populacja_ogolna", F.col("populacja_mezczyzni") + F.col("populacja_kobiety"))
    )

    # Filtrowanie wierszy z błędną lub pustą populacją / pensją
    filtered_df = agg_df.filter(
        F.col("miasto_gus").isNotNull() & (F.trim(F.col("miasto_gus")) != "") &
        (
            (F.col("populacja_ogolna").isNotNull() & (F.col("populacja_ogolna") > 0)) |
            F.col("przecietne_wynagrodzenie_brutto").isNotNull()
        )
    )

    return filtered_df.select(
        "miasto_gus", "glowne_miasto", "data", "populacja_ogolna",
        "populacja_mezczyzni", "populacja_kobiety", "zarejestrowani_bezrobotni",
        "przecietne_wynagrodzenie_brutto", "dochody_wlasne_jst"
    )


# ---------------------------------------------------------------------------
# IDW przez grid-bucketing (Pandas/NumPy)
# ---------------------------------------------------------------------------

def _idw_impute_pandas(pdf, col):
    """
    Wektorowa implementacja Inverse Distance Weighting w Pandas/NumPy.
    Dla brakujących współrzędnych wyszukuje 10 najbliższych punktów w tym samym mieście,
    liczy odległości euklidesowe, a następnie wyznacza średnią ważoną (waga = 1/d).
    W celu uniknięcia OOM dzieli proces na paczki po 1000 wierszy.
    """
    if col not in pdf.columns:
        return pdf
    
    is_null = pdf[col].isna()
    if not is_null.any():
        return pdf
        
    known_mask = ~is_null
    # Obliczenie mediany z poziomu miasta jako fallback
    city_medians = pdf[known_mask].groupby("city")[col].median().to_dict()
    
    lat = pdf["latitude"].astype(float).values
    lon = pdf["longitude"].astype(float).values
    cities = pdf["city"].values
    vals = pdf[col].astype(float).values
    
    unique_cities = pdf["city"].unique()
    
    # Proces przebiega niezależnie per miasto
    for city in unique_cities:
        city_mask = (cities == city)
        city_missing_mask = city_mask & is_null
        city_known_mask = city_mask & known_mask
        
        num_missing = np.sum(city_missing_mask)
        num_known = np.sum(city_known_mask)
        
        if num_missing == 0:
            continue
            
        fallback_val = city_medians.get(city, 0.0)
        if num_known == 0:
            pdf.loc[city_missing_mask, col] = fallback_val
            continue
            
        m_idx = np.where(city_missing_mask)[0]
        k_idx = np.where(city_known_mask)[0]
        
        m_lat, m_lon = lat[m_idx], lon[m_idx]
        k_lat, k_lon = lat[k_idx], lon[k_idx]
        k_vals = vals[k_idx]
        
        k_limit = min(10, num_known)
        batch_size = 1000
        
        # Przetwarzanie w bezpiecznych dla RAMu paczkach (batching)
        for i in range(0, num_missing, batch_size):
            batch_m_idx = m_idx[i : i + batch_size]
            batch_m_lat = m_lat[i : i + batch_size]
            batch_m_lon = m_lon[i : i + batch_size]
            
            # Wektorowe obliczanie macierzy odległości przy użyciu broadcasting NumPy
            dists = np.sqrt((batch_m_lat[:, None] - k_lat) ** 2 + (batch_m_lon[:, None] - k_lon) ** 2)
            
            # Wyszukanie 10 najbliższych sąsiadów
            partitioned_idx = np.argpartition(dists, k_limit - 1, axis=1)[:, :k_limit]
            
            row_idx = np.arange(len(batch_m_idx))[:, None]
            sub_dists = dists[row_idx, partitioned_idx]
            sorted_sub_idx = np.argsort(sub_dists, axis=1)
            
            nearest_partition_idx = partitioned_idx[row_idx, sorted_sub_idx]
            nearest_dists = dists[row_idx, nearest_partition_idx]
            
            # Dodanie małej stałej w celu uniknięcia dzielenia przez zero przy odległości 0m
            nearest_dists = np.maximum(nearest_dists, 1e-9)
            weights = 1.0 / nearest_dists
            
            # Średnia ważona
            weighted_vals = np.sum(weights * k_vals[nearest_partition_idx], axis=1)
            sum_weights = np.sum(weights, axis=1)
            
            imputed_vals = np.where(sum_weights > 0, weighted_vals / sum_weights, fallback_val)
            pdf.loc[pdf.index[batch_m_idx], col] = np.round(imputed_vals, 2)
        
    return pdf


# ---------------------------------------------------------------------------
# Spatial Grid Indexing (K-NN w NumPy)
# ---------------------------------------------------------------------------

def _impute_spatial_pandas(pdf):
    """
    Uzupełnia braki w cechach budynków (materiał, stan, rok, typ) oraz udogodnieniach
    przy użyciu zoptymalizowanego indeksu siatki przestrzennej (Spatial Grid Index).
    
    1. Cechy budynków (siatka ~2km):
       - Bliskie sąsiedztwo (<= 150m) -> kopiuje wartość najbliższego lokalu.
       - Dalsze sąsiedztwo (do 15km) -> moda/mediana z 20 sąsiadów.
       - Fallback -> dominanta per miasto.
    2. Udogodnienia (siatka ~100m):
       - Bardzo bliskie sąsiedztwo (<= 30m) -> kopiuje windę/parking (ten sam blok).
       - Fallback -> przypisuje "brak".
    """
    lat = pdf["latitude"].astype(float).values
    lon = pdf["longitude"].astype(float).values
    ids = pdf["id"].values
    cities = pdf["city"].values

    # Sprawdzenie brakujących wartości
    is_missing_mat  = pdf["buildingmaterial"].isna() | (pdf["buildingmaterial"].str.strip().isin(["brak informacji", ""]))
    is_missing_cond = pdf["condition"].isna()         | (pdf["condition"].str.strip().isin(["brak informacji", ""]))
    is_missing_year = pdf["buildyear"].isna()         | (pdf["buildyear"] <= 0)
    is_missing_type = pdf["type"].isna()              | (pdf["type"].str.strip().isin(["brak informacji", ""]))
    is_missing_bldg = is_missing_mat | is_missing_cond | is_missing_year | is_missing_type

    grid_size = 0.018  # Siatka ~2km do cech budynków

    # Tworzenie siatek hash-mapy dla szybkiego dostępu przestrzennego
    def _build_grid(mask):
        g = {}
        for i in np.where(mask)[0]:
            key = (int(lat[i] / grid_size), int(lon[i] / grid_size))
            g.setdefault(key, []).append(i)
        return g

    grid_mat  = _build_grid(~is_missing_mat)
    grid_cond = _build_grid(~is_missing_cond)
    grid_year = _build_grid(~is_missing_year)
    grid_type = _build_grid(~is_missing_type)

    # Wyznaczenie globalnych wartości fallback per miasto
    city_mat_mode    = pdf[~is_missing_mat].groupby("city")["buildingmaterial"].agg(
        lambda x: x.mode().iloc[0] if not x.mode().empty else "brak informacji").to_dict()
    city_cond_mode   = pdf[~is_missing_cond].groupby("city")["condition"].agg(
        lambda x: x.mode().iloc[0] if not x.mode().empty else "brak informacji").to_dict()
    city_year_median = pdf[~is_missing_year & (pdf["buildyear"] > 0)].groupby("city")["buildyear"].median().to_dict()
    city_type_mode   = pdf[~is_missing_type].groupby("city")["type"].agg(
        lambda x: x.mode().iloc[0] if not x.mode().empty else "brak informacji").to_dict()

    def _nearby(grid, idx, m_lat, m_lon, m_city):
        """Zwraca kandydatów z 9 sąsiednich komórek siatki (3x3) w tym samym mieście."""
        c_lat, c_lon = int(m_lat / grid_size), int(m_lon / grid_size)
        cands = []
        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                cands.extend(grid.get((c_lat + dy, c_lon + dx), []))
        return [c for c in cands if c != idx and cities[c] == m_city]
        
    _NEAR_THRESH  = 0.00135  # ~150 metrów
    _VOTE_RADIUS  = 0.135    # ~15 kilometrów
    _TOP_K        = 20
    
    def _impute_categorical(idx, m_lat, m_lon, m_city, grid, col, fallback):
        cands = _nearby(grid, idx, m_lat, m_lon, m_city)
        if not cands:
            return "brak informacji"
        c_lats, c_lons = lat[cands], lon[cands]
        dists = np.sqrt((c_lats - m_lat) ** 2 + (c_lons - m_lon) ** 2)
        nearest = int(np.argmin(dists))
        # Faza I: Kopiowanie z bliskiego sąsiedztwa
        if dists[nearest] <= _NEAR_THRESH:
            v = pdf.at[cands[nearest], col]
            if pd.notna(v) and str(v).strip() not in ("brak informacji", ""):
                return v
        # Faza II: Głosowanie większościowe z dalszego sąsiedztwa
        valid = np.where(dists <= _VOTE_RADIUS)[0]
        if len(valid) == 0:
            return "brak informacji"
        top_k = [cands[i] for i in valid[np.argsort(dists[valid])][:_TOP_K]]
        vals = [pdf.at[i, col] for i in top_k
                if pd.notna(pdf.at[i, col]) and str(pdf.at[i, col]).strip() not in ("brak informacji", "")]
        return max(set(vals), key=vals.count) if vals else "brak informacji"

    def _impute_year(idx, m_lat, m_lon, m_city):
        cands = _nearby(grid_year, idx, m_lat, m_lon, m_city)
        fallback = int(city_year_median.get(m_city, 2000))
        if not cands:
            return fallback
        c_lats, c_lons = lat[cands], lon[cands]
        dists = np.sqrt((c_lats - m_lat) ** 2 + (c_lons - m_lon) ** 2)
        nearest = int(np.argmin(dists))
        if dists[nearest] <= _NEAR_THRESH:
            return int(pdf.at[cands[nearest], "buildyear"])
        valid = np.where(dists <= _VOTE_RADIUS)[0]
        if len(valid) == 0:
            return fallback
        top_k = [cands[i] for i in valid[np.argsort(dists[valid])][:_TOP_K]]
        years = [pdf.at[i, "buildyear"] for i in top_k if pdf.at[i, "buildyear"] > 0]
        return int(np.median(years)) if years else fallback

    # Uruchomienie imputacji cech budynków
    for idx in np.where(is_missing_bldg)[0]:
        m_lat, m_lon, m_city = lat[idx], lon[idx], cities[idx]
        if is_missing_mat[idx]:
            pdf.at[idx, "buildingmaterial"] = _impute_categorical(
                idx, m_lat, m_lon, m_city, grid_mat, "buildingmaterial",
                city_mat_mode.get(m_city, "brak informacji"))
        if is_missing_cond[idx]:
            pdf.at[idx, "condition"] = _impute_categorical(
                idx, m_lat, m_lon, m_city, grid_cond, "condition",
                city_cond_mode.get(m_city, "brak informacji"))
        if is_missing_year[idx]:
            pdf.at[idx, "buildyear"] = _impute_year(idx, m_lat, m_lon, m_city)
        if is_missing_type[idx]:
            pdf.at[idx, "type"] = _impute_categorical(
                idx, m_lat, m_lon, m_city, grid_type, "type",
                city_type_mode.get(m_city, "brak informacji"))


    # 2. Imputacja udogodnień (bardzo mały promień siatki - ten sam blok mieszkalny)
    amenity_cols = ["hasparkingspace", "hasbalcony", "haselevator", "hassecurity", "hasstorageroom"]
    is_missing_amenity = pd.Series(False, index=pdf.index)
    for c in amenity_cols:
        is_missing_amenity |= pdf[c].isna() | (pdf[c].str.strip() == "brak informacji") | (pdf[c].str.strip() == "")

    known_mask_am = ~is_missing_amenity
    grid_size_30m = 0.0009  # Siatka ~100m

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
            if dists[min_dist_idx] <= 0.00027:  # Promień 30 metrów (ten sam blok)
                nearest_idx = candidates[min_dist_idx]
                for c in amenity_cols:
                    imputed_vals[c] = pdf.at[nearest_idx, c]

        for c in amenity_cols:
            is_miss = pd.isna(pdf.at[idx, c]) or str(pdf.at[idx, c]).strip() in ("brak informacji", "")
            if is_miss:
                pdf.at[idx, c] = imputed_vals[c]

    pdf["buildyear"] = pdf["buildyear"].fillna(2000).astype(int)
    return pdf


# ---------------------------------------------------------------------------
# Dim_Czas: ostrzeżenie o lukach w datach
# ---------------------------------------------------------------------------

def _warn_date_gaps(df):
    """Wyświetla ostrzeżenie diagnostyczne [WARN] w przypadku wykrycia dziur (brakujących miesięcy) w chronologii."""
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

    # --- 1. Oczyszczanie danych demograficznych ---
    df_demo, url, user, pwd = _pg_opts(spark, "demografia")
    df_demo = _clean_demografia(df_demo)
    df_demo = df_demo.localCheckpoint()
    _write_postgres(df_demo, "demografia", url, user, pwd)
    print(f"Clean OK: stg.demografia ({df_demo.count()} wierszy)")

    # --- 2. Oczyszczanie i przestrzenna imputacja ofert mieszkań ---
    df_apt, url, user, pwd = _pg_opts(spark, "apartments")

    schema_cols = df_apt.columns
    int_fields = [f.name for f in df_apt.schema.fields if isinstance(f.dataType, IntegerType)]
    decimal_fields = [(f.name, f.dataType.scale) for f in df_apt.schema.fields if isinstance(f.dataType, DecimalType)]

    # Dystrybucja i zrównoleglenie skomplikowanych obliczeń przestrzennych na executorach Spark
    def clean_group(key, pdf):
        from decimal import Decimal

        # Imputacja IDW odległości w Pandas
        for dist_col in _DISTANCE_COLS:
            pdf = _idw_impute_pandas(pdf, dist_col)

        # Imputacja cech budynków za pomocą Spatial Grid Index
        pdf = _impute_spatial_pandas(pdf)

        # Rzutowanie typów w Pandas w celu uniknięcia niezgodności typów PyArrow z silnikiem Spark
        for col_name in int_fields:
            if col_name in pdf.columns:
                pdf[col_name] = pd.to_numeric(pdf[col_name], errors='coerce')
                pdf[col_name] = pdf[col_name].astype(object).where(pdf[col_name].notnull(), None)
                pdf.loc[pdf[col_name].notnull(), col_name] = pdf.loc[pdf[col_name].notnull(), col_name].astype(int)

        for col_name, scale in decimal_fields:
            if col_name in pdf.columns:
                pdf[col_name] = pd.to_numeric(pdf[col_name], errors='coerce')
                pdf[col_name] = pdf[col_name].apply(lambda x: Decimal(f"{x:.{scale}f}") if pd.notna(x) else None)

        pdf = pdf.astype(object).where(pdf.notnull(), None)
        return pdf[schema_cols]

    # Uruchomienie czyszczenia w podziale per miasto
    df_apt = df_apt.groupBy("city").applyInPandas(clean_group, schema=df_apt.schema)
    df_apt = df_apt.localCheckpoint()

    # Ostateczny filtr jakości po imputacji
    df_apt = df_apt.filter(
        F.col("squaremeters").cast("double").between(10, 300) &
        (F.col("price").cast("double") > 0)
    )

    _write_postgres(df_apt, "apartments", url, user, pwd)
    print(f"Clean OK: stg.apartments ({df_apt.count()} wierszy)")

    # Wykrywanie luk chronologicznych
    _warn_date_gaps(df_apt)

    spark.stop()


if __name__ == "__main__":
    main()
