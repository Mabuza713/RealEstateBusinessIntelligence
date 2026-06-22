"""
Moduł odpowiedzialny za walidację poprawności wczytywanych danych wejściowych ("Pandas Split Engine")
oraz zarządzanie uruchamianiem skryptów ekstrakcji w ramach orkiestracji Airflow.
"""

import subprocess
from pathlib import Path
import pandas as pd

# Ustalenie katalogu głównego projektu w zależności od środowiska (lokalne vs Airflow Docker)
_ROOT = Path("/opt/airflow") if Path("/opt/airflow/dags").exists() else Path(__file__).resolve().parents[1]
_DATA = _ROOT / "data"

# Definicje źródeł danych: skrypty, argumenty, separatory, oczekiwane kolumny,
# klucze unikalne do deduplikacji oraz reguły walidacyjne (Pandas Split Engine)
_SOURCES = {
    "real_estate": (
        "get_real_estate_data.py", [], ",",
        ["data/raw/all_apartments_sell.csv", "data/raw/all_apartments_rent.csv"],
        ["source_date"], "source_date", None,
        [
            ("squareMeters", lambda s: pd.to_numeric(s, errors="coerce").between(10, 300), "squareMeters_out_of_range"),
            ("price",        lambda s: pd.to_numeric(s, errors="coerce") > 0,               "price_invalid"),
        ],
    ),
    "population": (
        "get_population_data.py", ["--rok_od", "2023", "--rok_do", "2025"], ",",
        ["data/raw/baza_bi_miasta_*.csv"],
        ["Miasto_GUS", "Glowne_Miasto", "Data"], "Data", ["Miasto_GUS", "Data"],
        [
            ("Populacja_Ogolna", lambda s: pd.to_numeric(s, errors="coerce") > 0, "populacja_invalid"),
        ],
    ),
    "overpass": (
        "get_overpass_data.py", [], ";",
        ["data/raw/all_cafes.csv", "data/raw/all_parkings.csv", "data/raw/all_bus_stops.csv"],
        ["City", "LAT", "LON"], None, ["City", "LAT", "LON"],
        [
            ("LAT", lambda s: pd.to_numeric(s, errors="coerce") >= 0, "lat_invalid"),
            ("LON", lambda s: pd.to_numeric(s, errors="coerce") >= 0, "lon_invalid"),
        ],
    ),
}


class ExtractionError(Exception):
    """Wyjątek zgłaszany w przypadku krytycznych błędów pobierania lub walidacji danych."""
    pass


def _scripts_dir() -> Path:
    """Zwraca absolutną ścieżkę do katalogu ze skryptami Pythona."""
    for path in (_ROOT / "scripts", Path(__file__).resolve().parents[1] / "scripts"):
        if path.is_dir():
            return path
    raise ExtractionError("Brak /opt/airflow/scripts — dodaj volume w docker-compose")


def _glob(pattern: str) -> list[Path]:
    """Pomocnicza metoda do wyszukiwania plików (glob) na podstawie wzorca tekstowego."""
    return sorted(_ROOT.glob(pattern)) if "*" in pattern else [_ROOT / pattern]


def _split(df: pd.DataFrame, req: list[str], date_col: str | None, keys: list[str] | None, validators: list | None = None) -> pd.DataFrame:
    """
    Pandas Split Engine: Waliduje wiersze ramki danych Pandas.
    Identyfikuje rekordy z brakami w kluczowych polach, brakami dat, duplikatami oraz
    błędami logicznymi zdefiniowanymi w regułach 'validators'.
    Zwraca przefiltrowaną, poprawną ramkę danych.
    """
    empty = lambda s: s.isna() | (s.astype(str).str.strip() == "")
    reasons = pd.Series("", index=df.index, dtype="object")
    
    # Weryfikacja obecności wymaganych kolumn
    for col in req:
        if col in df.columns:
            reasons = reasons.where(~empty(df[col]), reasons + f"brak_{col};")
            
    # Weryfikacja obecności poprawnej daty
    if date_col and date_col in df.columns:
        reasons = reasons.where(~empty(df[date_col]), reasons + "brak_daty;")
        
    # Weryfikacja i eliminacja duplikatów (na podstawie zdefiniowanych kluczy unikalnych)
    if keys and all(c in df.columns for c in keys):
        reasons = reasons.where(~df.duplicated(subset=keys, keep="first"), reasons + "duplikat;")
        
    # Uruchamianie dodatkowych walidatorów (np. zakres powierzchni, dodatnia cena)
    for col, fn, reason in (validators or []):
        if col in df.columns:
            reasons = reasons.where(fn(df[col]), reasons + f"{reason};")
            
    # Odrzucenie wadliwych rekordów (posiadających jakikolwiek zapisany kod błędu w 'reasons')
    bad = reasons.str.strip() != ""
    return df[~bad].copy()


def get_latest_month_from_db() -> str | None:
    """
    Łączy się bezpośrednio z PostgreSQL, aby sprawdzić najnowszy załadowany miesiąc (source_date).
    Służy do przyrostowego pobierania danych o nieruchomościach z Kaggle.
    """
    import os
    import psycopg2
    
    pg_user = os.environ.get("POSTGRES_USER", "postgres")
    pg_password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    pg_db = os.environ.get("POSTGRES_DB", "postgres")
    pg_host = os.environ.get("POSTGRES_HOST", "postgres")
    pg_port = os.environ.get("POSTGRES_PORT", "5432")
    
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=pg_db,
            user=pg_user,
            password=pg_password,
            host=pg_host,
            port=pg_port
        )
        with conn.cursor() as cur:
            # 1. Sprawdzamy czy tabela prod.dim_czas istnieje i ma rekordy
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'prod' 
                    AND table_name = 'dim_czas'
                );
            """)
            if cur.fetchone()[0]:
                cur.execute("SELECT MAX(source_date) FROM prod.dim_czas;")
                res = cur.fetchone()
                if res and res[0]:
                    return str(res[0])[:7]  # Zwraca format YYYY-MM
            
            # 2. Sprawdzamy czy tabela stg.apartments istnieje (fallback)
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'stg' 
                    AND table_name = 'apartments'
                );
            """)
            if cur.fetchone()[0]:
                cur.execute("SELECT MAX(source_date) FROM stg.apartments;")
                res = cur.fetchone()
                if res and res[0]:
                    return str(res[0])[:7]
    except Exception as e:
        print(f"Error querying database for latest month: {e}")
    finally:
        if conn:
            conn.close()
    return None


def run_extract(source_id: str, check_existing: bool = True) -> int:
    """
    Uruchamia skrypt ekstrakcji dla wybranego źródła danych i przeprowadza walidację Pandas.
    Wspiera przyrostowość (pyta bazę danych o ostatni miesiąc i przekazuje go do parametru --after).
    W przypadku braku nowych danych w Airflow rzuca AirflowSkipException.
    """
    import os
    script, args, sep, patterns, req, date_col, keys, validators = _SOURCES[source_id]
    errors, rows_ok = [], 0

    run_args = [*args]
    force_full_load = os.environ.get("FORCE_FULL_LOAD", "false").lower() == "true"
    
    latest_month = None
    # Pobieranie przyrostowe dla rynku nieruchomości
    if source_id == "real_estate" and not force_full_load:
        latest_month = get_latest_month_from_db()
        if latest_month:
            run_args.extend(["--after", latest_month])
            print(f"[{source_id}] Znaleziono ostatni miesiąc w bazie: {latest_month}. Pobieram tylko nowsze dane.")

    # Zapobieganie wielokrotnemu pobieraniu tych samych plików
    if check_existing:
        if source_id == "real_estate" and not force_full_load and latest_month:
            files_exist = False
        else:
            files_exist = all(
                any(p.exists() and p.stat().st_size > 0 for p in _glob(pattern))
                for pattern in patterns
            )
    else:
        files_exist = False

    # Pobieranie danych skryptem zewnętrznym, jeśli pliki nie istnieją
    if files_exist:
        print(f"[{source_id}] Pliki już istnieją, pomijam pobieranie.")
    else:
        try:
            subprocess.run(["python", str(_scripts_dir() / script), *run_args], check=True, cwd=_scripts_dir())
        except subprocess.CalledProcessError as exc:
            raise ExtractionError(f"[{source_id}] {exc}") from exc

    # Wczytanie i walidacja wyjściowych plików CSV za pomocą Pandas Split Engine
    frames = [
        (path, pd.read_csv(path, sep=sep, encoding="utf-8-sig"))
        for pattern in patterns
        for path in _glob(pattern)
        if path.exists() and path.stat().st_size
    ]
    if not frames:
        try:
            from airflow.exceptions import AirflowSkipException
            raise AirflowSkipException(f"[{source_id}] Brak nowych danych do przetworzenia.")
        except ImportError:
            raise ExtractionError(f"[{source_id}] Brak plików po ekstrakcji (brak nowych danych)")

    for path, df in frames:
        missing = [col for col in req if col not in df.columns]
        if missing:
            errors.append(f"{path.name}: brak kolumn {missing}")
            continue
        if df.empty:
            errors.append(f"{path.name}: pusty plik")
            continue

        # Uruchomienie filtracji na ramce danych Pandas
        good = _split(df, req, date_col, keys, validators)

        if not good.empty:
            rows_ok += len(good)
        else:
            errors.append(f"{path.name}: wszystkie wiersze niepoprawne")

    if errors:
        raise ExtractionError(f"[{source_id}] " + "; ".join(errors))

    return rows_ok
