"""Ekstrakcja: uruchom skrypt i waliduj."""

import subprocess
from pathlib import Path

import pandas as pd

_ROOT = Path("/opt/airflow") if Path("/opt/airflow/dags").exists() else Path(__file__).resolve().parents[1]
_DATA = _ROOT / "data"

# script, args, sep, patterns, required_cols, date_col, dedup_keys, validators[(col, fn, reason)]
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
    pass


def _scripts_dir() -> Path:
    for path in (_ROOT / "scripts", Path(__file__).resolve().parents[1] / "scripts"):
        if path.is_dir():
            return path
    raise ExtractionError("Brak /opt/airflow/scripts — dodaj volume w docker-compose")


def _glob(pattern: str) -> list[Path]:
    return sorted(_ROOT.glob(pattern)) if "*" in pattern else [_ROOT / pattern]


def _split(df: pd.DataFrame, req: list[str], date_col: str | None, keys: list[str] | None, validators: list | None = None) -> pd.DataFrame:
    empty = lambda s: s.isna() | (s.astype(str).str.strip() == "")
    reasons = pd.Series("", index=df.index, dtype="object")
    for col in req:
        if col in df.columns:
            reasons = reasons.where(~empty(df[col]), reasons + f"brak_{col};")
    if date_col and date_col in df.columns:
        reasons = reasons.where(~empty(df[date_col]), reasons + "brak_daty;")
    if keys and all(c in df.columns for c in keys):
        reasons = reasons.where(~df.duplicated(subset=keys, keep="first"), reasons + "duplikat;")
    for col, fn, reason in (validators or []):
        if col in df.columns:
            reasons = reasons.where(fn(df[col]), reasons + f"{reason};")
    bad = reasons.str.strip() != ""
    return df[~bad].copy()


def run_extract(source_id: str) -> int:
    script, args, sep, patterns, req, date_col, keys, validators = _SOURCES[source_id]
    errors, rows_ok = [], 0

    files_exist = all(
        any(p.exists() and p.stat().st_size > 0 for p in _glob(pattern))
        for pattern in patterns
    )

    if files_exist:
        print(f"[{source_id}] Pliki już istnieją, pomijam pobieranie.")
    else:
        try:
            subprocess.run(["python", str(_scripts_dir() / script), *args], check=True, cwd=_scripts_dir())
        except subprocess.CalledProcessError as exc:
            raise ExtractionError(f"[{source_id}] {exc}") from exc

    frames = [
        (path, pd.read_csv(path, sep=sep, encoding="utf-8-sig"))
        for pattern in patterns
        for path in _glob(pattern)
        if path.exists() and path.stat().st_size
    ]
    if not frames:
        raise ExtractionError(f"[{source_id}] Brak plików po ekstrakcji")

    for path, df in frames:
        missing = [col for col in req if col not in df.columns]
        if missing:
            errors.append(f"{path.name}: brak kolumn {missing}")
            continue
        if df.empty:
            errors.append(f"{path.name}: pusty plik")
            continue

        good = _split(df, req, date_col, keys, validators)

        if not good.empty:
            rows_ok += len(good)
        else:
            errors.append(f"{path.name}: wszystkie wiersze niepoprawne")

    if errors:
        raise ExtractionError(f"[{source_id}] " + "; ".join(errors))

    return rows_ok
