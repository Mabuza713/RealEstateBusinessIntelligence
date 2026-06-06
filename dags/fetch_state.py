"""Ekstrakcja: uruchom skrypt, waliduj, zapisz clean/rejected."""

import subprocess
from pathlib import Path

import pandas as pd

_ROOT = Path("/opt/airflow") if Path("/opt/airflow/dags").exists() else Path(__file__).resolve().parents[1]
_DATA = _ROOT / "data"

# script, args, sep, patterns, required_cols, date_col, dedup_keys
_SOURCES = {
    "real_estate": ("get_real_estate_data.py", [], ",", ["**/all_apartments_sell.csv", "**/all_apartments_rent.csv"], ["source_date"], "source_date", None),
    "population": ("get_population_data.py", ["--rok_od", "2023", "--rok_do", "2025"], ",", ["data/raw/baza_bi_miasta_*.csv"], ["Miasto_GUS", "Glowne_Miasto", "Data"], "Data", ["Miasto_GUS", "Data"]),
    "life_cost": ("get_life_cost_data.py", [], ";", ["data/raw/poland_real_estate_monthly*.csv"], ["Date"], "Date", ["Date"]),
    "overpass": ("get_overpass_data.py", [], ";", ["data/raw/all_cafes.csv", "data/raw/all_parkings.csv", "data/raw/all_bus_stops.csv"], ["City", "LAT", "LON"], None, ["City", "LAT", "LON"]),
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


def _split(df: pd.DataFrame, req: list[str], date_col: str | None, keys: list[str] | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    empty = lambda s: s.isna() | (s.astype(str).str.strip() == "")
    reasons = pd.Series("", index=df.index, dtype="object")
    for col in req:
        if col in df.columns:
            reasons = reasons.where(~empty(df[col]), reasons + f"brak_{col};")
    if date_col and date_col in df.columns:
        reasons = reasons.where(~empty(df[date_col]), reasons + "brak_daty;")
    if keys and all(c in df.columns for c in keys):
        reasons = reasons.where(~df.duplicated(subset=keys, keep="first"), reasons + "duplikat;")
    bad = reasons.str.strip() != ""
    good, rejected = df[~bad].copy(), df[bad].copy()
    if not rejected.empty:
        rejected["_rejection_reason"] = reasons[bad].str.strip(";")
    return good, rejected


def run_extract(source_id: str) -> tuple[int, int]:
    script, args, sep, patterns, req, date_col, keys = _SOURCES[source_id]
    errors, warnings, rows_ok, rows_bad = [], [], 0, 0

    try:
        subprocess.run(["python", str(_scripts_dir() / script), *args], check=True, cwd=_scripts_dir())
    except subprocess.CalledProcessError as exc:
        raise ExtractionError(f"[{source_id}] {exc}") from exc

    clean_dir, reject_dir = _DATA / "clean", _DATA / "rejected"
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

        good, bad = _split(df, req, date_col, keys)
        clean_dir.mkdir(parents=True, exist_ok=True)
        reject_dir.mkdir(parents=True, exist_ok=True)

        if not good.empty:
            good.to_csv(clean_dir / path.name, index=False, encoding="utf-8-sig", sep=sep)
            rows_ok += len(good)
        if not bad.empty:
            bad.to_csv(reject_dir / path.name, index=False, encoding="utf-8-sig", sep=sep)
            rows_bad += len(bad)
            warnings.append(f"{path.name}: {len(bad)} odrzuconych")
        if good.empty:
            errors.append(f"{path.name}: wszystkie wiersze odrzucone")

    if errors:
        raise ExtractionError(f"[{source_id}] " + "; ".join(errors))
    if warnings:
        print(f"[WARN] [{source_id}] " + "; ".join(warnings))

    return rows_ok, rows_bad
