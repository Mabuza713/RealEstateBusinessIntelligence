"""
Pobieranie wskaźników makroekonomicznych z API BDL (GUS).

Dla każdego wskaźnika skrypt automatycznie wykrywa dostępną częstotliwość:
  1. Próbuje miesięczne (year=YYYYMM)
  2. Jeśli brak danych → próbuje roczne (year=YYYY)

ID zmiennych BDL:
  461   – stopa bezrobocia rejestrowanego (%)
  2515  – przeciętne miesięczne wynagrodzenie brutto (zł)
  72305 – ludność ogółem
  60559 – gęstość zaludnienia (os./km²)

Klucz API (opcjonalny, zwiększa limity):
  export BDL_API_KEY=twój_klucz
"""

import os
import time
import requests
import pandas as pd

# ── Konfiguracja ────────────────────────────────────────────────────────────
BDL_API_KEY    = os.getenv("BDL_API_KEY", "")
POLAND_UNIT_ID = "011"   # Polska ogółem

START_YEAR = 2015
END_YEAR   = 2023

# (nazwa_kolumny, var_id)
INDICATORS = [
    ("Stopa_bezrobocia_%",       461),
    ("Wynagrodzenie_brutto_zl", 2515),
    ("Ludnosc_ogolem",         72305),
    ("Gestosc_zaludnienia",    60559),
]
# ────────────────────────────────────────────────────────────────────────────


def _call_api(var_id: int, year_params: list, page: int = 0) -> dict | None:
    """Wykonuje jedno żądanie do API BDL. Zwraca dict JSON lub None przy błędzie."""
    url     = f"https://bdl.stat.gov.pl/api/v1/data/by-variable/{var_id}"
    headers = {"X-ClientId": BDL_API_KEY} if BDL_API_KEY else {}
    params  = (
        [("format",    "json"),
         ("unit-id",   POLAND_UNIT_ID),
         ("page-size", "100"),
         ("page",      str(page))]
        + year_params
    )
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=20)
        except requests.exceptions.RequestException as e:
            print(f"\n      Błąd połączenia: {e}")
            return None

        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 60))
            print(f"\n      Rate limit – czekam {wait}s...", end=" ", flush=True)
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            return resp.json()

        # Inny błąd HTTP – nie próbuj ponownie
        print(f"\n      HTTP {resp.status_code} | URL: {resp.url}")
        print(f"      {resp.text[:300]}")
        return None

    return None


def _fetch_with_freq(var_id: int, freq: str,
                     start_year: int, end_year: int) -> list[dict]:
    """
    Pobiera wszystkie wartości dla danej częstotliwości (z paginacją).
    Zwraca listę słowników {'year': ..., 'val': ...} lub [] gdy brak.
    """
    if freq == "M":
        year_params = [("year", f"{y}{m:02d}")
                       for y in range(start_year, end_year + 1)
                       for m in range(1, 13)]
    elif freq == "Q":
        year_params = [("year", f"{y}{q}")
                       for y in range(start_year, end_year + 1)
                       for q in range(1, 5)]
    else:  # "A"
        year_params = [("year", str(y)) for y in range(start_year, end_year + 1)]

    all_values = []
    page       = 0

    while True:
        data = _call_api(var_id, year_params, page)
        if not data:
            break

        results = data.get("results", [])
        if not results:
            break

        values = results[0].get("values", [])
        all_values.extend(values)

        if page + 1 >= data.get("totalPages", 1):
            break
        page += 1
        time.sleep(0.4)

    return all_values


def _period_label(freq: str, raw_year) -> str:
    s = str(raw_year)
    if freq == "M" and len(s) == 6:
        return f"{s[:4]}-{s[4:]}"
    if freq == "Q" and len(s) == 5:
        return f"{s[:4]}-Q{s[4:]}"
    return s


def fetch_indicator(name: str, var_id: int,
                    start_year: int, end_year: int) -> tuple[pd.DataFrame, str]:
    """
    Próbuje kolejno: M → A.
    Zwraca (DataFrame z indeksem 'Okres', użyta częstotliwość).
    """
    for freq in ("M", "A"):
        label = {"M": "miesięczne", "A": "roczne"}[freq]
        print(f"  {name} (ID={var_id}) – próba [{label}]...", end=" ", flush=True)

        values = _fetch_with_freq(var_id, freq, start_year, end_year)

        if values:
            rows = [{"Okres": _period_label(freq, v["year"]), name: v["val"]}
                    for v in values]
            print(f"OK ({len(rows)} pomiarów, {label})")
            return pd.DataFrame(rows).set_index("Okres"), freq

        print("brak danych")

    print(f"  ✗ {name}: brak danych dla wszystkich częstotliwości.")
    return pd.DataFrame(columns=["Okres", name]).set_index("Okres"), "A"


def build_index(freq_used: dict[str, str], start_year: int, end_year: int) -> list[str]:
    """Buduje posortowany indeks wszystkich okresów użytych przez pobrane wskaźniki."""
    periods = set()
    for freq in set(freq_used.values()):
        if freq == "A":
            periods.update(str(y) for y in range(start_year, end_year + 1))
        elif freq == "M":
            periods.update(
                f"{y}-{m:02d}"
                for y in range(start_year, end_year + 1)
                for m in range(1, 13)
            )
        elif freq == "Q":
            periods.update(
                f"{y}-Q{q}"
                for y in range(start_year, end_year + 1)
                for q in range(1, 5)
            )
    return sorted(periods)


def main():
    print(f"=== BDL – wskaźniki makroekonomiczne {START_YEAR}–{END_YEAR} ===\n")

    dfs:      dict[str, pd.DataFrame] = {}
    freq_used: dict[str, str]          = {}

    for name, var_id in INDICATORS:
        df, freq         = fetch_indicator(name, var_id, START_YEAR, END_YEAR)
        dfs[name]        = df
        freq_used[name]  = freq
        time.sleep(0.3)

    # Buduj indeks na podstawie faktycznie użytych częstotliwości
    all_periods = build_index(freq_used, START_YEAR, END_YEAR)
    final_df    = pd.DataFrame(index=all_periods)
    final_df.index.name = "Okres"

    for name, df in dfs.items():
        final_df = final_df.join(df, how="left")

    output = "wskazniki_makroekonomiczne.csv"
    final_df.to_csv(output, encoding="utf-8-sig")

    print(f"\n✓  Zapisano: {output}")
    print(f"   {final_df.shape[0]} wierszy × {final_df.shape[1]} kolumn")
    print(f"   Użyte częstotliwości: { {n: f for n, f in freq_used.items()} }\n")

    preview = final_df.dropna(how="all")
    print("── Podgląd (pierwsze 8 wierszy z danymi) ──")
    print(preview.head(8).to_string())


if __name__ == "__main__":
    main()