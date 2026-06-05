import requests
import pandas as pd
import time
import argparse
import sys
import calendar
from functools import reduce

MIASTA = [
    "Warszawa", "Kraków", "Łódź", "Wrocław", "Poznań",
    "Gdańsk", "Szczecin", "Bydgoszcz", "Lublin", "Białystok"
]

POZIOMY_SZUKANIA = [6, 5, 4, 3, 2, 1, 0]

ZMIENNE_BI = {
    'Populacja_Ogolna': (72305, "osoby"),
    'Populacja_Mezczyzni': (72306, "osoby"),
    'Populacja_Kobiety': (72307, "osoby"),
    'Urodzenia_Zywe': (34227, "osoby"),
    'Zgony_Ogol': (34254, "osoby"),
    'Saldo_Migracji': (34483, "osoby"),
    'Przyrost_Naturalny': (34270, "osoby"),
    'Pracujacy_Ogol': (35543, "osoby"),
    'Zarejestrowani_Bezrobotni': (10514, "osoby"),
    'Stopa_Bezrobocia': (41390, "%"),
    'Oferty_Pracy': (10563, "szt."),
    'Przecietne_Wynagrodzenie_Brutto': (64428, "zł"),
    'Podmioty_Gospodarcze_REGON': (3643, "szt."),
    'Podmioty_Sektor_Prywatny': (3649, "szt."),
    'Podmioty_Sektor_Publiczny': (3648, "szt."),
    'Spolki_Handlowe': (3660, "szt."),
    'Spolki_Z_Udzialem_Zagranicznym': (3663, "szt."),
    'Mieszkania_Oddane_Do_Uzytku': (74628, "szt."),
    'Mieszkania_Ogol_Zasob': (74652, "szt."),
    'Przecietna_Pow_Uzytkowa_Mieszk': (74680, "m2"),
    'Wydane_Pozwolenia_Na_Budowe': (74564, "szt."),
    'Dochody_Budzetu_Ogol': (60560, "tys. zł"),
    'Wydatki_Budzetu_Ogol': (60561, "tys. zł"),
    'Dochody_Wlasne_JST': (60562, "tys. zł"),
    'Miejsca_Noclegowe': (56167, "miejsca"),
    'Turysci_Krajowi_Zagraniczni': (56173, "osoby"),
    'Drogi_Publiczne_Km': (64003, "km"),
}

FALLBACK_IDS = {
    'Populacja_Ogolna': [34032, 745208, 60559],
    'Przecietne_Wynagrodzenie_Brutto': [64428, 64429, 2497],
    'Stopa_Bezrobocia': [41390, 10553],
    'Dochody_Budzetu_Ogol': [60560, 73675],
}

BASE_URL = "https://bdl.stat.gov.pl/api/v1"


def make_request(url, headers, max_retries=5):
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 200:
                return r
            elif r.status_code == 429:
                wait = 2 ** attempt * 2
                time.sleep(wait)
            elif r.status_code == 503:
                wait = 2 ** attempt
                time.sleep(wait)
            elif r.status_code == 404:
                return None
            else:
                return None
        except requests.exceptions.RequestException as e:
            time.sleep(3)
    return None


def resolve_id_with_fallback(name, primary_id, headers, delay):
    candidates = [primary_id] + FALLBACK_IDS.get(name, [])
    for cid in candidates:
        for level in POZIOMY_SZUKANIA:
            test_url = f"{BASE_URL}/data/by-variable/{cid}?unit-level={level}&page-size=1&format=json"
            r = make_request(test_url, headers)
            time.sleep(delay)

            if r is not None and r.json().get("results"):
                msg = []
                if cid != primary_id:
                    msg.append(f"fallback ID {cid}")
                if level != POZIOMY_SZUKANIA[0]:
                    msg.append(f"poziom {level}")
                return cid, level
    return None, None


def pobierz_dane_bi(rok_od, rok_do, api_key=None):
    headers = {"X-ClientId": api_key} if api_key else {}
    delay = 0.35 if api_key else 1.2

    raport = {"ok": [], "skip_404": [], "brak_danych": [], "blad": []}
    wszystkie_ramki = []

    # Budujemy parametry lat dla zapytania (np. year=2023&year=2024&year=2025)
    lata_query = "&".join([f"year={r}" for r in range(rok_od, rok_do + 1)])

    for nazwa_kolumny, (zmienna_id, jednostka) in ZMIENNE_BI.items():
        kolumna_z_jednostka = f"{nazwa_kolumny}"
        aktywne_id, aktywny_poziom = resolve_id_with_fallback(nazwa_kolumny, zmienna_id, headers, delay)

        if aktywne_id is None:
            raport["skip_404"].append(nazwa_kolumny)
            continue

        url = (
            f"{BASE_URL}/data/by-variable/{aktywne_id}"
            f"?unit-level={aktywny_poziom}&{lata_query}&format=json&page-size=100"
        )
        dane_zmiennej = []

        while url:
            r = make_request(url, headers)
            if r is None:
                raport["blad"].append(nazwa_kolumny)
                break
            time.sleep(delay)

            dane_json = r.json()
            wyniki = dane_json.get("results", [])

            for wynik in wyniki:
                nazwa_jednostki = wynik.get("name", "")
                for miasto in MIASTA:
                    if miasto.lower() in nazwa_jednostki.lower():
                        for wartosc in wynik.get("values", []):
                            # Zabezpieczenie roku w razie braku (choć z API zawsze powinien wrócić)
                            rok_danej = wartosc.get("year", rok_od)

                            miesiac = 12
                            dzien = 31

                            okres = wartosc.get("period", {})
                            if okres:
                                nazwa_okresu = okres.get("name", "").lower()
                                if "miesiąc" in nazwa_okresu:
                                    try:
                                        miesiac = int(nazwa_okresu.replace("miesiąc", "").strip())
                                        dzien = calendar.monthrange(rok_danej, miesiac)[1]
                                    except ValueError:
                                        pass

                            data_kolumna = f"{rok_danej}-{miesiac:02d}-{dzien:02d}"

                            dane_zmiennej.append({
                                "Miasto_GUS": nazwa_jednostki,
                                "Glowne_Miasto": miasto,
                                "Data": data_kolumna,
                                kolumna_z_jednostka: wartosc.get("val"),
                            })
                        break
            url = dane_json.get("links", {}).get("next")

        if dane_zmiennej:
            df_temp = pd.DataFrame(dane_zmiennej).drop_duplicates(
                subset=["Miasto_GUS", "Data"]
            )
            wszystkie_ramki.append(df_temp)
            raport["ok"].append(nazwa_kolumny)
        else:
            raport["brak_danych"].append(nazwa_kolumny)

    if not wszystkie_ramki:
        print("Nie pobrano żadnych danych!")
        return pd.DataFrame()

    df_final = reduce(
        lambda left, right: pd.merge(
            left, right,
            on=["Miasto_GUS", "Glowne_Miasto", "Data"],
            how="outer"
        ),
        wszystkie_ramki
    )

    df_final = df_final.sort_values(["Data", "Glowne_Miasto"]).reset_index(drop=True)

    # Zmodyfikowana nazwa pliku zawierająca zakres lat
    nazwa_pliku = f"../data/raw/baza_bi_miasta_{rok_od}_{rok_do}.csv"
    df_final.to_csv(nazwa_pliku, index=False, encoding="utf-8-sig")

    print(f"Dane zostały pobrane i zapisane w: {nazwa_pliku}")
    return df_final


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pobieranie danych BDL GUS do BI")
    parser.add_argument("--rok_od", type=int, default=2023, help="Rok początkowy")
    parser.add_argument("--rok_do", type=int, default=2025, help="Rok końcowy")
    parser.add_argument("--klucz", type=str, default=None, help="Klucz API GUS (opcjonalny)")
    args = parser.parse_args()
    df = pobierz_dane_bi(rok_od=args.rok_od, rok_do=args.rok_do, api_key=args.klucz)
    if not df.empty:
        pd.set_option("display.max_columns", 10)
        pd.set_option("display.width", 200)
        print(df.head())
