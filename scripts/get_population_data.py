"""
Skrypt pobierający dane demograficzne i ekonomiczne z Banku Danych Lokalnych (BDL) GUS.
Odpowiada za warstwę ekstrakcji (RAW) dla wskaźników statystycznych 15 głównych miast Polski.
"""

import requests
import pandas as pd
import time
import argparse
import sys
import calendar
from functools import reduce

# Lista miast, dla których pobieramy statystyki
MIASTA = [
    "Warszawa", "Kraków", "Łódź", "Wrocław", "Poznań",
    "Gdańsk", "Szczecin", "Bydgoszcz", "Lublin", "Białystok",
    "Gdynia", "Katowice", "Radom", "Rzeszów", "Częstochowa"
]

# Poziomy podziału terytorialnego GUS (od powiatu/gminy do kraju), które przeszukujemy
POZIOMY_SZUKANIA = [5, 6, 4, 3, 2, 1, 0]

# Słownik wskaźników analitycznych z ich głównym identyfikatorem zmiennej w GUS BDL
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

# Alternatywne identyfikatory (fallbacks) w przypadku, gdyby GUS zmienił strukturę słowników zmiennych
FALLBACK_IDS = {
    'Populacja_Ogolna': [34032, 745208, 60559],
    'Przecietne_Wynagrodzenie_Brutto': [64428, 64429, 2497],
    'Stopa_Bezrobocia': [41390, 10553],
    'Dochody_Budzetu_Ogol': [60560, 73675],
}

BASE_URL = "https://bdl.stat.gov.pl/api/v1"


def make_request(url, headers, max_retries=5):
    """
    Wykonuje zapytanie HTTP GET do API GUS BDL.
    Wdraża obsługe przeciążenia (HTTP 429 i 503) poprzez odczekanie 30 sekund (exponential back-off).
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                return response
            if response.status_code == 429:
                print("Przekroczono limit zapytań (429). Czekam 30 sekund...")
                time.sleep(30)
            elif response.status_code == 503:
                print("Usługa niedostępna (503). Czekam 30 sekund...")
                time.sleep(30)
            else:
                return None
        except requests.exceptions.RequestException:
            time.sleep(3)
    return None


def resolve_id_with_fallback(name, primary_id, headers, delay):
    """
    Dynamicznie sprawdza, który z identyfikatorów zmiennej (główny lub fallbacki)
    zwraca dane na przeszukiwanych poziomach agregacji. Zwraca (aktywny_id, poziom).
    """
    candidates = [primary_id] + FALLBACK_IDS.get(name, [])
    for candidate_id in candidates:
        for level in POZIOMY_SZUKANIA:
            test_url = f"{BASE_URL}/data/by-variable/{candidate_id}?unit-level={level}&page-size=1&format=json"
            response = make_request(test_url, headers)
            time.sleep(delay)
            if response is not None and response.json().get("results"):
                return candidate_id, level
    return None, None


def pobierz_dane_bi(rok_od, rok_do, api_key=None):
    """
    Koordynuje pobieranie danych dla wszystkich 26 wskaźników GUS.
    Buduje ramki danych, usuwa duplikaty i łączy je w jeden spójny plik CSV za pomocą reduce/merge.
    """
    # Nagłówek uwierzytelniający (klucz API) przyspiesza limity zapytań
    headers = {"X-ClientId": api_key} if api_key else {}
    # Jeśli mamy klucz API, możemy wysyłać zapytania częściej
    delay = 0.35 if api_key else 1.2

    raport = {"ok": [], "skip_404": [], "brak_danych": [], "blad": []}
    wszystkie_ramki = []

    lata_query = "&".join([f"year={r}" for r in range(rok_od, rok_do + 1)])

    for nazwa_kolumny, (zmienna_id, jednostka) in ZMIENNE_BI.items():
        print(f"Przetwarzanie zmiennej: {nazwa_kolumny}...")
        aktywne_id, aktywny_poziom = resolve_id_with_fallback(nazwa_kolumny, zmienna_id, headers, delay)

        if aktywne_id is None:
            print(f"  [Pominięto] Brak aktywnego ID dla {nazwa_kolumny}")
            raport["skip_404"].append(nazwa_kolumny)
            continue

        url = (
            f"{BASE_URL}/data/by-variable/{aktywne_id}"
            f"?unit-level={aktywny_poziom}&{lata_query}&format=json&page-size=100"
        )
        dane_zmiennej = []

        # Obsługa paginacji w API GUS BDL
        while url:
            response = make_request(url, headers)
            if response is None:
                raport["blad"].append(nazwa_kolumny)
                break
            time.sleep(delay)

            dane_json = response.json()
            wyniki = dane_json.get("results", [])

            # Filtrowanie danych wyłącznie dla 15 zdefiniowanych głównych miast
            for wynik in wyniki:
                nazwa_jednostki = wynik.get("name", "")
                for miasto in MIASTA:
                    if miasto.lower() in nazwa_jednostki.lower():
                        for wartosc in wynik.get("values", []):
                            rok_danej = wartosc.get("year", rok_od)

                            # Konwersja okresu na datę dzienną (np. dane roczne -> 31 grudnia)
                            month, day = 12, 31
                            period_name = wartosc.get("period", {}).get("name", "").lower()
                            if "miesiąc" in period_name:
                                try:
                                    month = int(period_name.replace("miesiąc", "").strip())
                                    day = calendar.monthrange(rok_danej, month)[1]
                                except ValueError:
                                    pass
                            data_kolumna = f"{rok_danej}-{month:02d}-{day:02d}"

                            dane_zmiennej.append({
                                "Miasto_GUS": nazwa_jednostki,
                                "Glowne_Miasto": miasto,
                                "Data": data_kolumna,
                                nazwa_kolumny: wartosc.get("val"),
                            })
                        break
            url = dane_json.get("links", {}).get("next")

        # Tworzenie ramki danych dla danej zmiennej
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

    # Łączenie wszystkich wskaźników w jedną szeroką tabelę (outer join) po (Miasto_GUS, Glowne_Miasto, Data)
    print("Scalanie pobranych wskaźników GUS w jedną ramkę danych...")
    df_final = reduce(
        lambda left, right: pd.merge(
            left, right,
            on=["Miasto_GUS", "Glowne_Miasto", "Data"],
            how="outer"
        ),
        wszystkie_ramki
    )

    df_final = df_final.sort_values(["Data", "Glowne_Miasto"]).reset_index(drop=True)

    # Zapis końcowej tabeli do katalogu raw
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))
    nazwa_pliku = os.path.join(script_dir, "..", "data", "raw", f"baza_bi_miasta_{rok_od}_{rok_do}.csv")
    df_final.to_csv(nazwa_pliku, index=False, encoding="utf-8-sig")

    print(f"Dane zostały pobrane i zapisane w: {os.path.abspath(nazwa_pliku)}")
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
