"""
Skrypt pobierający dane o cenach i parametrach mieszkań w Polsce z platformy Kaggle.
Odpowiada za warstwę ekstrakcji (RAW) dla ofert nieruchomości (sprzedaż i wynajem).
"""

import kagglehub
import pandas as pd
import os
import re
import argparse

# Identyfikator zbioru danych w serwisie Kaggle
dataset_handle = "krzysztofjamroz/apartment-prices-in-poland"

if __name__ == "__main__":
    # Konfiguracja parsera argumentów linii komend do filtrowania przyrostowego
    parser = argparse.ArgumentParser(description="Pobieranie i scalanie danych o nieruchomościach z Kaggle")
    parser.add_argument("--after", type=str, default=None, help="Przetwarzaj tylko pliki z miesiącem późniejszym niż YYYY-MM")
    args = parser.parse_args()

    # Określenie i utworzenie katalogu docelowego dla surowych plików CSV
    target_dir = "../data/raw/"
    os.makedirs(target_dir, exist_ok=True)

    sell_target_path = os.path.join(target_dir, "all_apartments_sell.csv")
    rent_target_path = os.path.join(target_dir, "all_apartments_rent.csv")

    # Pobranie najnowszej wersji zbioru danych przy użyciu kagglehub
    print(f"Pobieranie zbioru danych {dataset_handle}...")
    cache_path = kagglehub.dataset_download(dataset_handle)
    downloaded_files = [plik for plik in os.listdir(cache_path) if plik.endswith(".csv")]

    if not downloaded_files:
        print("Nie znaleziono żadnych plików CSV w pobranym zbiorze!")
    else:
        sell_dataframes = []
        rent_dataframes = []

        # Przetwarzanie każdego pobranego pliku CSV osobno
        for plik in downloaded_files:
            file_path = os.path.join(cache_path, plik)
            try:
                # Ekstrakcja daty (RRRR_MM) bezpośrednio z nazwy pliku w celu przypisania do wierszy
                match = re.search(r'(\d{4}_\d{2})', plik)
                if match:
                    file_month = match.group(1).replace('_', '-')
                else:
                    file_month = ""

                # Filtracja przyrostowa: jeśli plik reprezentuje miesiąc starszy bądź równy
                # wartości --after (ostatnio przetworzonej dacie w bazie), pomijamy go.
                if args.after and file_month:
                    if file_month <= args.after:
                        print(f"Pomijam plik {plik} (miesiąc {file_month} <= {args.after})")
                        continue

                # Wczytanie pliku CSV i dodanie kolumny source_date z datą pobrania oferty
                df = pd.read_csv(file_path)
                df['source_date'] = file_month

                # Przypisanie wczytanej ramki do odpowiedniej kategorii (wynajem / sprzedaż)
                if 'rent' in plik.lower():
                    rent_dataframes.append(df)
                else:
                    sell_dataframes.append(df)

            except Exception as e:
                print(f"Błąd podczas przetwarzania {plik}: {e}")

        # Usuwamy stare pliki przed zapisem nowych (idempotentność - jeśli nie ma nowych danych, stare nie zostaną zdublowane)
        if os.path.exists(sell_target_path):
            os.remove(sell_target_path)
        if os.path.exists(rent_target_path):
            os.remove(rent_target_path)

        # Scalanie i zapisywanie plików ofert sprzedaży (sell)
        if sell_dataframes:
            print(f"Liczba wczytanych plików sprzedaży: {len(sell_dataframes)}")
            merged_sell = pd.concat(sell_dataframes, ignore_index=True)
            merged_sell.to_csv(sell_target_path, index=False)
            print(f"Sukces! Sprzedaż ({len(merged_sell)} wierszy) zapisana w: {sell_target_path}")
            
        # Scalanie i zapisywanie plików ofert wynajmu (rent)
        if rent_dataframes:
            print(f"Liczba wczytanych plików wynajmu: {len(rent_dataframes)}")
            merged_rent = pd.concat(rent_dataframes, ignore_index=True)
            merged_rent.to_csv(rent_target_path, index=False)
            print(f"Sukces! Wynajem ({len(merged_rent)} wierszy) zapisany w: {rent_target_path}")
