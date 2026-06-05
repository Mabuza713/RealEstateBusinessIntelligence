import kagglehub
import pandas as pd
import os
import re

dataset_handle = "krzysztofjamroz/apartment-prices-in-poland"
target_dir = "../data_raw/"
os.makedirs(target_dir, exist_ok=True)

# Definiujemy ścieżki dla dwóch osobnych plików
sell_target_path = os.path.join(target_dir, "all_apartments_sell.csv")
rent_target_path = os.path.join(target_dir, "all_apartments_rent.csv")

cache_path = kagglehub.dataset_download(dataset_handle)
downloaded_files = [plik for plik in os.listdir(cache_path) if plik.endswith(".csv")]

if not downloaded_files:
    print("Nie znaleziono żadnych plików CSV!")
else:
    sell_dataframes = []
    rent_dataframes = []

    for plik in downloaded_files:
        file_path = os.path.join(cache_path, plik)
        try:
            df = pd.read_csv(file_path)

            # Wyciąganie daty z nazwy pliku
            match = re.search(r'(\d{4}_\d{2})', plik)
            if match:
                df['source_date'] = match.group(1).replace('_', '-')
            else:
                df['source_date'] = ""

            if 'rent' in plik.lower():
                rent_dataframes.append(df)
            else:
                sell_dataframes.append(df)

        except Exception as e:
            print(f"Błąd podczas przetwarzania {plik}: {e}")

    # Zapisujemy plik ze sprzedażą
    if sell_dataframes:
        print(len(sell_dataframes))
        merged_sell = pd.concat(sell_dataframes, ignore_index=True)
        merged_sell.to_csv(sell_target_path, index=False)
        print(f"Sukces! Sprzedaż ({len(merged_sell)} wierszy) zapisana w: {sell_target_path}")

    # Zapisujemy plik z wynajmem
    if rent_dataframes:
        print(len(rent_dataframes))
        merged_rent = pd.concat(rent_dataframes, ignore_index=True)
        merged_rent.to_csv(rent_target_path, index=False)
        print(f"Sukces! Wynajem ({len(merged_rent)} wierszy) zapisany w: {rent_target_path}")