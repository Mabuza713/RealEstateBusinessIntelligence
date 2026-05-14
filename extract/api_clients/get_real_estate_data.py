import kagglehub
import pandas as pd
import os
import re

dataset_handle = "krzysztofjamroz/apartment-prices-in-poland"
target_dir = "../../data_raw/"
target_file_path = os.path.join(target_dir, "all_apartment_prices_poland.csv")

os.makedirs(target_dir, exist_ok=True)

cache_path = kagglehub.dataset_download(dataset_handle)

downloaded_files = [plik for plik in os.listdir(cache_path) if plik.endswith(".csv")]

if not downloaded_files:
    print("Nie znaleziono żadnych plików CSV!")
else:
    print(f"Znaleziono {len(downloaded_files)} plików. Łączenie...")

    datafame_list = []

    for plik in downloaded_files:
        file_path = os.path.join(cache_path, plik)

        try:
            df = pd.read_csv(file_path)

            match = re.search(r'(\d{4}_\d{2})', plik)

            if match:
                data_string = match.group(1)
                data_string = data_string.replace('_', '-')
                df['source_date'] = data_string
            else:
                df['source_date'] = ""

            datafame_list.append(df)

        except Exception as e:
            print(f"error during getting: {plik}: {e}")

    if datafame_list:
        merged_df = pd.concat(datafame_list, ignore_index=True)

        merged_df.to_csv(target_file_path, index=False)
        print(f"Merged ({len(merged_df)} rows) saved to: {target_file_path}")

        print("\n5 first rows:")
        print(merged_df.head())
    else:
        print("Couldnt connect to kaggle")