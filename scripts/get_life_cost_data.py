import argparse
import requests
import pandas as pd

BASE_URL = "https://bdl.stat.gov.pl/api/v1"

VARIABLES = {
    "Monthly_Wage_PLN": "450592",
    "CPI_YoY": "159648",
    "Housing_Price_M2": "1656",
}


def fetch_gus_monthly_data(rok_od: int = 2023, rok_do: int = 2025):
    lata_query = "&".join(f"year={r}" for r in range(rok_od, rok_do + 1))
    all_records = []

    for name, v_id in VARIABLES.items():
        url = f"{BASE_URL}/data/by-variable/{v_id}?unit-level=0&{lata_query}&format=json"
        response = requests.get(url, timeout=15)

        if response.status_code == 200:
            data = response.json()
            if 'results' in data and data['results']:
                values = data['results'][0]['values']

                for entry in values:
                    year = entry['year']
                    period_id = int(entry['id'])
                    if period_id <= 12:
                        month = period_id
                    elif 21 <= period_id <= 24:
                        month = (period_id - 20) * 3
                    else:
                        continue

                    all_records.append({
                        "Date": f"{year}-{month:02d}-01",
                        "Indicator": name,
                        "Value": entry['val']
                    })

    if not all_records:
        print("Brak danych. Sprawdź limity API GUS.")
        return

    df = pd.DataFrame(all_records)
    df_pivot = df.pivot_table(index="Date", columns="Indicator", values="Value", aggfunc='first').reset_index()

    df_pivot['Date'] = pd.to_datetime(df_pivot['Date'])
    df_pivot = df_pivot.sort_values('Date')

    output_path = f"../data/raw/poland_real_estate_monthly_{rok_od}_{rok_do}.csv"
    df_pivot.to_csv(output_path, index=False, sep=";")
    print(f"Dane zapisane w: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pobieranie miesięcznych wskaźników GUS")
    parser.add_argument("--rok_od", type=int, default=2023, help="Rok początkowy")
    parser.add_argument("--rok_do", type=int, default=2025, help="Rok końcowy")
    args = parser.parse_args()
    fetch_gus_monthly_data(rok_od=args.rok_od, rok_do=args.rok_do)
