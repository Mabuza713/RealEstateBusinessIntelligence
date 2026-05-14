import requests
import pandas as pd


def fetch_gus_monthly_data():
    variables = {
        "Monthly_Wage_PLN": "450592",
        "CPI_YoY": "159648",
        "Housing_Price_M2": "1656"
    }

    all_records = []

    for name, v_id in variables.items():
        url = f"https://bdl.stat.gov.pl/api/v1/data/by-variable/{v_id}?unit-level=0&year=2023&year=2024&format=json"
        response = requests.get(url)

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

    df_pivot.to_csv("../../data_raw/poland_real_estate_monthly_2023_2024.csv", index=False, sep=";")


if __name__ == "__main__":
    fetch_gus_monthly_data()