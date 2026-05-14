import requests
import csv

VARIABLE_ID = 72305
UNIT_LEVEL = 6
YEARS = [2022, 2023, 2024]
OUTPUT_INTERP = "../../data_raw/city_population_monthly_interpolated.csv"
PAGE_SIZE = 100

VOIVODESHIP_CITIES = {
    "Warszawa", "Kraków", "Łódź", "Wrocław", "Poznań", "Gdańsk",
    "Szczecin", "Bydgoszcz", "Lublin", "Białystok", "Katowice",
    "Rzeszów", "Opole", "Zielona Góra", "Gorzów Wielkopolski",
    "Olsztyn", "Kielce", "Toruń",
}


def fetch_bdl_data():
    results = []
    page = 0
    year_query = "&".join(f"year={y}" for y in YEARS)
    base_url = f"https://bdl.stat.gov.pl/api/v1/data/by-variable/{VARIABLE_ID}?unit-level={UNIT_LEVEL}&{year_query}&page-size={PAGE_SIZE}&format=json"

    while True:
        response = requests.get(f"{base_url}&page={page}", timeout=30)
        if response.status_code != 200:
            break

        data = response.json()
        results.extend(data.get("results", []))

        if not data.get("links", {}).get("next"):
            break
        page += 1
    return results


def get_interpolated_rows(units, target_years):
    rows = []
    for unit in units:
        city_name = unit.get("name", "").strip()
        if city_name not in VOIVODESHIP_CITIES:
            continue

        annual_map = {int(v["year"]): float(v["val"]) for v in unit.get("values", []) if v.get("val") is not None}

        for year in target_years:
            prev_pop = annual_map.get(year - 1)
            curr_pop = annual_map.get(year)

            if prev_pop is not None and curr_pop is not None:
                diff = curr_pop - prev_pop
                for month in range(1, 13):
                    rows.append({
                        "city": city_name,
                        "year_month": f"{year}-{month:02d}",
                        "population": round(prev_pop + diff * (month / 12))
                    })
    return rows


def main():
    raw_units = fetch_bdl_data()
    interpolated_data = get_interpolated_rows(raw_units, [2023, 2024])

    if interpolated_data:
        with open(OUTPUT_INTERP, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["city", "year_month", "population"])
            writer.writeheader()
            writer.writerows(interpolated_data)


if __name__ == "__main__":
    main()