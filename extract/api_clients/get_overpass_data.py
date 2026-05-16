import csv
import os
import time
import urllib.request
import overpy

opener = urllib.request.build_opener()
opener.addheaders = [("User-agent", "WyszukiwarkaOSM/1.0 (testowy-skrypt)")]
urllib.request.install_opener(opener)


def FetchAndAppendPoints(city, point_type, key, filename):
    api = overpy.Overpass()

    query = f"""
    [out:json];
    area[name="{city}"]->.searchArea;
    node["{key}"="{point_type}"](area.searchArea);
    out;
    """

    try:
        result = api.query(query)
        print(
            f"  Pobrano {len(result.nodes)} obiektów dla miasta {city} ({point_type})"
        )

        if len(result.nodes) == 0:
            return

        # Upewniamy się, że katalog docelowy istnieje (np. ../../data_raw/)
        if os.path.dirname(filename):
            os.makedirs(os.path.dirname(filename), exist_ok=True)

        # Sprawdzamy, czy plik już istnieje i ma zawartość, aby zdecydować o nagłówku
        file_exists = os.path.isfile(filename) and os.path.getsize(filename) > 0

        # Otwieramy w trybie dopisywania ('a' - append)
        with open(
            filename, mode="a", newline="", encoding="utf-8-sig"
        ) as csv_file:
            # Dodałem kolumnę 'City' jako pierwszy element struktury
            fieldnames = ["City", "Name", "Street", "Number", "LAT", "LON"]
            writer = csv.DictWriter(
                csv_file, fieldnames=fieldnames, delimiter=";"
            )

            # Nagłówek zapisze się tylko raz – przy pierwszym dodawaniu danych do czystego pliku
            if not file_exists:
                writer.writeheader()

            for node in result.nodes:
                name = node.tags.get("name", "")
                street = node.tags.get("addr:street", "")
                number = node.tags.get("addr:housenumber", "")
                lat = node.lat
                lon = node.lon

                writer.writerow(
                    {
                        "City": city,
                        "Name": name,
                        "Street": street,
                        "Number": number,
                        "LAT": lat,
                        "LON": lon,
                    }
                )

    except overpy.exception.OverpassTooManyRequests:
        print(
            "  Błąd: Zbyt wiele zapytań do API (OverpassTooManyRequests). Czekam 10s..."
        )
        time.sleep(10)
    except Exception as e:
        print(f"  Wystąpił błąd przy pobieraniu {city}: {e}")


if __name__ == "__main__":
    cities = [
        "Warszawa",
        "Kraków",
        "Łódź",
        "Wrocław",
        "Poznań",
        "Gdańsk",
        "Szczecin",
        "Bydgoszcz",
        "Lublin",
        "Białystok",
    ]

    # Definiujemy ścieżki do trzech zbiorczych plików
    output_files = {
        "cafe": "../../data_raw/all_cafes.csv",
        "parking": "../../data_raw/all_parkings.csv",
        "bus_stop": "../../data_raw/all_bus_stops.csv",
    }

    # WAŻNE: Czyścimy stare pliki na początku uruchomienia programu.
    # Dzięki temu, jeśli uruchomisz skrypt ponownie, dane nie będą się dublować w nieskończoność.
    for path in output_files.values():
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass

    # Główna pętla przechodząca przez miasta
    for city in cities:
        print(f"\nRozpoczynam pobieranie danych dla miasta: {city}")

        # 1. Kawiarnie
        FetchAndAppendPoints(city, "cafe", "amenity", output_files["cafe"])
        time.sleep(2)  # Krótka przerwa między zapytaniami w jednym mieście

        # 2. Parkingi
        FetchAndAppendPoints(
            city, "parking", "amenity", output_files["parking"]
        )
        time.sleep(2)

        # 3. Przystanki autobusowe
        FetchAndAppendPoints(
            city, "bus_stop", "highway", output_files["bus_stop"]
        )

        # Bezpieczna pauza przed zmianą miasta na kolejne
        print("Czekam 5 sekund przed kolejnym miastem...")
        time.sleep(5)

    print(
        "\nSukces! Wszystkie dane zostały pobrane i połączone w 3 plikach zbiorczych w katalogu '../../data_raw/'."
    )