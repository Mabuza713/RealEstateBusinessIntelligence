"""
Skrypt pobierający punkty użyteczności publicznej (POI) z OpenStreetMap za pomocą Overpass API.
Odpowiada za warstwę ekstrakcji (RAW) dla lokalizacji kawiarni, parkingów i przystanków autobusowych.
"""

import csv
import os
import time
import urllib.request
import overpy
import socket

# Ustawienie limitu czasu dla połączeń sieciowych socket
socket.setdefaulttimeout(60)

# Konfiguracja nagłówka User-Agent w celu uniknięcia blokowania zapytań przez OSM
opener = urllib.request.build_opener()
opener.addheaders = [("User-agent", "WyszukiwarkaOSM/1.0 (testowy-skrypt)")]
urllib.request.install_opener(opener)


def FetchAndAppendPoints(city, point_type, key, filename):
    """
    Formułuje i wysyła zapytanie Overpass QL dla wybranego miasta i kategorii POI.
    Zapisuje pobrane wyniki bezpośrednio do pliku CSV (zabezpieczając jednokrotny zapis nagłówka).
    """
    # Konstruowanie zapytania Overpass QL z filtrowaniem według obszaru miasta i klucza tagu OSM
    query = f"""
    [out:json][timeout:180];
    area[name="{city}"]->.searchArea;
    node["{key}"="{point_type}"](area.searchArea);
    out;
    """

    # Lista serwerów lustrzanych Overpass API w celu zwiększenia odporności na limity i niedostępność
    endpoints = [
        "https://overpass.kumi.systems/api/interpreter",
        "https://overpass-api.de/api/interpreter",
        "https://z.overpass-api.de/api/interpreter",
    ]

    result = None
    # Rotacja serwerów w przypadku niepowodzenia
    for url in endpoints:
        try:
            print(f"  Próba pobrania dla miasta {city} ({point_type}) z serwera: {url}...")
            api = overpy.Overpass(url=url)
            result = api.query(query)
            print(
                f"  Pobrano {len(result.nodes)} obiektów dla miasta {city} ({point_type}) z serwera {url}"
            )
            break
        except overpy.exception.OverpassTooManyRequests:
            print(
                f"  Błąd: Zbyt wiele zapytań do serwera {url}. Czekam 10s i próbuję dalej..."
            )
            time.sleep(10)
        except Exception as e:
            print(f"  Błąd dla serwera {url} przy pobieraniu {city}: {e}")

    if not result or len(result.nodes) == 0:
        print(f"  Brak danych lub błąd dla miasta {city} ({point_type})")
        return

    try:
        # Upewniamy się, że katalog docelowy istnieje (np. ../data/raw/)
        if os.path.dirname(filename):
            os.makedirs(os.path.dirname(filename), exist_ok=True)

        # Sprawdzamy, czy plik już istnieje i ma zawartość, aby zdecydować o zapisie nagłówka
        file_exists = os.path.isfile(filename) and os.path.getsize(filename) > 0

        # Otwieramy plik w trybie dopisywania ('a' - append), aby łączyć dane z kolejnych miast
        with open(
            filename, mode="a", newline="", encoding="utf-8-sig"
        ) as csv_file:
            fieldnames = ["City", "Name", "Street", "Number", "LAT", "LON"]
            writer = csv.DictWriter(
                csv_file, fieldnames=fieldnames, delimiter=";"
            )

            # Nagłówek zapisze się tylko raz – przy pierwszym dodawaniu danych do pustego pliku
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

    except Exception as e:
        print(f"  Wystąpił błąd przy zapisie danych dla {city}: {e}")


if __name__ == "__main__":
    # Analizowana lista 15 głównych polskich miast
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
        "Gdynia",
        "Katowice",
        "Radom",
        "Rzeszów",
        "Częstochowa",
    ]

    # Definiujemy ścieżki do trzech zbiorczych plików dla kategorii POI
    output_files = {
        "cafe": "../data/raw/all_cafes.csv",
        "parking": "../data/raw/all_parkings.csv",
        "bus_stop": "../data/raw/all_bus_stops.csv",
    }

    # WAŻNE: Czyścimy stare pliki na początku uruchomienia programu, aby uniknąć duplikowania przy restarcie
    for path in output_files.values():
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass

    # Główna pętla pobierania danych z zachowaniem przerw czasowych w celu uniknięcia banowania IP (throttling)
    for city in cities:
        print(f"\nRozpoczynam pobieranie danych dla miasta: {city}")

        # 1. Kawiarnie
        FetchAndAppendPoints(city, "cafe", "amenity", output_files["cafe"])
        time.sleep(2)  # Krótka przerwa między zapytaniami w obrębie jednego miasta

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
        "\nSukces! Wszystkie dane zostały pobrane i połączone w 3 plikach zbiorczych w katalogu '../data/raw/'."
    )
