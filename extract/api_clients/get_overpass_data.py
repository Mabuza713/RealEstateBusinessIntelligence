import overpy
import urllib.request
import csv

opener = urllib.request.build_opener()
opener.addheaders = [('User-agent', 'WyszukiwarkaOSM/1.0 (testowy-skrypt)')]
urllib.request.install_opener(opener)


def FindPointsInCityAndSaveCSV(city, point_type, key="amenity", filename="overpass.csv"):
    api = overpy.Overpass()

    query = f"""
    [out:json];
    area[name="{city}"]->.searchArea;
    node["{key}"="{point_type}"](area.searchArea);
    out;
    """

    try:
        result = api.query(query)

        if len(result.nodes) == 0:
            print(f"No points: {key}='{point_type}' in city {city}.")
            return

        with open(filename, mode='w', newline='', encoding='utf-8-sig') as csv_file:
            fieldnames = ["Name", "Street", "Number", "LAT", "LON"]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter=';')

            writer.writeheader()

            for node in result.nodes:
                name = node.tags.get("name", "")
                street = node.tags.get("addr:street", "")
                number = node.tags.get("addr:housenumber", "")


                lat = node.lat
                lon = node.lon

                writer.writerow({
                    'Name': name,
                    'Street': street,
                    "Number": number,
                    'LAT': lat,
                    'LON': lon,
                })

    except overpy.exception.OverpassTooManyRequests:
        print("Too many requests")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    FindPointsInCityAndSaveCSV("Wrocław", "cafe", filename="../../data_raw/wroclaw_cafes.csv")
    FindPointsInCityAndSaveCSV("Wrocław", "parking", filename="../../data_raw/parkings_wroclaw.csv")
    FindPointsInCityAndSaveCSV("Wrocław", "bus_stop", key="highway", filename="../../data_raw/wroclaw_bus_stops.csv")