CREATE TABLE IF NOT EXISTS stg.apartments (
    id VARCHAR(50),
    city VARCHAR(100),
    type VARCHAR(100),
    squareMeters NUMERIC(10, 2),
    rooms INT,
    floor INT,
    floorCount INT,
    buildYear INT,
    latitude NUMERIC(9, 6),
    longitude NUMERIC(9, 6),
    centreDistance NUMERIC(10, 2),
    poiCount INT,
    schoolDistance NUMERIC(10, 2),
    clinicDistance NUMERIC(10, 2),
    postOfficeDistance NUMERIC(10, 2),
    kindergartenDistance NUMERIC(10, 2),
    restaurantDistance NUMERIC(10, 2),
    collegeDistance NUMERIC(10, 2),
    pharmacyDistance NUMERIC(10, 2),
    ownership VARCHAR(100),
    buildingMaterial VARCHAR(100),
    condition VARCHAR(100),
    hasParkingSpace VARCHAR(50),
    hasBalcony VARCHAR(50),
    hasElevator VARCHAR(50),
    hasSecurity VARCHAR(50),
    hasStorageRoom VARCHAR(50),
    price NUMERIC(15, 2),
    source_date VARCHAR(10),
    listing_type VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS stg.demografia (
    Miasto_GUS VARCHAR(200),
    Glowne_Miasto VARCHAR(100),
    Data VARCHAR(20),
    Populacja_Ogolna NUMERIC,
    Populacja_Mezczyzni NUMERIC,
    Populacja_Kobiety NUMERIC,
    Zarejestrowani_Bezrobotni NUMERIC,
    Przecietne_Wynagrodzenie_Brutto NUMERIC,
    Dochody_Wlasne_JST NUMERIC
);

CREATE TABLE IF NOT EXISTS stg.poi (
    City VARCHAR(100),
    Name VARCHAR(200),
    Street VARCHAR(200),
    Number VARCHAR(50),
    LAT NUMERIC(9, 6),
    LON NUMERIC(9, 6),
    poi_type VARCHAR(50)
);

