CREATE SCHEMA IF NOT EXISTS prod;

-- ============================================================
-- WYMIARY
-- ============================================================

CREATE TABLE IF NOT EXISTS prod.Dim_Lokal (
    ID_Lokalu       SERIAL PRIMARY KEY,
    source_id       VARCHAR(50)  NOT NULL,         -- oryginalne id z stg.apartments
    listing_type    VARCHAR(10)  NOT NULL,
    latitude        NUMERIC(9,6),
    longitude       NUMERIC(9,6),
    city            VARCHAR(100),
    squareMeters    NUMERIC(10,2),
    rooms           INT,
    floor           INT,
    floorCount      INT,
    condition       VARCHAR(100),
    hasElevator     VARCHAR(50),
    hasParkingSpace VARCHAR(50),
    hasStorageRoom  VARCHAR(50),
    hasSecurity     VARCHAR(50),
    hasBalcony      VARCHAR(50),
    price           NUMERIC(15,2),
    ownership       VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS prod.Dim_Budynek (
    ID_Budynku      SERIAL PRIMARY KEY,
    source_id       VARCHAR(50)  NOT NULL,
    listing_type    VARCHAR(10)  NOT NULL,
    city            VARCHAR(100),
    type            VARCHAR(100),
    buildYear       INT,
    buildingMaterial VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS prod.Dim_Infrastruktura (
    ID_Infrastruktury SERIAL PRIMARY KEY,
    source_id         VARCHAR(50) NOT NULL,
    listing_type      VARCHAR(10) NOT NULL,
    centreDistance    NUMERIC(10,2),
    schoolDistance    NUMERIC(10,2),
    clinicDistance    NUMERIC(10,2),
    postOfficeDistance NUMERIC(10,2),
    collegeDistance   NUMERIC(10,2),
    kindergartenDistance NUMERIC(10,2),
    busstopDistance   NUMERIC(10,2),
    caffeDistance     NUMERIC(10,2),
    parkingDistance   NUMERIC(10,2),
    restaurantDistance NUMERIC(10,2),
    pharmacyDistance  NUMERIC(10,2),
    poiCount          INT
);

CREATE TABLE IF NOT EXISTS prod.Dim_Czas (
    ID_Czasu        SERIAL PRIMARY KEY,
    source_date     VARCHAR(10) NOT NULL UNIQUE,
    miesiac         INT         NOT NULL,
    rok             INT         NOT NULL,
    source_date_m   INT         NOT NULL,
    source_date_y   INT         NOT NULL
);

CREATE TABLE IF NOT EXISTS prod.Dim_Demografia (
    ID_Demografii                   SERIAL PRIMARY KEY,
    Miasto_GUS                      VARCHAR(200) NOT NULL,
    Glowne_Miasto                   VARCHAR(100),
    Data                            VARCHAR(20),
    Populacja_Ogolna                INT,
    Zarejestrowani_Bezrobotni       INT,
    Przecietne_Wynagrodzenie_Brutto NUMERIC(10,2),
    Dochody_Wlasne_JST              NUMERIC(10,2)
);

-- ============================================================
-- TABELA FAKTÓW — schemat gwiazdy
-- ============================================================

CREATE TABLE IF NOT EXISTS prod.Fact_Oferta_Nieruchomosci (
    ID_Faktu                    SERIAL PRIMARY KEY,
    ID_Lokalu                   INT NOT NULL REFERENCES prod.Dim_Lokal(ID_Lokalu),
    ID_Budynku                  INT NOT NULL REFERENCES prod.Dim_Budynek(ID_Budynku),
    ID_Infrastruktury           INT NOT NULL REFERENCES prod.Dim_Infrastruktura(ID_Infrastruktury),
    ID_Czasu                    INT NOT NULL REFERENCES prod.Dim_Czas(ID_Czasu),
    ID_Demografii               INT          REFERENCES prod.Dim_Demografia(ID_Demografii),
    -- miary
    Cena_Calkowita              NUMERIC(15,2),
    Cena_Za_M2                  NUMERIC(10,2),
    Powierzchnia_Lokalu         NUMERIC(10,2),
    Odchylenie_Procentowe_Ceny  NUMERIC(8,4),
    Stosunek_Najmu_Do_Wynagrodzenia NUMERIC(8,4),
    Premia_Lokalizacyjna        NUMERIC(10,2)
);

-- ============================================================
-- INDEKSY
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_fact_lokal   ON prod.Fact_Oferta_Nieruchomosci(ID_Lokalu);
CREATE INDEX IF NOT EXISTS idx_fact_budynek ON prod.Fact_Oferta_Nieruchomosci(ID_Budynku);
CREATE INDEX IF NOT EXISTS idx_fact_infra   ON prod.Fact_Oferta_Nieruchomosci(ID_Infrastruktury);
CREATE INDEX IF NOT EXISTS idx_fact_czas    ON prod.Fact_Oferta_Nieruchomosci(ID_Czasu);
CREATE INDEX IF NOT EXISTS idx_fact_demo    ON prod.Fact_Oferta_Nieruchomosci(ID_Demografii);
CREATE INDEX IF NOT EXISTS idx_dim_lokal_src  ON prod.Dim_Lokal(source_id, listing_type);
CREATE INDEX IF NOT EXISTS idx_dim_bud_src    ON prod.Dim_Budynek(source_id, listing_type);
CREATE INDEX IF NOT EXISTS idx_dim_infra_src  ON prod.Dim_Infrastruktura(source_id, listing_type);
CREATE INDEX IF NOT EXISTS idx_dim_demo_miasto ON prod.Dim_Demografia(Miasto_GUS);
