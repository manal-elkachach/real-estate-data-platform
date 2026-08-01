-- ---------------------------------------------------------------------------
-- Data Warehouse schema: star schema for real estate / land transactions.
--
-- WHY A STAR SCHEMA (and not one wide flat table):
-- Analytical queries here look like "average price by region and property
-- type, per quarter" -- they aggregate numeric measures (price, surface)
-- grouped by descriptive attributes (region, type, date, status). A star
-- schema separates these two roles explicitly:
--   - fact_transactions: one row per transaction, holding only numeric
--     measures + foreign keys. This table is large but narrow.
--   - dim_*: small reference tables holding descriptive text, each value
--     stored exactly once instead of repeated on every transaction row.
-- This keeps the fact table compact and makes joins/aggregations cheap,
-- which is the whole point of a warehouse used for reporting, as opposed
-- to an OLTP database used for transactional lookups.
-- ---------------------------------------------------------------------------

-- Dimensions are created first because the fact table's foreign keys
-- reference them.

CREATE TABLE IF NOT EXISTS dim_date (
    date_id         SERIAL PRIMARY KEY,
    full_date       DATE NOT NULL UNIQUE,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL  -- 1 = Monday ... 7 = Sunday
);

CREATE TABLE IF NOT EXISTS dim_property_type (
    property_type_id   SERIAL PRIMARY KEY,
    property_type_name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id    SERIAL PRIMARY KEY,
    region         VARCHAR(100) NOT NULL,
    city           VARCHAR(100),         -- nullable: some source rows have missing city
    UNIQUE (region, city)
);

CREATE TABLE IF NOT EXISTS dim_registration_status (
    registration_status_id   SERIAL PRIMARY KEY,
    status_name               VARCHAR(30) NOT NULL UNIQUE
);

-- Fact table: one row per cleaned, deduplicated transaction.
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id          VARCHAR(20) PRIMARY KEY,  -- business key from the source system
    listing_id              VARCHAR(20) NOT NULL,
    date_id                 INTEGER NOT NULL REFERENCES dim_date(date_id),
    property_type_id        INTEGER NOT NULL REFERENCES dim_property_type(property_type_id),
    location_id             INTEGER NOT NULL REFERENCES dim_location(location_id),
    registration_status_id  INTEGER NOT NULL REFERENCES dim_registration_status(registration_status_id),
    source_office            VARCHAR(50) NOT NULL,
    surface_m2                NUMERIC(10, 1),           -- nullable: some land parcels have no recorded surface
    price_mad                 NUMERIC(12, 2) NOT NULL
);

-- Indexes on foreign keys speed up the joins that every analytical query
-- against this fact table will perform.
CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_transactions(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_property_type ON fact_transactions(property_type_id);
CREATE INDEX IF NOT EXISTS idx_fact_location ON fact_transactions(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_status ON fact_transactions(registration_status_id);