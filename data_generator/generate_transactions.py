"""
Synthetic real estate / land transaction data generator.

WHY THIS SCRIPT EXISTS
-----------------------
In a real data engineering role (e.g. at ANCFCC), you rarely get a clean,
ready-made dataset. You get raw records from multiple offices/systems,
with inconsistent formats, missing fields, and occasional errors.

Instead of downloading a pre-cleaned Kaggle dataset, we generate our own
data here so we can:
  1. Control volume (simulate a realistic scale, e.g. hundreds of thousands
     of transactions, which is what actually justifies using Spark instead
     of plain pandas).
  2. Inject REALISTIC and DOCUMENTED data quality issues, so the batch ETL
     step has genuine cleaning work to do (and you can explain each issue
     by name in your oral defense).
  3. Reuse this same generator later in "streaming mode" to feed Kafka
     with one event at a time (Palier 2), instead of writing a second,
     unrelated data source.

This script currently runs in BATCH mode: it produces one large historical
CSV file, meant to be loaded once into the Data Lake as the initial
historical dataset.
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Reference data: kept intentionally realistic and Morocco-specific, since
# the project is framed around a land/property registry use case.
# ---------------------------------------------------------------------------

REGIONS_CITIES = {
    "Casablanca-Settat": ["Casablanca", "Mohammedia", "Settat", "Berrechid"],
    "Rabat-Sale-Kenitra": ["Rabat", "Sale", "Kenitra", "Temara"],
    "Marrakech-Safi": ["Marrakech", "Safi", "Essaouira"],
    "Fes-Meknes": ["Fes", "Meknes", "Ifrane"],
    "Tanger-Tetouan-Al Hoceima": ["Tanger", "Tetouan", "Al Hoceima"],
    "Souss-Massa": ["Agadir", "Taroudant"],
}

PROPERTY_TYPES = ["apartment", "villa", "land", "commercial"]

# Registration status: mirrors the real land registry concept of a property's
# legal standing. This field is the strongest thematic link to ANCFCC's
# actual mission, and it is deliberately imbalanced (most transactions are
# on already-registered property), which is realistic AND gives you a data
# quality / class imbalance story to tell, similar to the emirate_Dubai
# feature importance nuance in your UAE project.
REGISTRATION_STATUS = ["registered"] * 80 + ["in_progress"] * 15 + ["disputed"] * 5

SOURCE_OFFICES = ["office_casablanca", "office_rabat", "office_marrakech", "office_fes", "office_tanger"]

# ---------------------------------------------------------------------------
# Data quality issues injected on purpose (each one is a talking point):
#
# 1. Missing surface_m2 for ~3% of "land" records (land parcels are
#    sometimes registered by boundary description rather than exact area
#    at the time of the transaction).
# 2. Inconsistent price formatting: most rows store price as a plain
#    number, but a subset (simulating exports from an older legacy system)
#    store it as text with thousand separators, e.g. "1,250,000".
# 3. A handful of full duplicate rows (simulating the same transaction
#    re-exported by two different offices) -- this is the same category
#    of issue you found in your Moroccan dataset for the UAE project
#    (25 listings duplicated 187 times), but here injected on purpose and
#    at a controlled, known rate so you can verify your cleaning logic
#    catches exactly the right number.
# 4. Occasional missing city (kept region, city unknown) -- simulates
#    incomplete address capture at the point of registration.
# ---------------------------------------------------------------------------

def random_date(start_year=2019, end_year=2026):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 7, 31)
    delta_days = (end - start).days
    return start + timedelta(days=random.randint(0, delta_days))


def generate_price(property_type: str) -> float:
    """Return a base price in MAD, roughly scaled by property type."""
    base_ranges = {
        "apartment": (350_000, 2_500_000),
        "villa": (1_200_000, 8_000_000),
        "land": (150_000, 3_000_000),
        "commercial": (500_000, 5_000_000),
    }
    low, high = base_ranges[property_type]
    return round(random.uniform(low, high), 2)


def generate_row(row_id: int) -> dict:
    region = random.choice(list(REGIONS_CITIES.keys()))
    city = random.choice(REGIONS_CITIES[region])
    property_type = random.choice(PROPERTY_TYPES)
    price = generate_price(property_type)

    # Issue 1: missing surface for some land parcels
    if property_type == "land" and random.random() < 0.03:
        surface = ""
    else:
        surface = round(random.uniform(40, 1200), 1)

    # Issue 2: inconsistent price formatting for ~10% of rows
    if random.random() < 0.10:
        price_field = f"{price:,.0f}".replace(",", ",")  # e.g. "1,250,000"
    else:
        price_field = str(price)

    # Issue 4: occasional missing city
    if random.random() < 0.02:
        city = ""

    return {
        "transaction_id": f"TX{row_id:07d}",
        "listing_id": f"LST{row_id:07d}",
        "property_type": property_type,
        "region": region,
        "city": city,
        "surface_m2": surface,
        "price_mad": price_field,
        "registration_status": random.choice(REGISTRATION_STATUS),
        "source_office": random.choice(SOURCE_OFFICES),
        "transaction_date": random_date().strftime("%Y-%m-%d"),
    }


def generate_dataset(n_rows: int, duplicate_rate: float = 0.01) -> list:
    rows = [generate_row(i) for i in range(1, n_rows + 1)]

    # Issue 3: inject full duplicate rows at a known, controlled rate
    n_duplicates = int(n_rows * duplicate_rate)
    duplicates = [random.choice(rows).copy() for _ in range(n_duplicates)]
    rows.extend(duplicates)

    random.shuffle(rows)
    return rows


def main():
    n_rows = 200_000  # large enough to make Spark's distributed processing meaningful
    output_path = Path(__file__).parent / "historical_transactions.csv"

    print(f"Generating {n_rows:,} base transactions (plus duplicates)...")
    rows = generate_dataset(n_rows)

    fieldnames = list(rows[0].keys())
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Done. Wrote {len(rows):,} rows to {output_path}")
    print("Known injected issues:")
    print("  - missing surface_m2 for ~3% of land parcels")
    print("  - ~10% of rows have price_mad as formatted text, not plain numbers")
    print(f"  - ~{int(n_rows * 0.01):,} full duplicate rows injected")
    print("  - ~2% of rows have missing city")


if __name__ == "__main__":
    main()