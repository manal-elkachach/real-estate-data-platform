"""
Streaming producer: emits one real estate transaction event at a time to
Kafka, instead of writing a single large historical file.

WHY THIS REUSES THE SAME GENERATION LOGIC AS THE BATCH GENERATOR:
Same reasoning we set out at the start of this project -- one data model,
two delivery modes. In a real system, this would be the difference between
a nightly export job (batch) and a live event stream from a transaction
system (streaming); here, we simulate both from the same source logic so
the two pipelines are genuinely comparable.

This script requires the kafka-python library, which is NOT part of the
Spark image by default -- install it once with:
    pip install kafka-python --break-system-packages
"""

import json
import random
import time
from datetime import datetime, timedelta

from kafka import KafkaProducer

REGIONS_CITIES = {
    "Casablanca-Settat": ["Casablanca", "Mohammedia", "Settat", "Berrechid"],
    "Rabat-Sale-Kenitra": ["Rabat", "Sale", "Kenitra", "Temara"],
    "Marrakech-Safi": ["Marrakech", "Safi", "Essaouira"],
    "Fes-Meknes": ["Fes", "Meknes", "Ifrane"],
    "Tanger-Tetouan-Al Hoceima": ["Tanger", "Tetouan", "Al Hoceima"],
    "Souss-Massa": ["Agadir", "Taroudant"],
}
PROPERTY_TYPES = ["apartment", "villa", "land", "commercial"]
REGISTRATION_STATUS = ["registered"] * 80 + ["in_progress"] * 15 + ["disputed"] * 5
SOURCE_OFFICES = ["office_casablanca", "office_rabat", "office_marrakech", "office_fes", "office_tanger"]

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "real-estate-events"


def generate_price(property_type: str) -> float:
    base_ranges = {
        "apartment": (350_000, 2_500_000),
        "villa": (1_200_000, 8_000_000),
        "land": (150_000, 3_000_000),
        "commercial": (500_000, 5_000_000),
    }
    low, high = base_ranges[property_type]
    return round(random.uniform(low, high), 2)


def generate_event(row_id: int) -> dict:
    """
    Same shape as the batch CSV rows, but as a JSON-ready dict, and WITHOUT
    the deliberately injected data quality issues from the batch generator.
    Why: those issues simulated messy HISTORICAL exports from legacy
    systems. A live event stream represents transactions as they are
    entered today, through a single current system -- a different, and
    realistically cleaner, source. This distinction (why streaming data
    isn't automatically messier or cleaner than batch data -- it depends
    on the SOURCE, not the delivery mechanism) is worth having ready for
    the oral defense.
    """
    region = random.choice(list(REGIONS_CITIES.keys()))
    city = random.choice(REGIONS_CITIES[region])
    property_type = random.choice(PROPERTY_TYPES)

    return {
        "transaction_id": f"STREAM{row_id:07d}",
        "listing_id": f"LSTS{row_id:07d}",
        "property_type": property_type,
        "region": region,
        "city": city,
        "surface_m2": round(random.uniform(40, 1200), 1),
        "price_mad": generate_price(property_type),
        "registration_status": random.choice(REGISTRATION_STATUS),
        "source_office": random.choice(SOURCE_OFFICES),
        "transaction_date": datetime.now().strftime("%Y-%m-%d"),
        "event_timestamp": datetime.now().isoformat(),
    }


def main(interval_seconds: float = 2.0, num_events: int = None):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Producing events to topic '{TOPIC_NAME}' every {interval_seconds}s. Ctrl+C to stop.")
    row_id = 1
    try:
        while num_events is None or row_id <= num_events:
            event = generate_event(row_id)
            producer.send(TOPIC_NAME, value=event)
            print(f"Sent: {event['transaction_id']} | {event['property_type']} | {event['region']}")
            row_id += 1
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()