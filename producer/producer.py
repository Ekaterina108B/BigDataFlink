import os
import csv
import json
import time
import glob
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "sales_raw")
CSV_DIR = os.environ.get("CSV_DIR", "/csv_data")


def connect_producer(retries=30, delay=5):
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return producer
        except (KafkaError, OSError) as e:
            print(f"Attempt {attempt}/{retries}: Kafka not ready ({e}), retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after all retries")


def get_csv_files(directory):
    files = sorted(glob.glob(os.path.join(directory, "MOCK_DATA*.csv")))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {directory}")
    return files


def main():
    producer = connect_producer()
    csv_files = get_csv_files(CSV_DIR)
    print(f"Found {len(csv_files)} CSV files")

    total = 0
    for filepath in csv_files:
        filename = os.path.basename(filepath)
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total += 1
                row["global_sale_id"] = str(total)
                producer.send(TOPIC, value=row)

        print(f"Sent file: {filename} (running total: {total})")

    producer.flush()
    producer.close()
    print(f"Done! Total messages sent: {total}")


if __name__ == "__main__":
    main()
