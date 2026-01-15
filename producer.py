import csv
import json
import time
import argparse
from kafka import KafkaProducer

def make_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        linger_ms=50,
    )

def main():
    parser = argparse.ArgumentParser(description="CSV -> Kafka producer")
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument("--topic", required=True, help="Kafka topic name")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between messages")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of produced rows (0 = no limit)")
    args = parser.parse_args()

    producer = make_producer(args.bootstrap)

    sent = 0
    with open(args.csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Optional cleanup: strip whitespace from keys/values
            clean = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

            # Use a stable key if exists (helps partitioning). Adjust if your CSV has other id field.
            key = clean.get("VIN (1-10)") or clean.get("vin") or None

            producer.send(args.topic, key=key, value=clean)
            sent += 1

            if args.sleep > 0:
                time.sleep(args.sleep)

            if args.limit and sent >= args.limit:
                break

    producer.flush()
    producer.close()
    print(f"✅ Produced {sent} messages to topic '{args.topic}'")

if __name__ == "__main__":
    main()
