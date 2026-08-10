import csv
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "retail-sales-summary"
INPUT_FILE = "/opt/airflow/kafka_scripts/data/monthly_category_sales.csv"

def build_event(row):
    return {
        "event_id": f"SALES-{row['sales_month']}-{row['category']}",
        "event_type": "MONTHLY_CATEGORY_SALES_READY",
        "sales_month": row["sales_month"],
        "category": row["category"],
        "order_count": int(row["order_count"]),
        "total_quantity": int(row["total_quantity"]),
        "total_revenue": float(row["total_revenue"])
    }

def main():
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            event = build_event(row)
            producer.send(TOPIC, value=event)
            print(f"Sent: {event}")
            count += 1

    producer.flush()
    producer.close()
    print(f"\nDone. Published {count} events to '{TOPIC}'.")

if __name__ == "__main__":
    main()