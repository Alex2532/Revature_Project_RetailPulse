import json
from kafka import KafkaConsumer

TOPIC = "retail-sales-summary"
OUTPUT_FILE = "/opt/airflow/kafka_scripts/data/consumed_events.jsonl"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="retailpulse-consumer-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=5000 
)

def main():
    print(f"Listening on topic '{TOPIC}'... \n")
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        for message in consumer:
            event = message.value
            print(f"Consumed: {event}")
            f.write(json.dumps(event) + "\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()