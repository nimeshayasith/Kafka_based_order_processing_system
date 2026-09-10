"""Small CLI to inspect the contents of the 'orders-dlq' topic."""
import functools
import os

print = functools.partial(print, flush=True)

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
DLQ_TOPIC = "orders-dlq"
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schemas", "order_dlq.avsc")


def build_dlq_consumer():
    with open(SCHEMA_PATH) as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": avro_deserializer,
        "group.id": "dlq-viewer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return DeserializingConsumer(consumer_conf)


def main():
    consumer = build_dlq_consumer()
    consumer.subscribe([DLQ_TOPIC])

    print(f"[dlq-viewer] reading from '{DLQ_TOPIC}' (Ctrl+C to stop)...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[dlq-viewer] error: {msg.error()}")
                continue

            record = msg.value()
            if record is None:
                continue
            print(
                f"[DLQ] orderId={record['orderId']} product={record['product']} "
                f"price={record['price']:.2f} retries={record['retries']} "
                f"reason={record['errorReason']} failedAtEpochMs={record['failedAtEpochMs']}"
            )
    except KeyboardInterrupt:
        print("[dlq-viewer] stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
