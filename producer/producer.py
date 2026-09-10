"""Producer: publishes random Order messages (Avro) to the 'orders' topic."""
import argparse
import os
import random
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = "orders"
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schemas", "order.avsc")

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def order_to_dict(order, ctx):
    return order


def build_producer():
    with open(SCHEMA_PATH) as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, schema_str, order_to_dict)

    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
    }
    return SerializingProducer(producer_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] delivery failed: {err}")
    else:
        print(f"[producer] delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def make_order(order_id):
    return {
        "orderId": str(order_id),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def main():
    parser = argparse.ArgumentParser(description="Order producer")
    parser.add_argument("--count", type=int, default=0, help="number of orders to send (0 = infinite)")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between sends")
    args = parser.parse_args()

    producer = build_producer()
    order_id = random.randint(1000, 1000000)

    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            order = make_order(order_id)
            producer.produce(topic=TOPIC, key=order["orderId"], value=order, on_delivery=delivery_report)
            producer.poll(0)
            print(f"[producer] sent order {order}")
            order_id += 1
            sent += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("[producer] interrupted, flushing...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
