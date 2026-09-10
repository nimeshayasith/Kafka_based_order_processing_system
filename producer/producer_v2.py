"""Schema evolution demo: publishes orders using order_v2.avsc (adds an
optional `currency` field with default "USD") to the SAME 'orders' topic
and subject as producer.py's v1 schema.

Because `currency` has a default value, this is a backward-compatible
change under the Schema Registry's default BACKWARD compatibility mode --
registering v2 succeeds, and consumer.py (still reading with the v1
schema) keeps working unmodified: Avro's schema resolution rules mean the
v1 reader simply ignores the extra `currency` field on v2-written records.

Usage:
  python producer/producer_v2.py --count 5
"""
import argparse
import functools
import os
import random
import time

print = functools.partial(print, flush=True)

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from producer import BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, TOPIC, PRODUCTS, delivery_report

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schemas", "order_v2.avsc")
CURRENCIES = ["USD", "EUR", "GBP", "LKR"]


def order_v2_to_dict(order, ctx):
    return order


def build_producer_v2():
    with open(SCHEMA_PATH) as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, schema_str, order_v2_to_dict)

    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
    }
    return SerializingProducer(producer_conf)


def make_order_v2(order_id):
    return {
        "orderId": str(order_id),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
        "currency": random.choice(CURRENCIES),
    }


def main():
    parser = argparse.ArgumentParser(description="Order producer using the evolved (v2) schema")
    parser.add_argument("--count", type=int, default=5, help="number of v2 orders to send")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between sends")
    args = parser.parse_args()

    print(f"[producer-v2] registering/using schema from {SCHEMA_PATH}")
    producer = build_producer_v2()
    order_id = random.randint(1000, 1000000)

    for _ in range(args.count):
        order = make_order_v2(order_id)
        producer.produce(topic=TOPIC, key=order["orderId"], value=order, on_delivery=delivery_report)
        producer.poll(0)
        print(f"[producer-v2] sent v2 order (with currency) {order}")
        order_id += 1
        time.sleep(args.interval)

    producer.flush()
    print(
        "[producer-v2] done. Check consumer.py's log: it's still reading with the v1 "
        "schema and will process these v2 records fine, just without the currency field."
    )


if __name__ == "__main__":
    main()
