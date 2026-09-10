"""Replay messages from 'orders-dlq' back into 'orders', closing the loop:
produce -> fail -> DLQ -> (fix) -> replay -> succeeds.

By default, orders that used a demo failure trigger (FORCE_TRANSIENT_FAIL /
FORCE_PERMANENT_FAIL) are "fixed" by swapping in a normal product before
replay, simulating a real remediation (e.g. a downstream bug getting
patched). Pass --no-fix to replay messages byte-for-byte instead, which is
useful to demonstrate that an unfixed permanent failure lands in the DLQ
again.

Uses its own consumer group ('dlq-replay') with manual offset commits, so
re-running this script only replays DLQ messages it hasn't already handled.

Usage:
  python dlq_viewer/dlq_replay.py             # replay everything currently in the DLQ, fixing forced failures
  python dlq_viewer/dlq_replay.py --no-fix     # replay as-is
  python dlq_viewer/dlq_replay.py --max 5      # replay at most 5 messages then stop
"""
import argparse
import functools
import os
import random
import sys

print = functools.partial(print, flush=True)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producer"))
from producer import build_producer, delivery_report, TOPIC as ORDERS_TOPIC, PRODUCTS  # noqa: E402

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
DLQ_TOPIC = "orders-dlq"
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schemas", "order_dlq.avsc")

FORCED_FAILURE_PRODUCTS = {"FORCE_TRANSIENT_FAIL", "FORCE_PERMANENT_FAIL"}


def build_dlq_consumer():
    with open(SCHEMA_PATH) as f:
        schema_str = f.read()
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(registry, schema_str)
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": deserializer,
        "group.id": "dlq-replay",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return DeserializingConsumer(conf)


def fixed_order(record):
    """Simulate a remediation: swap a forced-failure marker for a real product."""
    product = record["product"]
    if product in FORCED_FAILURE_PRODUCTS:
        product = random.choice(PRODUCTS)
    return {"orderId": record["orderId"], "product": product, "price": record["price"]}


def main():
    parser = argparse.ArgumentParser(description="Replay DLQ messages back into 'orders'")
    parser.add_argument("--no-fix", action="store_true", help="replay records unchanged, without fixing forced failures")
    parser.add_argument("--max", type=int, default=None, help="stop after replaying this many messages")
    parser.add_argument("--poll-timeout", type=float, default=3.0, help="seconds to wait for a DLQ message before giving up")
    args = parser.parse_args()

    consumer = build_dlq_consumer()
    producer = build_producer()
    consumer.subscribe([DLQ_TOPIC])

    replayed = 0
    print(f"[dlq-replay] scanning '{DLQ_TOPIC}' for messages to replay into '{ORDERS_TOPIC}'...")
    try:
        while args.max is None or replayed < args.max:
            msg = consumer.poll(args.poll_timeout)
            if msg is None:
                print("[dlq-replay] no more DLQ messages waiting, stopping")
                break
            if msg.error():
                print(f"[dlq-replay] consumer error: {msg.error()}")
                continue

            record = msg.value()
            if record is None:
                consumer.commit(message=msg)
                continue

            order = record if args.no_fix else fixed_order(record)
            producer.produce(topic=ORDERS_TOPIC, key=order["orderId"], value=order, on_delivery=delivery_report)
            producer.flush()

            print(
                f"[dlq-replay] replayed order {record['orderId']} "
                f"(originally failed: {record['errorReason']}) -> product={order['product']}"
            )
            consumer.commit(message=msg)
            replayed += 1
    except KeyboardInterrupt:
        print("[dlq-replay] interrupted")
    finally:
        consumer.close()

    print(f"[dlq-replay] done, replayed {replayed} message(s)")


if __name__ == "__main__":
    main()
