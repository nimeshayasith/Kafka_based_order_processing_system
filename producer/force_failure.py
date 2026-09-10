"""Demo helper: publish a single order that forces a transient or
permanent failure on the consumer, on command.

Usage:
  python producer/force_failure.py transient
  python producer/force_failure.py permanent
"""
import argparse
import random

from producer import build_producer, delivery_report, TOPIC

TRIGGER_PRODUCTS = {
    "transient": "FORCE_TRANSIENT_FAIL",
    "permanent": "FORCE_PERMANENT_FAIL",
}


def main():
    parser = argparse.ArgumentParser(description="Force a specific consumer failure path for the demo")
    parser.add_argument("kind", choices=TRIGGER_PRODUCTS.keys())
    args = parser.parse_args()

    producer = build_producer()
    order = {
        "orderId": str(random.randint(1000, 1000000)),
        "product": TRIGGER_PRODUCTS[args.kind],
        "price": round(random.uniform(5.0, 500.0), 2),
    }
    producer.produce(topic=TOPIC, key=order["orderId"], value=order, on_delivery=delivery_report)
    producer.flush()
    print(f"[force_failure] sent {args.kind} trigger order: {order}")


if __name__ == "__main__":
    main()
