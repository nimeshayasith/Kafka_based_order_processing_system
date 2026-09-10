"""Consumer: reads Order messages, keeps a running average of price,
retries transient failures with exponential backoff, and routes
messages that exhaust retries to the 'orders-dlq' topic.

Demo triggers (set an order's `product` field to one of these to force
a specific failure path on command):
  FORCE_TRANSIENT_FAIL  -> fails the first couple of attempts, then
                            succeeds within the retry budget.
  FORCE_PERMANENT_FAIL  -> always fails, exhausts retries, lands in DLQ.
"""
import functools
import os
import random
import time

print = functools.partial(print, flush=True)

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
ORDERS_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"

MAX_RETRIES = int(os.environ.get("CONSUMER_MAX_RETRIES", "3"))
INITIAL_BACKOFF_SECONDS = float(os.environ.get("CONSUMER_INITIAL_BACKOFF", "0.5"))
RANDOM_TRANSIENT_FAILURE_RATE = float(os.environ.get("CONSUMER_RANDOM_FAILURE_RATE", "0.1"))

SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "..", "schemas")


class TransientProcessingError(Exception):
    """Simulated recoverable failure (e.g. downstream service hiccup)."""


class PermanentProcessingError(Exception):
    """Simulated unrecoverable failure."""


def build_consumer():
    with open(os.path.join(SCHEMA_DIR, "order.avsc")) as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": avro_deserializer,
        "group.id": "order-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return DeserializingConsumer(consumer_conf)


def build_dlq_producer():
    with open(os.path.join(SCHEMA_DIR, "order_dlq.avsc")) as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, schema_str, lambda obj, ctx: obj)

    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
    }
    return SerializingProducer(producer_conf)


class RunningAverage:
    def __init__(self):
        self.count = 0
        self.total = 0.0

    def update(self, price):
        self.count += 1
        self.total += price
        return self.total / self.count


def process_order(order, attempt):
    """Raises TransientProcessingError / PermanentProcessingError to simulate
    failures; returns normally on success."""
    product = order["product"]

    if product == "FORCE_PERMANENT_FAIL":
        raise PermanentProcessingError(f"order {order['orderId']} is a forced permanent failure")

    if product == "FORCE_TRANSIENT_FAIL":
        if attempt < 2:
            raise TransientProcessingError(
                f"order {order['orderId']} forced transient failure (attempt {attempt + 1})"
            )
        return

    if random.random() < RANDOM_TRANSIENT_FAILURE_RATE:
        raise TransientProcessingError(f"order {order['orderId']} simulated transient failure")


def handle_with_retry(order):
    """Attempts to process the order with exponential backoff.
    Returns (success: bool, error_reason: str | None, attempts_used: int)."""
    backoff = INITIAL_BACKOFF_SECONDS
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            process_order(order, attempt)
            return True, None, attempt + 1
        except PermanentProcessingError as e:
            return False, str(e), attempt + 1
        except TransientProcessingError as e:
            last_error = str(e)
            print(f"[consumer] transient failure on attempt {attempt + 1}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2

    return False, last_error, MAX_RETRIES


def send_to_dlq(dlq_producer, order, error_reason, attempts_used):
    dlq_record = {
        "orderId": order["orderId"],
        "product": order["product"],
        "price": order["price"],
        "errorReason": error_reason or "unknown error",
        "failedAtEpochMs": int(time.time() * 1000),
        "retries": attempts_used,
    }

    def on_delivery(err, msg):
        if err is not None:
            print(f"[consumer] FAILED to deliver to DLQ: {err}")
        else:
            print(f"[consumer] routed order {order['orderId']} to DLQ ({error_reason})")

    dlq_producer.produce(topic=DLQ_TOPIC, key=order["orderId"], value=dlq_record, on_delivery=on_delivery)
    dlq_producer.flush()


INSTANCE_ID = os.environ.get("CONSUMER_INSTANCE_ID", f"pid-{os.getpid()}")


def on_assign(consumer, partitions):
    assigned = ", ".join(f"{p.topic}[{p.partition}]" for p in partitions)
    print(f"[consumer:{INSTANCE_ID}] GROUP JOIN: assigned partitions -> {assigned or '(none)'}")


def on_revoke(consumer, partitions):
    revoked = ", ".join(f"{p.topic}[{p.partition}]" for p in partitions)
    print(f"[consumer:{INSTANCE_ID}] REBALANCE: partitions revoked -> {revoked or '(none)'}")


def main():
    consumer = build_consumer()
    dlq_producer = build_dlq_producer()
    # on_assign/on_revoke fire every time the consumer group coordinator
    # (re)assigns partitions -- on first join, and again whenever a
    # consumer joins/leaves the group and the group rebalances.
    consumer.subscribe([ORDERS_TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    running_avg = RunningAverage()

    print(f"[consumer:{INSTANCE_ID}] subscribing to '{ORDERS_TOPIC}', max_retries={MAX_RETRIES}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[consumer:{INSTANCE_ID}] consumer error: {msg.error()}")
                continue

            order = msg.value()
            if order is None:
                consumer.commit(message=msg)
                continue

            partition_tag = f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"
            success, error_reason, attempts_used = handle_with_retry(order)

            if success:
                avg = running_avg.update(order["price"])
                print(
                    f"[consumer:{INSTANCE_ID}] {partition_tag} processed order {order['orderId']} "
                    f"product={order['product']} price={order['price']:.2f} | "
                    f"running_avg={avg:.2f} (n={running_avg.count})"
                )
            else:
                print(f"[consumer:{INSTANCE_ID}] {partition_tag} retries exhausted for order {order['orderId']}: {error_reason}")
                send_to_dlq(dlq_producer, order, error_reason, attempts_used)

            # Commit only after success or after the message has safely
            # landed in the DLQ, so nothing is lost or duplicated.
            consumer.commit(message=msg)
    except KeyboardInterrupt:
        print(f"[consumer:{INSTANCE_ID}] interrupted, shutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
