# CLAUDE.md

Guidance for Claude Code (or any assistant) working in this repository.

## Project

Big Data Analysis module assignment: a Kafka-based order processing system.

**Requirements (from assignment brief):**
- Producer and consumer exchange `Order` messages over Kafka.
- Messages are Avro-serialized against `order.avsc`.
- Consumer maintains a real-time running average of `price`.
- Consumer has retry logic for transient failures (with backoff).
- Messages that exhaust retries go to a Dead Letter Queue (DLQ) topic instead of being dropped.
- System must be demoed live and submitted via Git.
- Language is free choice — this repo uses **Python** (`confluent-kafka` + Schema Registry).

## Order schema (`order.avsc`)

| Field     | Type   | Notes                          |
|-----------|--------|---------------------------------|
| orderId   | string | e.g. "1001"                    |
| product   | string | e.g. "Item1"                   |
| price     | float  | randomized for the assignment  |

```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price", "type": "float"}
  ]
}
```

## Architecture

```
producer.py  --> [orders topic]  --> consumer.py --> running avg (stdout/log)
                                            |
                                       retry (backoff)
                                            |
                                     [orders-dlq topic]  (on exhausted retries)
```

## Repo layout

```
/producer/producer.py
/consumer/consumer.py
/schemas/order.avsc
docker-compose.yml       # Kafka + Schema Registry (Confluent images)
requirements.txt
README.md                # setup + run instructions
```

## Implementation plan (do these in order)

1. **Infra**: `docker-compose.yml` with Kafka (KRaft mode, no Zookeeper needed) + `cp-schema-registry`. Confirm both come up with `docker compose up -d` and Schema Registry responds on `localhost:8081`.
2. **Schema**: add `schemas/order.avsc` exactly as above.
3. **Producer**: `producer.py`
   - Uses `confluent_kafka.avro.AvroProducer` (or `AvroSerializer` + `SerializingProducer`).
   - Generates orders with random `orderId`, `product` from a small fixed list, `price` random float.
   - Publishes on a loop with a short sleep, to `orders` topic.
4. **Consumer — base**: `consumer.py`
   - `AvroConsumer`/`DeserializingConsumer` subscribed to `orders`.
   - On each message, update `count`/`sum`, print running average.
5. **Consumer — retry logic**:
   - Wrap message handling in try/except.
   - Simulate transient failure (e.g., random chance of raising an exception, or a toggle for the demo).
   - Retry N times with exponential backoff before giving up.
   - Only commit the offset after success or after routing to DLQ (don't lose or duplicate messages).
6. **Consumer — DLQ**:
   - On exhausted retries, produce the failed message (plus error reason) to `orders-dlq` topic, using the same or a DLQ-specific Avro schema.
   - Then commit the original offset so the consumer moves on.
7. **Demo readiness**:
   - A way to force a transient failure (recovers via retry) and a permanent failure (lands in DLQ) on command, for the live demo.
   - A small consumer/CLI to show DLQ contents.
8. **README**: how to start the stack, run producer/consumer, and trigger both failure demos.

## Conventions for whoever (or whatever) edits this repo

- Keep producer and consumer runnable independently (`python producer/producer.py`, `python consumer/consumer.py`).
- Don't commit `.env` or broker credentials if any get added.
- Commit incrementally per the steps above (schema → producer → consumer → retry → DLQ) rather than one large commit.
