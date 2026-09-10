# Kafka Order Processing System

Producer/consumer pair exchanging Avro-serialized `Order` messages over Kafka.
The consumer keeps a real-time running average of `price`, retries transient
failures with exponential backoff, and routes permanently-failed messages to
a Dead Letter Queue (`orders-dlq`).

## Prerequisites

- Docker Desktop
- Python 3.9+

## 1. Start the stack

```
docker compose up -d
```

Wait for both containers to be healthy, then confirm the Schema Registry is up:

```
curl http://localhost:8081/subjects
```

(should return `[]` on a fresh cluster).

## 2. Install Python dependencies

```
python -m venv .venv
.venv\Scripts\activate      # Windows
pip install -r requirements.txt
```

## 3. Run the consumer

In one terminal:

```
python consumer/consumer.py
```

This subscribes to `orders`, prints a running average of `price` for each
message processed, and will retry/DLQ as needed.

## 4. Run the producer

In another terminal:

```
python producer/producer.py
```

Sends a random order (`orderId`, `product`, `price`) once per second,
forever. Use `--count N` to send a fixed number, `--interval S` to change
the pace.

## 5. Demo: forcing failures on command

With the consumer already running, trigger each failure path from a third
terminal:

**Transient failure (recovers via retry):**

```
python producer/force_failure.py transient
```

The consumer log will show 1-2 failed attempts with increasing backoff,
then a successful processing of that order.

**Permanent failure (exhausts retries, lands in DLQ):**

```
python producer/force_failure.py permanent
```

The consumer log will show all retries failing, then the message being
routed to `orders-dlq`.

The consumer also randomly simulates transient failures on ordinary orders
(~10% of attempts, configurable via `CONSUMER_RANDOM_FAILURE_RATE`) so
retry behavior is visible even without the explicit triggers.

## 6. Inspect the DLQ

```
python dlq_viewer/dlq_viewer.py
```

Prints every message that landed in `orders-dlq`, along with the failure
reason, timestamp, and number of retries attempted.

## Configuration

Environment variables (all optional, defaults shown):

| Variable                        | Default                | Purpose                                   |
|----------------------------------|-------------------------|--------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`        | `localhost:9092`        | Kafka broker address                       |
| `SCHEMA_REGISTRY_URL`            | `http://localhost:8081` | Schema Registry address                    |
| `CONSUMER_MAX_RETRIES`           | `3`                     | Retry attempts before routing to DLQ       |
| `CONSUMER_INITIAL_BACKOFF`       | `0.5`                   | Initial backoff (seconds), doubles each try|
| `CONSUMER_RANDOM_FAILURE_RATE`   | `0.1`                   | Probability an ordinary order simulates a transient failure per attempt |

## Repo layout

```
/producer/producer.py       # publishes random orders to 'orders'
/producer/force_failure.py  # demo helper: force a transient/permanent failure
/consumer/consumer.py       # running average + retry/backoff + DLQ routing
/dlq_viewer/dlq_viewer.py   # CLI to inspect 'orders-dlq'
/schemas/order.avsc         # Avro schema for Order
/schemas/order_dlq.avsc     # Avro schema for DLQ records (Order + failure metadata)
docker-compose.yml          # Kafka (KRaft) + Schema Registry
requirements.txt
```

## Shutting down

```
docker compose down -v
```
