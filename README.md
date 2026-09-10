# Kafka Order Processing System

Producer/consumer pair exchanging Avro-serialized `Order` messages over
Kafka. The consumer keeps a real-time running average of `price`, retries
transient failures with exponential backoff, and routes permanently-failed
messages to a Dead Letter Queue (`orders-dlq`), with a tool to replay fixed
DLQ messages back into the pipeline.

## Prerequisites

- Docker Desktop
- Python 3.9+

## 1. Start the stack

```
docker compose up -d
```

This brings up a Kafka broker (KRaft mode, no ZooKeeper), a Schema
Registry, and a one-shot `kafka-init` job that creates `orders` and
`orders-dlq` with **3 partitions each** — enough to demo partition
assignment and consumer-group rebalancing (see step 3) even on one broker.
Confirm everything is up:

```
docker compose ps
curl http://localhost:8081/subjects
```

**Optional — true multi-broker replication demo:** `docker-compose.multibroker.yml`
stands up a dedicated KRaft controller plus 3 separate broker containers with
replication factor 3, so the dashboard's cluster panel shows real partition
leaders/replicas/ISR spread across different brokers (matching a textbook
Kafka cluster diagram), not just a single broker owning everything. It's
**meaningfully heavier** — 4 extra JVM containers — and on constrained
Docker Desktop/WSL2 setups can make the whole host sluggish or cause the
KRaft quorum to struggle to stabilize; only reach for it if your machine has
Docker allocated several CPUs/GB of RAM to spare, and prefer the default
single-broker stack for the actual demo/submission:

```
docker compose -f docker-compose.multibroker.yml up -d
```

(Use the matching `docker compose -f docker-compose.multibroker.yml down -v`
to tear it down, and unset/reset `KAFKA_BOOTSTRAP_SERVERS` back to
`localhost:9092` when you switch back to the default single-broker stack.)

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
message processed, and will retry/DLQ as needed. On startup (and on every
rebalance — e.g. if you start a second consumer instance) it logs which
partitions the group coordinator assigned it:

```
[consumer:pid-1234] GROUP JOIN: assigned partitions -> orders[0], orders[1], orders[2]
```

Set `CONSUMER_INSTANCE_ID=A` (and `B`, `C`, ...) and start multiple
instances in separate terminals to see partitions redistributed live
across consumers in the same group — a real Kafka rebalance, not a
simulation.

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

## 6. Replay DLQ messages

After some messages have landed in `orders-dlq`, close the loop by
replaying them back into `orders`:

```
python dlq_viewer/dlq_replay.py
```

By default this simulates a fix: any forced-failure trigger product gets
swapped for a normal product before replay, so the message now succeeds.
Use `--no-fix` to replay records unchanged (useful to show an unfixed
permanent failure landing back in the DLQ), or `--max N` to cap how many
messages get replayed. It uses its own consumer group (`dlq-replay`) with
manual offset commits, so re-running it only replays messages it hasn't
handled yet.

## 7. Live demo dashboard

With the consumer already running, start the dashboard in another terminal:

```
python dashboard/dashboard.py
```

Open [http://localhost:5000](http://localhost:5000). It shows, updating
every second:

- Running average (with a sparkline) and a live order feed
- DLQ entries with failure reason and retry count
- Buttons to fire the transient/permanent failure triggers without
  switching terminals
- **Cluster topology**: every partition of `orders`/`orders-dlq` with its
  leader broker, replica set, and in-sync replicas (ISR) — the actual
  Kafka replication picture, pulled live via the Admin API
- **Consumer group panel**: which consumer instance(s) are currently in
  `order-consumer-group`, which partitions each was assigned, and total
  consumer lag — this is what "how does the broker enroll a consumer"
  looks like in practice: the group coordinator tracks membership and
  reassigns partitions on join/leave

The dashboard consumes with its own consumer groups (`dashboard-orders-*`,
`dashboard-dlq-*`), so it never interferes with `consumer.py`'s offsets.

## 8. Schema evolution demo

Show the Schema Registry enforcing compatibility, and a producer using an
evolved schema without breaking the running consumer:

```
python schemas/check_compatibility.py
```

Registers/validates `schemas/order_v2.avsc` (adds an optional `currency`
field with a default) against the `orders-value` subject — it's accepted
because it's backward-compatible — then attempts to register a genuinely
breaking schema (`price` changed from `float` to `string`, no default) and
shows the registry **rejecting** it.

```
python producer/producer_v2.py --count 5
```

Publishes orders using the v2 schema (with `currency`) to the same `orders`
topic. `consumer.py` keeps running unmodified — it's still reading with the
v1 schema, and Avro's schema resolution rules mean it simply ignores the
extra `currency` field on those records.

## 9. Inspect the DLQ (raw)

```
python dlq_viewer/dlq_viewer.py
```

Prints every message that landed in `orders-dlq`, along with the failure
reason, timestamp, and number of retries attempted.

## Running the tests

```
pytest tests/ -v
```

Unit tests exercise the retry/backoff/DLQ decision logic
(`handle_with_retry`, `process_order`) directly — no live broker needed —
using the same deterministic `FORCE_TRANSIENT_FAIL`/`FORCE_PERMANENT_FAIL`
trigger products the live demo uses.

## Delivery semantics (at-least-once, not exactly-once)

`enable.auto.commit=False` and the consumer only commits an offset after
the order either (a) processes successfully or (b) is safely written to
the DLQ — never before. This guarantees **no message is silently lost**.

It does **not** guarantee exactly-once processing: if the consumer crashes
after processing an order (updating the running average) but before the
commit call completes, that order will be redelivered and reprocessed on
restart, incrementing the running average twice for one real order. This
is standard **at-least-once** delivery, the same trade-off most Kafka
consumers make in exchange for never dropping data. A fully exactly-once
pipeline would need idempotent processing (e.g. deduplicating by
`orderId` with a persisted seen-set, or Kafka transactions) — out of scope
here but worth calling out explicitly.

## Configuration

Environment variables (all optional, defaults shown):

| Variable                        | Default                                       | Purpose                                   |
|----------------------------------|------------------------------------------------|--------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`        | `localhost:9092`                                | Kafka broker address(es) — set to `localhost:9092,localhost:9093,localhost:9094` when using `docker-compose.multibroker.yml` |
| `SCHEMA_REGISTRY_URL`            | `http://localhost:8081`                         | Schema Registry address                    |
| `CONSUMER_MAX_RETRIES`           | `3`                                             | Retry attempts before routing to DLQ       |
| `CONSUMER_INITIAL_BACKOFF`       | `0.5`                                           | Initial backoff (seconds), doubles each try|
| `CONSUMER_RANDOM_FAILURE_RATE`   | `0.1`                                           | Probability an ordinary order simulates a transient failure per attempt |
| `CONSUMER_INSTANCE_ID`           | `pid-<process id>`                              | Label for this consumer instance in logs — set distinct values to demo rebalancing across multiple instances |

## Repo layout

```
/producer/producer.py           # publishes random orders to 'orders' (v1 schema)
/producer/producer_v2.py        # schema evolution demo: publishes with order_v2.avsc (adds currency)
/producer/force_failure.py      # demo helper: force a transient/permanent failure
/consumer/consumer.py           # running average + retry/backoff + DLQ routing + rebalance logging
/dlq_viewer/dlq_viewer.py       # CLI to inspect 'orders-dlq'
/dlq_viewer/dlq_replay.py       # replays (optionally "fixed") DLQ messages back into 'orders'
/dashboard/dashboard.py         # live web UI: running avg, feed, DLQ, cluster topology, consumer group, demo triggers
/schemas/order.avsc             # Avro schema for Order (v1)
/schemas/order_v2.avsc          # Avro schema for Order (v2, adds optional currency)
/schemas/order_dlq.avsc         # Avro schema for DLQ records (Order + failure metadata)
/schemas/check_compatibility.py # demo: Schema Registry compatibility checks (accept v2, reject a breaking change)
/tests/test_consumer_retry.py   # unit tests for retry/backoff/DLQ decision logic
docker-compose.yml              # single-broker Kafka cluster (KRaft) + Schema Registry + topic init job
docker-compose.multibroker.yml  # optional: dedicated controller + 3 brokers, RF=3, for a real replication demo
requirements.txt
```

## Shutting down

```
docker compose down -v
```
