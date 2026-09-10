"""Live demo dashboard: a small local web UI showing the running average,
a live order feed, DLQ entries, and buttons to trigger the transient /
permanent failure demo on command.

Runs its own read-only Kafka consumers (separate consumer groups from the
real consumer.py, offset auto.offset.reset=latest) so it never interferes
with the assignment's actual consume/retry/DLQ pipeline -- it's purely an
observer + a producer for the demo trigger buttons.

Usage:
  python dashboard/dashboard.py
Then open http://localhost:5000
"""
import os
import random
import sys
import threading
import time
from collections import deque

from flask import Flask, jsonify, render_template_string

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producer"))
from producer import build_producer, delivery_report, TOPIC as ORDERS_TOPIC  # noqa: E402

from confluent_kafka import Consumer, DeserializingConsumer, TopicPartition
from confluent_kafka._model import ConsumerGroupTopicPartitions
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
DLQ_TOPIC = "orders-dlq"
MAIN_CONSUMER_GROUP = "order-consumer-group"
SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "..", "schemas")

TRIGGER_PRODUCTS = {
    "transient": "FORCE_TRANSIENT_FAIL",
    "permanent": "FORCE_PERMANENT_FAIL",
}

app = Flask(__name__)
state_lock = threading.Lock()
state = {
    "orders": {"count": 0, "total": 0.0, "avg": 0.0, "recent": deque(maxlen=20), "history": deque(maxlen=60)},
    "dlq": {"count": 0, "recent": deque(maxlen=20)},
    "cluster": {"brokers": [], "topics": {}, "consumer_group": {"state": "Unknown", "members": []}},
}


def build_schema_consumer(schema_file, group_id):
    with open(os.path.join(SCHEMA_DIR, schema_file)) as f:
        schema_str = f.read()
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(registry, schema_str)
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": deserializer,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    }
    return DeserializingConsumer(conf)


def orders_watch_loop():
    consumer = build_schema_consumer("order.avsc", f"dashboard-orders-{int(time.time())}")
    consumer.subscribe([ORDERS_TOPIC])
    while True:
        try:
            msg = consumer.poll(1.0)
        except Exception as e:
            # Topic may not exist yet (created lazily by the producer) --
            # keep retrying instead of letting the watcher thread die.
            print(f"[dashboard] orders watch error, retrying: {e}")
            time.sleep(2)
            continue
        if msg is None or msg.error():
            continue
        order = msg.value()
        if order is None:
            continue
        with state_lock:
            o = state["orders"]
            # Forced-failure trigger orders are demo noise, not real sales data.
            if order["product"] not in TRIGGER_PRODUCTS.values():
                o["count"] += 1
                o["total"] += order["price"]
                o["avg"] = o["total"] / o["count"]
                o["history"].append(round(o["avg"], 2))
            o["recent"].appendleft(
                {"orderId": order["orderId"], "product": order["product"], "price": round(order["price"], 2)}
            )


def dlq_watch_loop():
    consumer = build_schema_consumer("order_dlq.avsc", f"dashboard-dlq-{int(time.time())}")
    consumer.subscribe([DLQ_TOPIC])
    while True:
        try:
            msg = consumer.poll(1.0)
        except Exception as e:
            print(f"[dashboard] DLQ watch error, retrying: {e}")
            time.sleep(2)
            continue
        if msg is None or msg.error():
            continue
        rec = msg.value()
        if rec is None:
            continue
        with state_lock:
            d = state["dlq"]
            d["count"] += 1
            d["recent"].appendleft(
                {
                    "orderId": rec["orderId"],
                    "product": rec["product"],
                    "price": round(rec["price"], 2),
                    "errorReason": rec["errorReason"],
                    "retries": rec["retries"],
                }
            )


def _topic_partition_info(admin, topic_names, timeout=5):
    """Leader broker + replica set + in-sync-replica set per partition --
    this is what makes a Kafka cluster different from a plain queue: each
    partition has a single leader broker (handling all reads/writes for
    it) and followers replicating it, so the cluster survives a broker
    going down."""
    metadata = admin.list_topics(timeout=timeout)
    topics = {}
    for name in topic_names:
        topic_meta = metadata.topics.get(name)
        if topic_meta is None or topic_meta.error is not None:
            topics[name] = []
            continue
        partitions = []
        for pid, pmeta in sorted(topic_meta.partitions.items()):
            partitions.append(
                {
                    "partition": pid,
                    "leader": pmeta.leader,
                    "replicas": list(pmeta.replicas),
                    "isr": list(pmeta.isrs),
                }
            )
        topics[name] = partitions
    return topics


def _consumer_group_info(admin, group_id, timeout=5):
    """Which broker/consumer instance owns which partition right now, and
    how far behind (lag) each partition is -- this is the 'how does a
    consumer enroll' picture: the group coordinator assigns partitions to
    whichever consumer processes are currently in the group, and
    reassigns them (a rebalance) whenever one joins or leaves."""
    try:
        desc_futures = admin.describe_consumer_groups([group_id], request_timeout=timeout)
        group_desc = desc_futures[group_id].result()
    except Exception as e:
        return {"state": f"unavailable ({e})", "members": [], "total_lag": None}

    members = []
    for m in group_desc.members:
        assigned = [
            {"topic": tp.topic, "partition": tp.partition}
            for tp in (m.assignment.topic_partitions if m.assignment else [])
        ]
        members.append({"client_id": m.client_id, "host": m.host, "assigned": assigned})

    total_lag = None
    try:
        offsets_future = admin.list_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(group_id)], request_timeout=timeout
        )
        committed = offsets_future[group_id].result()
        watermark_consumer = Consumer(
            {"bootstrap.servers": BOOTSTRAP_SERVERS, "group.id": "dashboard-watermark-probe"}
        )
        total_lag = 0
        for tp in committed.topic_partitions:
            if tp.offset is None or tp.offset < 0:
                continue
            low, high = watermark_consumer.get_watermark_offsets(
                TopicPartition(tp.topic, tp.partition), timeout=timeout, cached=False
            )
            total_lag += max(0, high - tp.offset)
        watermark_consumer.close()
    except Exception:
        total_lag = None

    return {"state": str(group_desc.state), "members": members, "total_lag": total_lag}


def cluster_watch_loop():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    while True:
        try:
            metadata = admin.list_topics(timeout=5)
            brokers = sorted(
                [{"id": b.id, "host": b.host, "port": b.port} for b in metadata.brokers.values()],
                key=lambda b: b["id"],
            )
            topics = _topic_partition_info(admin, [ORDERS_TOPIC, DLQ_TOPIC])
            group = _consumer_group_info(admin, MAIN_CONSUMER_GROUP)

            with state_lock:
                state["cluster"] = {"brokers": brokers, "topics": topics, "consumer_group": group}
        except Exception as e:
            print(f"[dashboard] cluster watch error: {e}")
        time.sleep(3)


@app.route("/api/state")
def api_state():
    with state_lock:
        o = state["orders"]
        d = state["dlq"]
        return jsonify(
            {
                "orders": {
                    "count": o["count"],
                    "avg": round(o["avg"], 2),
                    "recent": list(o["recent"]),
                    "history": list(o["history"]),
                },
                "dlq": {"count": d["count"], "recent": list(d["recent"])},
                "cluster": state["cluster"],
            }
        )


@app.route("/api/force/<kind>", methods=["POST"])
def api_force(kind):
    if kind not in TRIGGER_PRODUCTS:
        return jsonify({"ok": False, "error": "unknown kind"}), 400

    producer = build_producer()
    order = {
        "orderId": str(random.randint(1000, 1000000)),
        "product": TRIGGER_PRODUCTS[kind],
        "price": round(random.uniform(5.0, 500.0), 2),
    }
    producer.produce(topic=ORDERS_TOPIC, key=order["orderId"], value=order, on_delivery=delivery_report)
    producer.flush()
    return jsonify({"ok": True, "order": order})


PAGE = """
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Order Pipeline Dashboard</title>
<style>
  body { font-family: system-ui, sans-serif; background: #0f1117; color: #e6e6e6; margin: 0; padding: 24px; }
  h1 { font-size: 20px; margin-bottom: 4px; }
  .sub { color: #9aa0a6; margin-bottom: 20px; font-size: 13px; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  .card { background: #161923; border: 1px solid #262a38; border-radius: 10px; padding: 16px; }
  .stat { font-size: 34px; font-weight: 700; }
  .label { color: #9aa0a6; font-size: 12px; text-transform: uppercase; letter-spacing: .05em; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 8px; }
  th, td { text-align: left; padding: 5px 8px; border-bottom: 1px solid #262a38; }
  th { color: #9aa0a6; font-weight: 500; }
  .dlq-row { color: #ff8a8a; }
  button { background: #2a2f45; color: #e6e6e6; border: 1px solid #3b4160; border-radius: 6px; padding: 8px 14px;
           cursor: pointer; margin-right: 10px; font-size: 13px; }
  button:hover { background: #3b4160; }
  button.danger { border-color: #6b2c2c; }
  button.danger:hover { background: #4a1f1f; }
  #sparkline { width: 100%; height: 60px; }
  .toast { font-size: 12px; color: #7ee787; margin-top: 8px; min-height: 16px; }
</style>
</head>
<body>
  <h1>Kafka Order Pipeline — Live Dashboard</h1>
  <div class="sub">Running average, live order feed, and Dead Letter Queue, updated every second.</div>

  <div class="grid">
    <div class="card">
      <div class="label">Running Average Price</div>
      <div class="stat" id="avg">-</div>
      <div class="label" style="margin-top:4px">Orders Processed: <span id="count">0</span></div>
      <canvas id="sparkline"></canvas>
    </div>

    <div class="card">
      <div class="label">Demo Controls</div>
      <p style="font-size:13px;color:#9aa0a6">Force a specific failure path on the running consumer.</p>
      <button onclick="force('transient')">Force Transient Failure</button>
      <button class="danger" onclick="force('permanent')">Force Permanent Failure (→ DLQ)</button>
      <div class="toast" id="toast"></div>
    </div>

    <div class="card" style="grid-column: span 2">
      <div class="label">Live Order Feed</div>
      <table>
        <thead><tr><th>Order ID</th><th>Product</th><th>Price</th></tr></thead>
        <tbody id="orders-body"></tbody>
      </table>
    </div>

    <div class="card" style="grid-column: span 2">
      <div class="label">Dead Letter Queue (<span id="dlq-count">0</span>)</div>
      <table>
        <thead><tr><th>Order ID</th><th>Product</th><th>Price</th><th>Retries</th><th>Reason</th></tr></thead>
        <tbody id="dlq-body"></tbody>
      </table>
    </div>

    <div class="card">
      <div class="label">Cluster Topology — Partitions &amp; Replicas</div>
      <p style="font-size:12px;color:#9aa0a6;margin:4px 0">
        Each partition has one leader broker (bold) handling reads/writes, with followers
        replicating it. If a broker goes down, a surviving in-sync replica (ISR) takes over.
      </p>
      <table>
        <thead><tr><th>Topic</th><th>Partition</th><th>Leader</th><th>Replicas</th><th>ISR</th></tr></thead>
        <tbody id="topology-body"></tbody>
      </table>
    </div>

    <div class="card">
      <div class="label">Consumer Group: order-consumer-group</div>
      <p style="font-size:12px;color:#9aa0a6;margin:4px 0">
        State: <span id="group-state">-</span> · Total lag: <span id="group-lag">-</span>
      </p>
      <table>
        <thead><tr><th>Consumer (client id)</th><th>Host</th><th>Assigned Partitions</th></tr></thead>
        <tbody id="group-body"></tbody>
      </table>
    </div>
  </div>

<script>
async function force(kind) {
  const toast = document.getElementById('toast');
  toast.textContent = 'sending ' + kind + ' trigger...';
  const res = await fetch('/api/force/' + kind, { method: 'POST' });
  const data = await res.json();
  toast.textContent = data.ok ? ('sent order ' + data.order.orderId + ' (' + kind + ')') : 'error: ' + data.error;
}

function drawSparkline(values) {
  const canvas = document.getElementById('sparkline');
  const ctx = canvas.getContext('2d');
  canvas.width = canvas.clientWidth;
  canvas.height = canvas.clientHeight;
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  if (values.length < 2) return;
  const min = Math.min(...values), max = Math.max(...values);
  const range = (max - min) || 1;
  ctx.strokeStyle = '#7ee787';
  ctx.lineWidth = 2;
  ctx.beginPath();
  values.forEach((v, i) => {
    const x = (i / (values.length - 1)) * canvas.width;
    const y = canvas.height - ((v - min) / range) * canvas.height;
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.stroke();
}

async function refresh() {
  const res = await fetch('/api/state');
  const data = await res.json();

  document.getElementById('avg').textContent = '$' + data.orders.avg.toFixed(2);
  document.getElementById('count').textContent = data.orders.count;
  document.getElementById('dlq-count').textContent = data.dlq.count;

  document.getElementById('orders-body').innerHTML = data.orders.recent.map(o =>
    `<tr><td>${o.orderId}</td><td>${o.product}</td><td>$${o.price.toFixed(2)}</td></tr>`
  ).join('');

  document.getElementById('dlq-body').innerHTML = data.dlq.recent.map(d =>
    `<tr class="dlq-row"><td>${d.orderId}</td><td>${d.product}</td><td>$${d.price.toFixed(2)}</td><td>${d.retries}</td><td>${d.errorReason}</td></tr>`
  ).join('');

  drawSparkline(data.orders.history);

  const rows = [];
  for (const [topic, partitions] of Object.entries(data.cluster.topics)) {
    partitions.forEach(p => {
      rows.push(`<tr><td>${topic}</td><td>${p.partition}</td><td><b>broker-${p.leader}</b></td>` +
        `<td>${p.replicas.map(r => 'broker-' + r).join(', ')}</td><td>${p.isr.map(r => 'broker-' + r).join(', ')}</td></tr>`);
    });
  }
  document.getElementById('topology-body').innerHTML = rows.join('');

  const group = data.cluster.consumer_group;
  document.getElementById('group-state').textContent = group.state;
  document.getElementById('group-lag').textContent = group.total_lag === null ? 'unknown' : group.total_lag;
  document.getElementById('group-body').innerHTML = group.members.map(m =>
    `<tr><td>${m.client_id}</td><td>${m.host}</td><td>${m.assigned.map(a => a.topic + '[' + a.partition + ']').join(', ') || '(none)'}</td></tr>`
  ).join('') || '<tr><td colspan="3" style="color:#9aa0a6">no active members — start consumer.py</td></tr>';
}

setInterval(refresh, 1000);
refresh();
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(PAGE)


def main():
    threading.Thread(target=orders_watch_loop, daemon=True).start()
    threading.Thread(target=dlq_watch_loop, daemon=True).start()
    threading.Thread(target=cluster_watch_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5000, debug=False)


if __name__ == "__main__":
    main()
