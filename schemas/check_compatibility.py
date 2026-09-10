"""Demo: ask the Schema Registry whether candidate schemas are compatible
with the 'orders-value' subject, and show what happens when you try to
register a genuinely breaking change.

Usage:
  python schemas/check_compatibility.py
"""
import functools
import os

print = functools.partial(print, flush=True)

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
SUBJECT = "orders-value"
SCHEMA_DIR = os.path.dirname(__file__)

# A deliberately breaking change: price changes type from float to string,
# with no default -- this violates BACKWARD compatibility (existing
# consumers reading old data with this new schema would fail).
BREAKING_SCHEMA = """
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price", "type": "string"}
  ]
}
"""


def load_schema(filename):
    with open(os.path.join(SCHEMA_DIR, filename)) as f:
        return f.read()


def main():
    client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    try:
        mode = client.get_compatibility(SUBJECT)
    except SchemaRegistryError:
        # No subject-level override -- falls back to the registry's global default.
        mode = f"{client.get_compatibility()} (global default, no subject-level override)"
    print(f"[compat-check] subject '{SUBJECT}' compatibility mode: {mode}")

    v2_str = load_schema("order_v2.avsc")
    v2_compatible = client.test_compatibility(SUBJECT, Schema(v2_str, "AVRO"))
    print(f"[compat-check] order_v2.avsc (adds 'currency' with default) compatible? -> {v2_compatible}")

    breaking_compatible = client.test_compatibility(SUBJECT, Schema(BREAKING_SCHEMA, "AVRO"))
    print(f"[compat-check] breaking schema (price float -> string) compatible? -> {breaking_compatible}")

    print("[compat-check] attempting to register the breaking schema anyway...")
    try:
        client.register_schema(SUBJECT, Schema(BREAKING_SCHEMA, "AVRO"))
        print("[compat-check] unexpectedly succeeded (registry compatibility mode may be set to NONE)")
    except SchemaRegistryError as e:
        print(f"[compat-check] registration REJECTED by Schema Registry, as expected: {e}")


if __name__ == "__main__":
    main()
