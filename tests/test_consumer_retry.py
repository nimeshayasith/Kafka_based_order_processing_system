"""Unit tests for the consumer's retry/backoff/DLQ decision logic.

These exercise handle_with_retry() and process_order() directly -- no
Kafka broker or Schema Registry required -- using the same deterministic
demo trigger products (FORCE_TRANSIENT_FAIL / FORCE_PERMANENT_FAIL) the
live demo uses, so the tests double as documentation of that behavior.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "consumer"))
import consumer  # noqa: E402


def make_order(product, order_id="1", price=100.0):
    return {"orderId": order_id, "product": product, "price": price}


def test_transient_failure_recovers_within_retry_budget(monkeypatch):
    monkeypatch.setattr(consumer.time, "sleep", lambda _: None)

    order = make_order("FORCE_TRANSIENT_FAIL")
    success, error_reason, attempts_used = consumer.handle_with_retry(order)

    assert success is True
    assert error_reason is None
    # fails on attempts 1 and 2, succeeds on attempt 3
    assert attempts_used == 3


def test_permanent_failure_exhausts_immediately_and_reports_reason():
    order = make_order("FORCE_PERMANENT_FAIL")
    success, error_reason, attempts_used = consumer.handle_with_retry(order)

    assert success is False
    assert "forced permanent failure" in error_reason
    assert attempts_used == 1


def test_ordinary_order_succeeds_when_no_simulated_failure(monkeypatch):
    monkeypatch.setattr(consumer.random, "random", lambda: 1.0)  # never below failure rate

    order = make_order("Item1")
    success, error_reason, attempts_used = consumer.handle_with_retry(order)

    assert success is True
    assert error_reason is None
    assert attempts_used == 1


def test_transient_failure_routes_to_dlq_once_retry_budget_is_smaller_than_needed(monkeypatch):
    monkeypatch.setattr(consumer, "MAX_RETRIES", 1)
    monkeypatch.setattr(consumer.time, "sleep", lambda _: None)

    order = make_order("FORCE_TRANSIENT_FAIL")
    success, error_reason, attempts_used = consumer.handle_with_retry(order)

    assert success is False
    assert "forced transient failure" in error_reason
    assert attempts_used == 1


def test_running_average_updates_correctly():
    avg = consumer.RunningAverage()
    assert avg.update(10.0) == 10.0
    assert avg.update(20.0) == 15.0
    assert avg.update(30.0) == 20.0
    assert avg.count == 3
