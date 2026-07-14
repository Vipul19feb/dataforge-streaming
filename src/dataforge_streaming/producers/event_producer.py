"""
Kafka event producer for DataForge.

Simulates realistic event streams for development and testing.
Supports configurable event rates, schemas, and data patterns
including late data injection for testing watermark behavior.

Example:
    >>> producer = EventProducer(config={
    ...     "kafka": {"bootstrap_servers": "localhost:9092", "topic": "orders"},
    ...     "rate": {"events_per_second": 100},
    ...     "late_data": {"enabled": True, "probability": 0.05, "max_delay_minutes": 30},
    ... })
    >>> producer.start(duration_seconds=60)
"""

from __future__ import annotations

import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)


class EventProducer:
    """Simulates realistic Kafka event streams.

    Generates configurable event streams with support for:
    - Configurable throughput (events per second)
    - Realistic data patterns (IDs, timestamps, statuses)
    - Controllable late data injection
    - Multiple event types

    Attributes:
        config: Producer configuration.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize the event producer.

        Args:
            config: Configuration with keys:
                - kafka: Kafka connection settings (bootstrap_servers, topic).
                - rate: Throughput settings (events_per_second).
                - late_data: Late data simulation settings.
                - schema: Event schema type.
        """
        self.config = config
        self._producer: Any = None
        self._event_counter = 0

    def start(self, duration_seconds: int = 60) -> None:
        """Start producing events.

        Args:
            duration_seconds: How long to produce events.
        """
        self._init_producer()
        rate = self.config.get("rate", {}).get("events_per_second", 10)
        late_config = self.config.get("late_data", {})
        topic = self.config["kafka"]["topic"]

        logger.info(
            "Starting event production: topic=%s, rate=%d/s, duration=%ds",
            topic, rate, duration_seconds,
        )

        start_time = time.monotonic()
        while time.monotonic() - start_time < duration_seconds:
            batch_start = time.monotonic()

            for _ in range(rate):
                event = self._generate_event(late_config)
                self._send_event(topic, event)
                self._event_counter += 1

            # Throttle to maintain target rate
            elapsed = time.monotonic() - batch_start
            sleep_time = max(0, 1.0 - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

        self._flush()
        logger.info("Production complete: %d events sent", self._event_counter)

    def generate_batch(self, count: int) -> list[dict[str, Any]]:
        """Generate a batch of events without sending to Kafka.

        Useful for testing and local development.

        Args:
            count: Number of events to generate.

        Returns:
            List of event dictionaries.
        """
        late_config = self.config.get("late_data", {})
        events = []
        for _ in range(count):
            event = self._generate_event(late_config)
            events.append(event)
            self._event_counter += 1
        return events

    def _generate_event(self, late_config: dict[str, Any]) -> dict[str, Any]:
        """Generate a single event with optional late data simulation.

        Args:
            late_config: Late data configuration.

        Returns:
            Event dictionary.
        """
        now = datetime.now(timezone.utc)

        # Simulate late data
        if late_config.get("enabled", False):
            probability = late_config.get("probability", 0.05)
            if random.random() < probability:
                max_delay = late_config.get("max_delay_minutes", 30)
                delay = random.randint(1, max_delay)
                event_time = now - timedelta(minutes=delay)
                is_late = True
            else:
                event_time = now
                is_late = False
        else:
            event_time = now
            is_late = False

        # Generate event data
        event = {
            "order_id": f"ORD-{self._event_counter:08d}",
            "customer_id": f"CUST-{random.randint(1, 1000):05d}",
            "product_id": f"PROD-{random.randint(1, 500):04d}",
            "quantity": random.randint(1, 10),
            "unit_price": round(random.uniform(5.0, 500.0), 2),
            "event_time": event_time.isoformat(),
            "event_type": random.choice([
                "ORDER_CREATED",
                "ORDER_UPDATED",
                "ORDER_SHIPPED",
                "ORDER_DELIVERED",
                "ORDER_CANCELLED",
            ]),
            "status": random.choice([
                "PENDING", "CONFIRMED", "SHIPPED", "DELIVERED",
            ]),
            "payment_method": random.choice([
                "CREDIT_CARD", "PAYPAL", "APPLE_PAY", "DEBIT_CARD",
            ]),
            "_metadata": {
                "producer": "dataforge-event-producer",
                "is_late": is_late,
                "produced_at": now.isoformat(),
            },
        }

        return event

    def _init_producer(self) -> None:
        """Initialize the Kafka producer."""
        try:
            from confluent_kafka import Producer

            kafka_config = self.config["kafka"]
            self._producer = Producer({
                "bootstrap.servers": kafka_config["bootstrap_servers"],
                "client.id": "dataforge-producer",
                "acks": "all",
                "enable.idempotence": True,
            })
            logger.info("Kafka producer initialized")
        except ImportError:
            logger.warning("confluent-kafka not installed, using mock producer")
            self._producer = None

    def _send_event(self, topic: str, event: dict[str, Any]) -> None:
        """Send an event to Kafka.

        Args:
            topic: Kafka topic name.
            event: Event dictionary to send.
        """
        if self._producer:
            key = event.get("order_id", "").encode("utf-8")
            value = json.dumps(event).encode("utf-8")
            self._producer.produce(topic, key=key, value=value)
        else:
            # Mock mode — just log
            if self._event_counter % 100 == 0:
                logger.debug("Mock produced event #%d: %s", self._event_counter, event["order_id"])

    def _flush(self) -> None:
        """Flush pending messages."""
        if self._producer:
            self._producer.flush(timeout=10)


class CDCProducer:
    """Simulates Change Data Capture (CDC) events.

    Generates Debezium-style CDC events for testing streaming
    pipelines that consume database change events.

    CDC Events contain:
    - before: Previous state (null for INSERT)
    - after: Current state (null for DELETE)
    - op: Operation type (c=create, u=update, d=delete, r=read/snapshot)
    - ts_ms: Timestamp in milliseconds
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self._counter = 0

    def generate_cdc_event(
        self,
        operation: str = "c",
        table: str = "customers",
    ) -> dict[str, Any]:
        """Generate a Debezium-style CDC event.

        Args:
            operation: CDC operation — 'c' (create), 'u' (update), 'd' (delete).
            table: Source table name.

        Returns:
            CDC event dictionary.
        """
        self._counter += 1
        now = datetime.now(timezone.utc)
        record_id = f"REC-{self._counter:06d}"

        after_state = {
            "id": record_id,
            "name": f"Customer {self._counter}",
            "email": f"customer{self._counter}@example.com",
            "updated_at": now.isoformat(),
        }

        before_state = None
        if operation == "u":
            before_state = {
                **after_state,
                "email": f"old_email{self._counter}@example.com",
                "updated_at": (now - timedelta(hours=1)).isoformat(),
            }
        elif operation == "d":
            before_state = after_state
            after_state = None

        return {
            "schema": {"type": "struct", "name": f"{table}.Envelope"},
            "payload": {
                "before": before_state,
                "after": after_state,
                "source": {
                    "version": "2.4.0",
                    "connector": "postgresql",
                    "name": "dataforge",
                    "ts_ms": int(now.timestamp() * 1000),
                    "db": "dataforge",
                    "schema": "public",
                    "table": table,
                },
                "op": operation,
                "ts_ms": int(now.timestamp() * 1000),
            },
        }
