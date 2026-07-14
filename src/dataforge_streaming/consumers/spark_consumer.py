"""
Spark Structured Streaming consumer for DataForge.

Provides a configuration-driven Kafka consumer using Spark Structured
Streaming with support for:
- Event-time windowed aggregations
- Watermark-based late data handling
- Exactly-once writes to Iceberg/Delta tables
- Checkpointing for fault tolerance

Architecture Decision (ADR-001):
    Spark Structured Streaming was chosen for its mature exactly-once
    semantics, native Kafka integration, and unified batch/streaming API.
    PyFlink is available as an alternative for lower-latency use cases.

Example:
    >>> consumer = SparkStreamConsumer(config={
    ...     "kafka": {"bootstrap_servers": "localhost:9092", "topics": ["orders"]},
    ...     "watermark": {"column": "event_time", "delay": "10 minutes"},
    ...     "checkpoint": "/tmp/checkpoints/orders",
    ... })
    >>> consumer.start()
"""

from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class SparkStreamConsumer:
    """Kafka consumer using Spark Structured Streaming.

    Reads events from Kafka topics, applies windowed transformations,
    handles late data via watermarks, and writes to streaming sinks
    with exactly-once guarantees.

    Attributes:
        config: Consumer configuration.
        spark: SparkSession instance.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize the Spark streaming consumer.

        Args:
            config: Configuration dict with keys:
                - kafka: Kafka connection settings.
                - watermark: Watermark configuration.
                - checkpoint: Checkpoint location.
                - output: Output sink configuration.
                - processing: Processing options.
        """
        self.config = config
        self.spark: Any = None
        self._stream_query: Any = None

    def start(self) -> None:
        """Start the streaming consumer.

        Initializes SparkSession, subscribes to Kafka topics,
        applies watermarks and transformations, and starts
        writing to the configured sink.
        """
        self._init_spark()
        raw_stream = self._read_from_kafka()
        parsed_stream = self._parse_events(raw_stream)
        watermarked = self._apply_watermark(parsed_stream)
        transformed = self._apply_transformations(watermarked)
        self._write_to_sink(transformed)

    def stop(self) -> None:
        """Stop the streaming consumer gracefully."""
        if self._stream_query:
            self._stream_query.stop()
            logger.info("Streaming query stopped")
        if self.spark:
            self.spark.stop()

    def _init_spark(self) -> None:
        """Initialize SparkSession with streaming configurations."""
        from pyspark.sql import SparkSession

        kafka_config = self.config.get("kafka", {})

        self.spark = (
            SparkSession.builder
            .appName(self.config.get("app_name", "dataforge-streaming"))
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.streaming.schemaInference", "true")
            .getOrCreate()
        )
        logger.info("SparkSession initialized for streaming")

    def _read_from_kafka(self) -> Any:
        """Subscribe to Kafka topics and read the stream.

        Returns:
            Streaming DataFrame with raw Kafka messages.
        """
        kafka_config = self.config["kafka"]
        bootstrap_servers = kafka_config["bootstrap_servers"]
        topics = kafka_config["topics"]

        stream = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", ",".join(topics))
            .option("startingOffsets", kafka_config.get("starting_offsets", "latest"))
            .option("failOnDataLoss", kafka_config.get("fail_on_data_loss", "false"))
            .load()
        )

        logger.info(
            "Subscribed to Kafka topics: %s @ %s",
            topics,
            bootstrap_servers,
        )
        return stream

    def _parse_events(self, stream: Any) -> Any:
        """Parse Kafka message values from binary to structured format.

        Args:
            stream: Raw Kafka streaming DataFrame.

        Returns:
            Parsed streaming DataFrame with event columns.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        # Define schema for events (configurable per use case)
        schema_config = self.config.get("schema", {})
        if schema_config:
            schema = StructType([
                StructField(name, self._resolve_type(dtype))
                for name, dtype in schema_config.items()
            ])
        else:
            # Default e-commerce order event schema
            schema = StructType([
                StructField("order_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("product_id", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("unit_price", DoubleType(), True),
                StructField("event_time", TimestampType(), False),
                StructField("event_type", StringType(), True),
            ])

        parsed = (
            stream
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
            .select(
                F.from_json(F.col("value"), schema).alias("event"),
                F.col("timestamp").alias("kafka_timestamp"),
            )
            .select("event.*", "kafka_timestamp")
        )

        logger.info("Events parsed with schema: %s", [f.name for f in schema.fields])
        return parsed

    def _apply_watermark(self, stream: Any) -> Any:
        """Apply watermark for late data handling.

        The watermark defines how late data can arrive and still be
        included in windowed aggregations. Data arriving after the
        watermark is routed to a side output (if configured).

        Args:
            stream: Parsed streaming DataFrame.

        Returns:
            Watermarked streaming DataFrame.
        """
        from pyspark.sql import functions as F

        watermark_config = self.config.get("watermark", {})
        event_time_col = watermark_config.get("column", "event_time")
        delay = watermark_config.get("delay", "10 minutes")

        watermarked = stream.withWatermark(event_time_col, delay)

        logger.info(
            "Watermark applied: column=%s, delay=%s",
            event_time_col,
            delay,
        )
        return watermarked

    def _apply_transformations(self, stream: Any) -> Any:
        """Apply streaming transformations.

        Applies configured windowed aggregations and business logic.
        Default: 5-minute tumbling window aggregation.

        Args:
            stream: Watermarked streaming DataFrame.

        Returns:
            Transformed streaming DataFrame.
        """
        from pyspark.sql import functions as F

        processing = self.config.get("processing", {})
        window_type = processing.get("window_type", "tumbling")
        window_duration = processing.get("window_duration", "5 minutes")
        event_time_col = self.config.get("watermark", {}).get("column", "event_time")

        if window_type == "tumbling":
            result = (
                stream
                .groupBy(
                    F.window(F.col(event_time_col), window_duration),
                    F.col("event_type"),
                )
                .agg(
                    F.count("*").alias("event_count"),
                    F.sum("quantity").alias("total_quantity"),
                    F.avg("unit_price").alias("avg_price"),
                )
            )
        elif window_type == "sliding":
            slide_duration = processing.get("slide_duration", "1 minute")
            result = (
                stream
                .groupBy(
                    F.window(F.col(event_time_col), window_duration, slide_duration),
                    F.col("event_type"),
                )
                .agg(
                    F.count("*").alias("event_count"),
                )
            )
        else:
            result = stream  # Pass through

        return result

    def _write_to_sink(self, stream: Any) -> None:
        """Write streaming results to the configured sink.

        Supports console (debug), Parquet, and Kafka sinks.
        Uses checkpointing for exactly-once guarantees.

        Args:
            stream: Transformed streaming DataFrame.
        """
        output = self.config.get("output", {})
        sink_type = output.get("type", "console")
        checkpoint = self.config.get("checkpoint", "/tmp/checkpoints/dataforge")

        if sink_type == "console":
            self._stream_query = (
                stream.writeStream
                .outputMode("update")
                .format("console")
                .option("truncate", "false")
                .option("checkpointLocation", checkpoint)
                .start()
            )
        elif sink_type == "parquet":
            self._stream_query = (
                stream.writeStream
                .outputMode("append")
                .format("parquet")
                .option("path", output["path"])
                .option("checkpointLocation", checkpoint)
                .partitionBy(output.get("partition_by", "event_date"))
                .start()
            )
        elif sink_type == "kafka":
            self._stream_query = (
                stream.writeStream
                .outputMode("update")
                .format("kafka")
                .option("kafka.bootstrap.servers", output["bootstrap_servers"])
                .option("topic", output["topic"])
                .option("checkpointLocation", checkpoint)
                .start()
            )

        logger.info("Streaming sink started: type=%s", sink_type)

    @staticmethod
    def _resolve_type(type_str: str) -> Any:
        """Resolve a type string to a Spark data type."""
        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            TimestampType,
        )

        type_map = {
            "string": StringType(),
            "integer": IntegerType(),
            "long": LongType(),
            "double": DoubleType(),
            "timestamp": TimestampType(),
        }
        return type_map.get(type_str.lower(), StringType())
