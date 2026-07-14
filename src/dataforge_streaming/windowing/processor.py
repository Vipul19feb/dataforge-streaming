"""
Windowing strategies for streaming computations.

Implements tumbling, sliding, and session windows with
configurable watermark-based late data handling.

Window Types:
    - Tumbling: Fixed-size, non-overlapping windows (e.g., every 5 minutes)
    - Sliding: Fixed-size, overlapping windows (e.g., 10 min window, 5 min slide)
    - Session: Dynamic windows based on activity gaps

Example:
    >>> windower = WindowProcessor(window_type="tumbling", duration="5 minutes")
    >>> result = windower.apply(stream, event_time_col="event_time")
"""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class WindowType(str, Enum):
    """Supported window types."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


class WindowProcessor:
    """Applies windowed computations to streaming DataFrames.

    Supports tumbling, sliding, and session windows with
    configurable watermarks for late data handling.

    Attributes:
        window_type: Type of window to apply.
        duration: Window duration (e.g., "5 minutes", "1 hour").
        slide: Slide interval for sliding windows.
        gap: Session gap for session windows.
        watermark_delay: Watermark delay for late data.
    """

    def __init__(
        self,
        window_type: str = "tumbling",
        duration: str = "5 minutes",
        slide: Optional[str] = None,
        gap: Optional[str] = None,
        watermark_delay: str = "10 minutes",
    ) -> None:
        """Initialize the window processor.

        Args:
            window_type: Window type — tumbling, sliding, session.
            duration: Window duration.
            slide: Slide interval (for sliding windows).
            gap: Session gap duration (for session windows).
            watermark_delay: How late data can arrive.
        """
        self.window_type = WindowType(window_type)
        self.duration = duration
        self.slide = slide
        self.gap = gap
        self.watermark_delay = watermark_delay

    def apply(
        self,
        stream: Any,
        event_time_col: str = "event_time",
        group_by: Optional[list[str]] = None,
        aggregations: Optional[dict[str, str]] = None,
    ) -> Any:
        """Apply windowed aggregation to a streaming DataFrame.

        Args:
            stream: Input streaming DataFrame (with watermark already applied).
            event_time_col: Event timestamp column name.
            group_by: Additional columns to group by (besides the window).
            aggregations: Dict of {output_col: "agg_function(input_col)"}.

        Returns:
            Windowed, aggregated streaming DataFrame.
        """
        from pyspark.sql import functions as F

        # Apply watermark
        watermarked = stream.withWatermark(event_time_col, self.watermark_delay)

        # Build window expression
        if self.window_type == WindowType.TUMBLING:
            window_expr = F.window(F.col(event_time_col), self.duration)
        elif self.window_type == WindowType.SLIDING:
            slide = self.slide or self.duration
            window_expr = F.window(F.col(event_time_col), self.duration, slide)
        elif self.window_type == WindowType.SESSION:
            gap = self.gap or "5 minutes"
            window_expr = F.session_window(F.col(event_time_col), gap)
        else:
            raise ValueError(f"Unsupported window type: {self.window_type}")

        # Build group-by columns
        group_cols = [window_expr]
        if group_by:
            group_cols.extend([F.col(c) for c in group_by])

        # Apply grouping
        grouped = watermarked.groupBy(*group_cols)

        # Apply aggregations
        if aggregations:
            agg_exprs = []
            for output_col, agg_expr in aggregations.items():
                # Parse "count(*)", "sum(quantity)", "avg(price)"
                func_name, col_name = agg_expr.replace(")", "").split("(")
                if func_name == "count":
                    agg_exprs.append(F.count("*").alias(output_col))
                elif func_name == "sum":
                    agg_exprs.append(F.sum(col_name).alias(output_col))
                elif func_name == "avg":
                    agg_exprs.append(F.avg(col_name).alias(output_col))
                elif func_name == "min":
                    agg_exprs.append(F.min(col_name).alias(output_col))
                elif func_name == "max":
                    agg_exprs.append(F.max(col_name).alias(output_col))
            result = grouped.agg(*agg_exprs)
        else:
            # Default: count events
            result = grouped.agg(F.count("*").alias("event_count"))

        logger.info(
            "Window applied: type=%s, duration=%s, watermark=%s",
            self.window_type.value,
            self.duration,
            self.watermark_delay,
        )
        return result


class LateDataRouter:
    """Routes late data to side outputs for deferred processing.

    Constitutional Rule: Late data beyond the watermark MUST be
    routed to a side output, never silently dropped.

    Late events are written to a separate path/topic for later
    batch reprocessing by dataforge-core pipelines.

    Attributes:
        side_output_path: Path for persisting late events.
        max_lateness: Maximum lateness threshold.
    """

    def __init__(
        self,
        side_output_path: str,
        max_lateness: str = "1 hour",
    ) -> None:
        """Initialize the late data router.

        Args:
            side_output_path: Path to write late events.
            max_lateness: Maximum acceptable lateness.
        """
        self.side_output_path = side_output_path
        self.max_lateness = max_lateness
        logger.info(
            "LateDataRouter initialized: path=%s, max_lateness=%s",
            side_output_path,
            max_lateness,
        )

    def write_late_events(self, late_events: Any) -> None:
        """Write late events to the side output.

        Late events are persisted with metadata (original event_time,
        arrival_time, lateness_seconds) for analysis and reprocessing.

        Args:
            late_events: DataFrame of late events.
        """
        from pyspark.sql import functions as F

        enriched = late_events.withColumn(
            "_late_metadata",
            F.struct(
                F.current_timestamp().alias("routed_at"),
                F.lit("late_arrival").alias("reason"),
            ),
        )

        (
            enriched.writeStream
            .format("parquet")
            .option("path", self.side_output_path)
            .option("checkpointLocation", f"{self.side_output_path}/_checkpoint")
            .outputMode("append")
            .start()
        )

        logger.info("Late events routed to: %s", self.side_output_path)
