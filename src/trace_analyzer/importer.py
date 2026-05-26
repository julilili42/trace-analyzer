from pathlib import Path
from polars import DataFrame
import polars as pl

TRACE_COLUMNS = ["timestamp_ms", "stream_id", "payload_bytes", "latency_ms"]

TRACE_DTYPES = {
    "timestamp_ms": pl.Int32,
    "stream_id": pl.Categorical,
    "payload_bytes": pl.Int16,
    "latency_ms": pl.Float32,
}

TRACE_SCAN_DTYPES = {
    **TRACE_DTYPES,
    "stream_id": pl.String,
}


class Importer:
    @staticmethod
    def import_csv(input_path: str | Path) -> DataFrame:
        return pl.read_csv(
            input_path,
            columns=TRACE_COLUMNS,
            schema_overrides=TRACE_DTYPES,
        )

    @staticmethod
    def create_polars_df(
        input_path: str | Path,
        window_ms: float,
        link_speed_mbit: float,
        frame_size: int = 64,
    ) -> DataFrame:
        if frame_size <= 0:
            raise ValueError("frame_size must be greater than 0")

        if window_ms <= 0:
            raise ValueError("window_ms must be greater than 0")

        if link_speed_mbit <= 0:
            raise ValueError("link_speed_mbit must be greater than 0")

        scale = 8 / (link_speed_mbit * 1_000_000 * (window_ms / 1000)) * 100

        if float(window_ms).is_integer():
            window_bucket = (
                pl.col("timestamp_ms") // int(window_ms)
            ).cast(pl.Int64)
        else:
            window_bucket = (
                (pl.col("timestamp_ms") / window_ms)
                .floor()
                .cast(pl.Int64)
            )

        lf = (
            pl.scan_csv(
                source=input_path,
                separator=",",
                schema_overrides=TRACE_SCAN_DTYPES,
            )
            .select(TRACE_COLUMNS)
        )

        per_window = (
            lf.with_columns(window_bucket=window_bucket)
            .group_by(["stream_id", "window_bucket"])
            .agg(
                payload_per_window=pl.col("payload_bytes").sum(),
                row_count=pl.len(),
                latency_sum=pl.col("latency_ms").cast(pl.Float64).sum(),
                max_payload=pl.col("payload_bytes").max(),
                max_latency=pl.col("latency_ms").max(),
            )
            .with_columns(
                busload_per_window=pl.col("payload_per_window") * scale
            )
        )

        stats = (
            per_window
            .group_by("stream_id")
            .agg(
                max_payload=pl.col("max_payload").max(),
                payload_sum=pl.col("payload_per_window").sum(),
                row_count=pl.col("row_count").sum(),
                max_latency=pl.col("max_latency").max(),
                latency_sum=pl.col("latency_sum").sum(),
                busload_min=pl.col("busload_per_window").min(),
                busload_max=pl.col("busload_per_window").max(),
                busload_mean=pl.col("busload_per_window").mean(),
            )
            .with_columns(
                mean_payload=pl.col("payload_sum") / pl.col("row_count"),
                sum_frames=(
                    pl.col("payload_sum") / frame_size
                ).ceil().cast(pl.Int64),
                mean_latency=pl.col("latency_sum") / pl.col("row_count"),
            )
            .select(
                "stream_id",
                "max_payload",
                "mean_payload",
                "sum_frames",
                "max_latency",
                "mean_latency",
                "busload_min",
                "busload_max",
                "busload_mean",
                "row_count",
            )
            .sort("stream_id")
        )

        return stats.collect(engine="streaming")
