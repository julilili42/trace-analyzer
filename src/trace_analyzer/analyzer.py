from dataclasses import dataclass
from pandas import DataFrame
import numpy as np


@dataclass
class Payload:
    mean: float
    max: float
    sum_frames: int


@dataclass
class Latency:
    mean: float
    max: float


@dataclass
class Stats:
    payload: Payload
    latency: Latency


class Analyzer:
    def __init__(self, df: DataFrame):
        self.df = df
        self.stats: dict[str, Stats] = {}

    def calc_stats(self, frame_size: int = 64) -> dict[str, Stats]:
        stream_group = self.df.groupby("stream_id")

        payload = stream_group["payload_bytes"].agg(
            max="max",
            mean="mean",
            sum_frames=lambda x: self.sum_frames(x, frame_size),
        )

        latency = stream_group["latency_ms"].agg(
            max="max",
            mean="mean",
        )

        for stream_id in payload.index:
            self.stats[stream_id] = Stats(
                payload=Payload(
                    max=payload.loc[stream_id, "max"],
                    mean=payload.loc[stream_id, "mean"],
                    sum_frames=int(payload.loc[stream_id, "sum_frames"]),
                ),
                latency=Latency(
                    max=latency.loc[stream_id, "max"],
                    mean=latency.loc[stream_id, "mean"],
                ),
            )

        return self.stats

    def detect_anomalies(self) -> DataFrame:
        df = self.df.copy()

        df["high_latency"] = df.groupby("stream_id")["latency_ms"].transform(
            lambda x: x > x.quantile(0.95)
        )

        df["large_payload"] = df.groupby("stream_id")["payload_bytes"].transform(
            lambda x: x > x.quantile(0.95)
        )

        df["is_anomaly"] = df["high_latency"] | df["large_payload"]

        return df

    def bus_load(self, window_ms: float = 10, link_speed_mbit: float = 100) -> DataFrame:
        df = self.df.copy()

        df["window_start_ms"] = (df["timestamp_ms"] // window_ms) * window_ms

        busload = (
            df.groupby("window_start_ms")
            .agg(
                frames=("payload_bytes", "count"),
                payload_bytes_total=("payload_bytes", "sum"),
            )
            .reset_index()
        )

        window_seconds = window_ms / 1000
        max_bits_per_window = link_speed_mbit * 1_000_000 * window_seconds

        busload["payload_bits_total"] = busload["payload_bytes_total"] * 8
        busload["busload_percent"] = (
            busload["payload_bits_total"] / max_bits_per_window * 100
        )

        return busload

    def sum_frames(self, x, frame_size: int) -> int:
        if frame_size <= 0:
            raise ValueError("frame_size must be greater than 0")

        return int(np.ceil(x.sum() / frame_size))
