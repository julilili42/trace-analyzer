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
class Busload:
    min: float
    max: float
    mean: float


@dataclass
class Stats:
    payload: Payload
    latency: Latency
    busload: Busload


class Analyzer:
    def __init__(self, df: DataFrame):
        self.df = df

    def calc_stats(
        self,
        frame_size: int = 64,
        window_ms: float = 10,
        link_speed_mbit: float = 100,
    ) -> dict[str, Stats]:
        stats: dict[str, Stats] = {}
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

        busload = self._calc_busload_stats(
            window_ms=window_ms,
            link_speed_mbit=link_speed_mbit,
        )

        for stream_id in payload.index:
            stats[stream_id] = Stats(
                payload=Payload(
                    max=payload.loc[stream_id, "max"],
                    mean=payload.loc[stream_id, "mean"],
                    sum_frames=int(payload.loc[stream_id, "sum_frames"]),
                ),
                latency=Latency(
                    max=latency.loc[stream_id, "max"],
                    mean=latency.loc[stream_id, "mean"],
                ),
                busload=Busload(
                    min=busload.loc[stream_id, "min"],
                    max=busload.loc[stream_id, "max"],
                    mean=busload.loc[stream_id, "mean"],
                ),
            )

        return stats

    def _calc_busload_stats(
        self,
        window_ms: float,
        link_speed_mbit: float,
    ) -> DataFrame:
        if window_ms <= 0:
            raise ValueError("window_ms must be greater than 0")

        if link_speed_mbit <= 0:
            raise ValueError("link_speed_mbit must be greater than 0")

        df = self.df.copy()

        df["window_start_ms"] = (df["timestamp_ms"] // window_ms) * window_ms

        busload = (
            df.groupby(["stream_id", "window_start_ms"])
            .agg(
                payload_bytes_total=("payload_bytes", "sum"),
            )
        )

        window_seconds = window_ms / 1000
        max_bits_per_window = link_speed_mbit * 1_000_000 * window_seconds

        busload["payload_bits_total"] = busload["payload_bytes_total"] * 8
        busload["busload_percent"] = (
            busload["payload_bits_total"] / max_bits_per_window * 100
        )

        return busload.groupby(level="stream_id")["busload_percent"].agg(
            ["min", "max", "mean"]
        )

    def sum_frames(self, x, frame_size: int) -> int:
        if frame_size <= 0:
            raise ValueError("frame_size must be greater than 0")

        return int(np.ceil(x.sum() / frame_size))
