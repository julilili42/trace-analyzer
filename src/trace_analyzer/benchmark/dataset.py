import csv
import random
from dataclasses import dataclass
from pathlib import Path


DEFAULT_PAYLOAD_SIZES = (64, 128, 256, 512, 1024, 1280, 1500)
DEFAULT_LATENCY_MIN_MS = 0.2
DEFAULT_LATENCY_MAX_MS = 5.0


@dataclass(frozen=True)
class PreparedDataset:
    path: Path
    requested_trace_count: int | None
    row_count: int
    size_mb: float
    generated: bool
    seed: int | None
    payload_sizes: tuple[int, ...] | None
    latency_min_ms: float | None
    latency_max_ms: float | None


class TraceDatasetProvider:
    def __init__(self, generated_dir: str | Path = "input_data/"):
        self.generated_dir = Path(generated_dir)

    def prepare(
        self,
        input_path: str | Path,
        trace_count: int | None = None,
        seed: int = 42,
        payload_sizes: tuple[int, ...] = DEFAULT_PAYLOAD_SIZES,
        latency_min_ms: float = DEFAULT_LATENCY_MIN_MS,
        latency_max_ms: float = DEFAULT_LATENCY_MAX_MS,
    ) -> PreparedDataset:
        if trace_count is None:
            path = Path(input_path)
            return PreparedDataset(
                path=path,
                requested_trace_count=None,
                row_count=_count_csv_rows(path),
                size_mb=_file_size_mb(path),
                generated=False,
                seed=None,
                payload_sizes=None,
                latency_min_ms=None,
                latency_max_ms=None,
            )

        if trace_count <= 0:
            raise ValueError("trace_count must be greater than 0")

        payload_sizes = _validate_payload_sizes(payload_sizes)
        _validate_latency_bounds(latency_min_ms, latency_max_ms)

        path = self.generated_dir / _dataset_filename(
            trace_count=trace_count,
            seed=seed,
            payload_sizes=payload_sizes,
            latency_min_ms=latency_min_ms,
            latency_max_ms=latency_max_ms,
        )
        generated = False

        if not path.exists() or _count_csv_rows(path) != trace_count:
            _generate_trace_csv(
                path=path,
                rows=trace_count,
                seed=seed,
                payload_sizes=payload_sizes,
                latency_min_ms=latency_min_ms,
                latency_max_ms=latency_max_ms,
            )
            generated = True

        return PreparedDataset(
            path=path,
            requested_trace_count=trace_count,
            row_count=trace_count,
            size_mb=_file_size_mb(path),
            generated=generated,
            seed=seed,
            payload_sizes=payload_sizes,
            latency_min_ms=latency_min_ms,
            latency_max_ms=latency_max_ms,
        )


def _generate_trace_csv(
    path: Path,
    rows: int,
    seed: int,
    payload_sizes: tuple[int, ...],
    latency_min_ms: float,
    latency_max_ms: float,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)

    streams = [
        ("A", "ECU1", "ECU2"),
        ("B", "ECU3", "ECU4"),
        ("C", "ECU2", "ECU5"),
        ("D", "ECU6", "ECU1"),
    ]

    timestamp_ms = 0

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "timestamp_ms",
                "stream_id",
                "src",
                "dst",
                "payload_bytes",
                "latency_ms",
            ]
        )

        for _ in range(rows):
            stream_id, src, dst = rng.choice(streams)
            timestamp_ms += rng.randint(1, 5)
            payload_bytes = rng.choice(payload_sizes)
            latency_ms = round(rng.uniform(latency_min_ms, latency_max_ms), 3)
            writer.writerow(
                [
                    timestamp_ms,
                    stream_id,
                    src,
                    dst,
                    payload_bytes,
                    latency_ms,
                ]
            )


def _count_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8") as f:
        line_count = sum(1 for _ in f)

    return max(line_count - 1, 0)


def _file_size_mb(path: Path) -> float:
    return path.stat().st_size / 1024 / 1024


def _validate_payload_sizes(payload_sizes: tuple[int, ...]) -> tuple[int, ...]:
    if not payload_sizes:
        raise ValueError("payload_sizes must contain at least one value")

    if any(size <= 0 for size in payload_sizes):
        raise ValueError("payload_sizes must only contain positive integers")

    return tuple(payload_sizes)


def _validate_latency_bounds(latency_min_ms: float, latency_max_ms: float) -> None:
    if latency_min_ms < 0 or latency_max_ms < 0:
        raise ValueError("latency bounds must be greater than or equal to 0")

    if latency_min_ms > latency_max_ms:
        raise ValueError("latency_min_ms must be less than or equal to latency_max_ms")


def _dataset_filename(
    trace_count: int,
    seed: int,
    payload_sizes: tuple[int, ...],
    latency_min_ms: float,
    latency_max_ms: float,
) -> str:
    sizes = "-".join(str(size) for size in payload_sizes)
    latency_min = _format_number_for_path(latency_min_ms)
    latency_max = _format_number_for_path(latency_max_ms)
    return (
        f"traces_{trace_count}_seed_{seed}_sizes_{sizes}_"
        f"lat_{latency_min}-{latency_max}.csv"
    )


def _format_number_for_path(value: float) -> str:
    return f"{value:g}".replace("-", "m").replace(".", "p")
