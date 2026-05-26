import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from trace_analyzer.benchmark.dataset import (
    DEFAULT_LATENCY_MAX_MS,
    DEFAULT_LATENCY_MIN_MS,
    DEFAULT_PAYLOAD_SIZES,
)


DEFAULT_PROFILE_DIR = Path("benchmark_profiles")


@dataclass(frozen=True)
class BenchmarkProfile:
    name: str
    description: str = ""
    input_path: str = "input_data/data.csv"
    output_root: str = "output_data/benchmark_runs"
    results_file: str = "benchmark_results/results.jsonl"
    runs: int = 5
    warmups: int = 1
    scenario: str | None = None
    trace_count: int | None = None
    dataset_seed: int = 42
    generated_input_dir: str = "input_data/"
    payload_sizes: tuple[int, ...] = DEFAULT_PAYLOAD_SIZES
    latency_min_ms: float = DEFAULT_LATENCY_MIN_MS
    latency_max_ms: float = DEFAULT_LATENCY_MAX_MS
    frame_size: int = 64
    window_ms: float = 10.0
    link_speed_mbit: float = 100.0

    def scenario_name(self) -> str:
        return self.scenario or self.name

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["payload_sizes"] = list(self.payload_sizes)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BenchmarkProfile":
        payload_sizes = data.get("payload_sizes", DEFAULT_PAYLOAD_SIZES)
        return cls(
            name=data["name"],
            description=data.get("description", ""),
            input_path=data.get("input_path", "input_data/data.csv"),
            output_root=data.get("output_root", "output_data/benchmark_runs"),
            results_file=data.get(
                "results_file", "benchmark_results/results.jsonl"),
            runs=int(data.get("runs", 5)),
            warmups=int(data.get("warmups", 1)),
            scenario=data.get("scenario"),
            trace_count=data.get("trace_count"),
            dataset_seed=int(data.get("dataset_seed", 42)),
            generated_input_dir=data.get("generated_input_dir", "input_data/"),
            payload_sizes=tuple(int(size) for size in payload_sizes),
            latency_min_ms=float(
                data.get("latency_min_ms", DEFAULT_LATENCY_MIN_MS)),
            latency_max_ms=float(
                data.get("latency_max_ms", DEFAULT_LATENCY_MAX_MS)),
            frame_size=int(data.get("frame_size", 64)),
            window_ms=float(data.get("window_ms", 10.0)),
            link_speed_mbit=float(data.get("link_speed_mbit", 100.0)),
        )


class BenchmarkProfileStore:
    def __init__(self, profile_dir: str | Path = DEFAULT_PROFILE_DIR):
        self.profile_dir = Path(profile_dir)

    def save(self, profile: BenchmarkProfile, overwrite: bool = False) -> Path:
        path = self.path_for(profile.name)
        if path.exists() and not overwrite:
            raise FileExistsError(f"profile already exists: {path}")

        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(profile.to_dict(), f, indent=2, sort_keys=True)
            f.write("\n")

        return path

    def load(self, name: str) -> BenchmarkProfile:
        path = self.path_for(name)
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        return BenchmarkProfile.from_dict(data)

    def list_profiles(self) -> list[str]:
        if not self.profile_dir.exists():
            return []

        return sorted(path.stem for path in self.profile_dir.glob("*.json"))

    def path_for(self, name: str) -> Path:
        _validate_profile_name(name)
        return self.profile_dir / f"{name}.json"


def _validate_profile_name(name: str) -> None:
    if not name:
        raise ValueError("profile name must not be empty")

    if Path(name).name != name:
        raise ValueError("profile name must not contain path separators")
