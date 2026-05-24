import argparse
import gc
import hashlib
import subprocess
import tracemalloc
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from trace_analyzer.benchmark.dataset import (
    DEFAULT_LATENCY_MAX_MS,
    DEFAULT_LATENCY_MIN_MS,
    DEFAULT_PAYLOAD_SIZES,
    TraceDatasetProvider,
)
from trace_analyzer.benchmark.models import BenchmarkRecord
from trace_analyzer.benchmark.recorder import JsonlRecorder
from trace_analyzer.pipeline import Pipeline, PipelineConfig, PipelineResult


class BenchmarkRunner:
    def __init__(
        self,
        input_path: str | Path = "input_data/data.csv",
        output_root: str | Path = "output_data/benchmark_runs",
        results_file: str | Path = "benchmark_results/results.jsonl",
        runs: int = 5,
        warmups: int = 1,
        scenario: str = "baseline",
        pipeline_config: PipelineConfig | None = None,
        verbose: bool = True,
        trace_count: int | None = None,
        dataset_seed: int = 42,
        generated_input_dir: str | Path = "input_data/",
        payload_sizes: tuple[int, ...] = DEFAULT_PAYLOAD_SIZES,
        latency_min_ms: float = DEFAULT_LATENCY_MIN_MS,
        latency_max_ms: float = DEFAULT_LATENCY_MAX_MS,
        dataset_provider: TraceDatasetProvider | None = None,
    ):
        if runs <= 0:
            raise ValueError("runs must be greater than 0")

        if warmups < 0:
            raise ValueError("warmups must be greater than or equal to 0")

        self.output_root = Path(output_root)
        self.results_file = Path(results_file)
        self.runs = runs
        self.warmups = warmups
        self.scenario = scenario
        self.pipeline = Pipeline(pipeline_config)
        self.recorder = JsonlRecorder(self.results_file)
        self.verbose = verbose
        self.dataset_provider = dataset_provider or TraceDatasetProvider(
            generated_dir=generated_input_dir
        )
        self.dataset = self.dataset_provider.prepare(
            input_path=input_path,
            trace_count=trace_count,
            seed=dataset_seed,
            payload_sizes=payload_sizes,
            latency_min_ms=latency_min_ms,
            latency_max_ms=latency_max_ms,
        )
        self.input_path = self.dataset.path

    def run(self) -> list[BenchmarkRecord]:
        if self.verbose:
            source = "generated" if self.dataset.generated else "existing"
            print(
                f"dataset={self.dataset.path} "
                f"rows={self.dataset.row_count} "
                f"source={source}"
            )

        # warmup runs
        # not included in benchmark
        for iteration in range(1, self.warmups + 1):
            self._execute(iteration=iteration, is_warmup=True)

        # actual runs
        records: list[BenchmarkRecord] = []
        for iteration in range(1, self.runs + 1):
            record = self._execute(iteration=iteration, is_warmup=False)
            self.recorder.append(record)
            records.append(record)
            if self.verbose:
                print(
                    f"run {iteration}/{self.runs}: "
                    f"total={record.total_wall_time_s:.4f}s, "
                    f"rows/s={record.rows_per_second:.0f}, "
                    f"peak_memory={record.peak_memory_mb:.2f}MB"
                )

        return records

    def start_benchmark(self, iterations: int = 1) -> list[BenchmarkRecord]:
        self.runs = iterations
        return self.run()

    def _execute(self, iteration: int, is_warmup: bool) -> BenchmarkRecord:
        run_id = self._new_run_id(prefix="warmup" if is_warmup else "run")
        output_dir = self.output_root / self.scenario / run_id

        # collect unused references to standarize performance
        gc.collect()

        # start timing process
        tracemalloc.start()
        result = self.pipeline.run(self.input_path, output_dir)
        _, peak_memory_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        return self._record_from_result(
            result=result,
            run_id=run_id,
            iteration=iteration,
            peak_memory_mb=peak_memory_bytes / 1024 / 1024,
        )

    def _record_from_result(
        self,
        result: PipelineResult,
        run_id: str,
        iteration: int,
        peak_memory_mb: float,
    ) -> BenchmarkRecord:
        dataset = self.dataset
        output_size_mb = _directory_size_mb(result.output_dir)
        total_time_s = result.timings.total_wall_time_s
        rows_per_second = result.row_count / total_time_s if total_time_s > 0 else 0.0

        return BenchmarkRecord(
            run_id=run_id,
            scenario=self.scenario,
            iteration=iteration,
            timestamp_utc=datetime.now(timezone.utc).isoformat(),
            git_commit=_git_commit(),
            input_path=str(dataset.path),
            input_size_mb=dataset.size_mb,
            requested_trace_count=dataset.requested_trace_count,
            dataset_seed=dataset.seed,
            dataset_generated=dataset.generated,
            payload_sizes=dataset.payload_sizes,
            latency_min_ms=dataset.latency_min_ms,
            latency_max_ms=dataset.latency_max_ms,
            row_count=result.row_count,
            output_dir=str(result.output_dir),
            output_size_mb=output_size_mb,
            total_wall_time_s=total_time_s,
            import_time_s=result.timings.import_time_s,
            analyze_time_s=result.timings.analyze_time_s,
            export_time_s=result.timings.export_time_s,
            rows_per_second=rows_per_second,
            peak_memory_mb=peak_memory_mb,
            result_hash=_hash_directory(result.output_dir),
            validation_status=_validate_outputs(result),
        )

    @staticmethod
    def _new_run_id(prefix: str) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        return f"{prefix}_{timestamp}_{uuid4().hex[:8]}"


def _directory_size_mb(path: Path) -> float:
    total_bytes = sum(
        file.stat().st_size for file in path.rglob("*") if file.is_file())
    return total_bytes / 1024 / 1024


def _hash_directory(path: Path) -> str:
    digest = hashlib.sha256()

    for file in sorted(item for item in path.rglob("*") if item.is_file()):
        digest.update(str(file.relative_to(path)).encode("utf-8"))
        digest.update(b"\0")
        with file.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                digest.update(chunk)

    return digest.hexdigest()


def _validate_outputs(result: PipelineResult) -> str:
    artifacts = (
        result.artifacts.stats_json,
        result.artifacts.busload_json,
        result.artifacts.busload_csv,
        result.artifacts.anomalies_json,
        result.artifacts.anomalies_csv,
    )

    if result.row_count <= 0:
        return "failed_empty_input"

    if not all(path.exists() and path.stat().st_size > 0 for path in artifacts):
        return "failed_missing_output"

    return "passed"


def _git_commit() -> str | None:
    result = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True,
        check=False,
        text=True,
    )

    if result.returncode != 0:
        return None

    return result.stdout.strip()


def _parse_payload_sizes(value: str) -> tuple[int, ...]:
    try:
        return tuple(int(item.strip()) for item in value.split(",") if item.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "payload sizes must be a comma-separated list of integers"
        ) from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run trace analyzer benchmarks.")
    parser.add_argument("input_path", nargs="?", default="input_data/data.csv")
    parser.add_argument("--output-root", default="output_data/benchmark_runs")
    parser.add_argument(
        "--results-file", default="benchmark_results/results.jsonl")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--scenario", default="baseline")
    parser.add_argument(
        "--trace-count",
        type=int,
        default=None,
        help="Generate/use a deterministic input CSV with this number of traces.",
    )
    parser.add_argument(
        "--dataset-seed",
        type=int,
        default=42,
        help="Seed used when --trace-count generates synthetic data.",
    )
    parser.add_argument(
        "--generated-input-dir",
        default="input_data/",
        help="Directory for generated benchmark input CSV files.",
    )
    parser.add_argument(
        "--payload-sizes",
        "--ethernet-frame-sizes",
        dest="payload_sizes",
        type=_parse_payload_sizes,
        default=DEFAULT_PAYLOAD_SIZES,
        help="Comma-separated allowed payload byte sizes for generated traces.",
    )
    parser.add_argument(
        "--latency-min-ms",
        type=float,
        default=DEFAULT_LATENCY_MIN_MS,
        help="Minimum generated latency in milliseconds.",
    )
    parser.add_argument(
        "--latency-max-ms",
        type=float,
        default=DEFAULT_LATENCY_MAX_MS,
        help="Maximum generated latency in milliseconds.",
    )
    parser.add_argument("--frame-size", type=int, default=64)
    parser.add_argument("--window-ms", type=float, default=10.0)
    parser.add_argument("--link-speed-mbit", type=float, default=100.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = PipelineConfig(
        frame_size=args.frame_size,
        window_ms=args.window_ms,
        link_speed_mbit=args.link_speed_mbit,
    )
    runner = BenchmarkRunner(
        input_path=args.input_path,
        output_root=args.output_root,
        results_file=args.results_file,
        runs=args.runs,
        warmups=args.warmups,
        scenario=args.scenario,
        pipeline_config=config,
        trace_count=args.trace_count,
        dataset_seed=args.dataset_seed,
        generated_input_dir=args.generated_input_dir,
        payload_sizes=args.payload_sizes,
        latency_min_ms=args.latency_min_ms,
        latency_max_ms=args.latency_max_ms,
    )
    records = runner.run()
    print(f"wrote {len(records)} benchmark records to {runner.results_file}")


if __name__ == "__main__":
    main()
