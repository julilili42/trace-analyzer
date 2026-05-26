import argparse
import gc
import hashlib
import subprocess
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
from trace_analyzer.benchmark.profiles import (
    DEFAULT_PROFILE_DIR,
    BenchmarkProfile,
    BenchmarkProfileStore,
)
from trace_analyzer.benchmark.recorder import JsonlRecorder
from trace_analyzer.benchmark.rss import RssSampler
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

    def run(self, lazy_loading: bool = False) -> list[BenchmarkRecord]:
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
            self._execute(iteration=iteration, is_warmup=True,
                          lazy_loading=lazy_loading)

        # actual runs
        records: list[BenchmarkRecord] = []
        for iteration in range(1, self.runs + 1):
            record = self._execute(iteration=iteration,
                                   is_warmup=False, lazy_loading=lazy_loading)
            self.recorder.append(record)
            records.append(record)
            if self.verbose:
                print(
                    f"run {iteration}/{self.runs}: "
                    f"total={record.total_wall_time_s:.4f}s, "
                    f"import={record.import_time_s:.4f}s, "
                    f"analyze={record.analyze_time_s:.4f}s, "
                    f"export={record.export_time_s:.4f}s, "
                    f"rows/s={record.rows_per_second:.0f}, "
                    f"peak_memory={record.peak_memory_mb:.2f}MB"
                )

        return records

    def _execute(self, iteration: int, is_warmup: bool, lazy_loading: bool) -> BenchmarkRecord:
        run_id = self._new_run_id(prefix="warmup" if is_warmup else "run")
        output_dir = self.output_root / self.scenario / run_id

        # collect unused references to standarize performance
        gc.collect()

        # start timing and memory process
        with RssSampler() as memory:
            result = self.pipeline.run(
                self.input_path, output_dir, lazy_loading)

        peak_memory_mb = memory.peak_mb

        return self._record_from_result(
            result=result,
            run_id=run_id,
            iteration=iteration,
            peak_memory_mb=peak_memory_mb,
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
    if result.row_count <= 0:
        return "failed_empty_input"

    if (
        not result.artifacts.stats_json.exists()
        or result.artifacts.stats_json.stat().st_size <= 0
    ):
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


def _profile_from_args(args: argparse.Namespace, name: str) -> BenchmarkProfile:
    return BenchmarkProfile(
        name=name,
        description=args.profile_description,
        input_path=args.input_path,
        output_root=args.output_root,
        results_file=args.results_file,
        runs=args.runs,
        warmups=args.warmups,
        scenario=args.scenario,
        trace_count=args.trace_count,
        dataset_seed=args.dataset_seed,
        generated_input_dir=args.generated_input_dir,
        payload_sizes=args.payload_sizes,
        latency_min_ms=args.latency_min_ms,
        latency_max_ms=args.latency_max_ms,
        frame_size=args.frame_size,
        window_ms=args.window_ms,
        link_speed_mbit=args.link_speed_mbit,
    )


def _runner_from_profile(profile: BenchmarkProfile) -> BenchmarkRunner:
    config = PipelineConfig(
        frame_size=profile.frame_size,
        window_ms=profile.window_ms,
        link_speed_mbit=profile.link_speed_mbit,
    )
    return BenchmarkRunner(
        input_path=profile.input_path,
        output_root=profile.output_root,
        results_file=profile.results_file,
        runs=profile.runs,
        warmups=profile.warmups,
        scenario=profile.scenario_name(),
        pipeline_config=config,
        trace_count=profile.trace_count,
        dataset_seed=profile.dataset_seed,
        generated_input_dir=profile.generated_input_dir,
        payload_sizes=profile.payload_sizes,
        latency_min_ms=profile.latency_min_ms,
        latency_max_ms=profile.latency_max_ms,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run trace analyzer benchmarks.")
    parser.add_argument("input_path", nargs="?", default="input_data/data.csv")
    parser.add_argument("--profile", help="Run a saved benchmark profile.")
    parser.add_argument(
        "--profile-dir",
        default=str(DEFAULT_PROFILE_DIR),
        help="Directory containing benchmark profile JSON files.",
    )
    parser.add_argument(
        "--list-profiles",
        action="store_true",
        help="List saved benchmark profiles and exit.",
    )
    parser.add_argument(
        "--create-profile",
        help="Create a benchmark profile from the current CLI options and exit.",
    )
    parser.add_argument(
        "--overwrite-profile",
        action="store_true",
        help="Overwrite an existing profile when used with --create-profile.",
    )
    parser.add_argument(
        "--profile-description",
        default="",
        help="Description stored when creating a profile.",
    )
    parser.add_argument("--output-root", default="output_data/benchmark_runs")
    parser.add_argument(
        "--results-file", default="benchmark_results/results.jsonl")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--scenario", default=None)
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
    parser.add_argument(
        "--lazy-loading",
        dest="lazy_loading",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    store = BenchmarkProfileStore(args.profile_dir)

    if args.list_profiles:
        profiles = store.list_profiles()
        if profiles:
            print("\n".join(profiles))
        else:
            print(f"no profiles found in {store.profile_dir}")
        return

    if args.create_profile:
        profile = _profile_from_args(args, name=args.create_profile)
        path = store.save(profile, overwrite=args.overwrite_profile)
        print(f"wrote profile {profile.name} to {path}")
        return

    if args.profile:
        profile = store.load(args.profile)
    else:
        profile = _profile_from_args(args, name=args.scenario or "baseline")

    runner = _runner_from_profile(profile)
    records = runner.run(lazy_loading=args.lazy_loading)
    print(f"wrote {len(records)} benchmark records to {runner.results_file}")


if __name__ == "__main__":
    main()
