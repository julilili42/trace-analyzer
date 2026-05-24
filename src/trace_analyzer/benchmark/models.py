from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class BenchmarkRecord:
    run_id: str
    scenario: str
    iteration: int
    timestamp_utc: str
    git_commit: str | None
    input_path: str
    input_size_mb: float
    requested_trace_count: int | None
    dataset_seed: int | None
    dataset_generated: bool
    payload_sizes: tuple[int, ...] | None
    latency_min_ms: float | None
    latency_max_ms: float | None
    row_count: int
    output_dir: str
    output_size_mb: float
    total_wall_time_s: float
    import_time_s: float
    analyze_time_s: float
    export_time_s: float
    rows_per_second: float
    peak_memory_mb: float
    result_hash: str
    validation_status: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
