from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

from trace_analyzer.importer import Importer
from trace_analyzer.exporter import Exporter
from trace_analyzer.analyzer import Analyzer


@dataclass(frozen=True)
class PipelineConfig:
    frame_size: int = 64
    window_ms: float = 10.0
    link_speed_mbit: float = 100.0


@dataclass(frozen=True)
class PipelineTimings:
    total_wall_time_s: float
    import_time_s: float
    analyze_time_s: float
    export_time_s: float


@dataclass(frozen=True)
class PipelineArtifacts:
    stats_json: Path


@dataclass(frozen=True)
class PipelineResult:
    input_path: Path
    output_dir: Path
    row_count: int
    timings: PipelineTimings
    artifacts: PipelineArtifacts


class Pipeline:
    def __init__(self, config: PipelineConfig | None = None):
        self.config = config or PipelineConfig()

    def run(self, input_path: str | Path, output_path: str | Path, lazy_loading: bool = False) -> PipelineResult:
        input_path = Path(input_path)
        output_path = Path(output_path)

        total_start = perf_counter()

        if lazy_loading:
            # aggregation and analyze in one step
            import_time_s = 0.0
        else:
            import_start = perf_counter()
            df = Importer.import_csv(input_path=input_path)
            import_time_s = perf_counter() - import_start

        # analyze
        analyze_start = perf_counter()

        if lazy_loading:
            stats_df = Importer.create_polars_df(
                input_path=input_path,
                frame_size=self.config.frame_size,
                window_ms=self.config.window_ms,
                link_speed_mbit=self.config.link_speed_mbit,
            )
            stats, row_count = Analyzer.stats_from_polars_df(stats_df)
        else:
            analyzer = Analyzer(df)
            stats = analyzer.calc_stats(
                frame_size=self.config.frame_size,
                window_ms=self.config.window_ms,
                link_speed_mbit=self.config.link_speed_mbit,
            )

        analyze_time_s = perf_counter() - analyze_start

        # export
        export_start = perf_counter()
        exporter = Exporter(output_path)
        artifacts = PipelineArtifacts(
            stats_json=exporter.export_stats_json(stats, "stats.json"),
        )
        export_time_s = perf_counter() - export_start

        timings = PipelineTimings(
            total_wall_time_s=perf_counter() - total_start,
            import_time_s=import_time_s,
            analyze_time_s=analyze_time_s,
            export_time_s=export_time_s,
        )

        return PipelineResult(
            input_path=input_path,
            output_dir=exporter.output_dir,
            row_count=row_count if lazy_loading else len(df),
            timings=timings,
            artifacts=artifacts,
        )
