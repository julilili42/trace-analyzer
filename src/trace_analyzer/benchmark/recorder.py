import json
from pathlib import Path

from trace_analyzer.benchmark.models import BenchmarkRecord


class JsonlRecorder:
    def __init__(self, results_file: str | Path):
        self.results_file = Path(results_file)
        self.results_file.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record: BenchmarkRecord) -> None:
        with self.results_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record.to_dict(), sort_keys=True))
            f.write("\n")
