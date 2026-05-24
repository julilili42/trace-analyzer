import json
from pathlib import Path
from dataclasses import asdict, is_dataclass
import numpy as np


class Exporter:
    def __init__(self, output_dir: str | Path = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _json_default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)

        if isinstance(obj, np.floating):
            return float(obj)

        if isinstance(obj, np.ndarray):
            return obj.tolist()

        raise TypeError(
            f"Object of type {type(obj).__name__} is not JSON serializable")

    def export_stats_json(self, stats: dict, filename: str) -> Path:
        path = self.output_dir / filename

        serializable = {
            stream_id: asdict(value) if is_dataclass(value) else value
            for stream_id, value in stats.items()
        }

        with path.open("w", encoding="utf-8") as f:
            json.dump(serializable, f, indent=2, default=self._json_default)

        return path
