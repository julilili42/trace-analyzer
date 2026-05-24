import csv
import json
import tempfile
import unittest
from pathlib import Path

from trace_analyzer.benchmark.runner import BenchmarkRunner
from trace_analyzer.pipeline import Pipeline


def _write_trace_csv(path: Path) -> None:
    rows = [
        [1, "A", "ECU1", "ECU2", 100, 1.0],
        [2, "A", "ECU1", "ECU2", 200, 2.0],
        [3, "B", "ECU3", "ECU4", 300, 3.0],
        [4, "B", "ECU3", "ECU4", 400, 4.0],
    ]

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
        writer.writerows(rows)


class PipelineBaselineTest(unittest.TestCase):
    def test_pipeline_returns_timings_and_writes_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            input_path = temp_path / "trace.csv"
            output_dir = temp_path / "output"
            _write_trace_csv(input_path)

            result = Pipeline().run(input_path, output_dir)

            self.assertEqual(result.row_count, 4)
            self.assertGreaterEqual(result.timings.total_wall_time_s, 0)
            self.assertGreaterEqual(result.timings.import_time_s, 0)
            self.assertGreaterEqual(result.timings.analyze_time_s, 0)
            self.assertGreaterEqual(result.timings.export_time_s, 0)

            for artifact in result.artifacts.__dict__.values():
                self.assertTrue(artifact.exists())
                self.assertGreater(artifact.stat().st_size, 0)

    def test_benchmark_runner_records_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            input_path = temp_path / "trace.csv"
            output_root = temp_path / "runs"
            results_file = temp_path / "results.jsonl"
            _write_trace_csv(input_path)

            runner = BenchmarkRunner(
                input_path=input_path,
                output_root=output_root,
                results_file=results_file,
                runs=2,
                warmups=1,
                scenario="unit",
                verbose=False,
            )
            records = runner.run()

            self.assertEqual(len(records), 2)
            self.assertTrue(results_file.exists())

            lines = results_file.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 2)

            first_record = json.loads(lines[0])
            self.assertEqual(first_record["scenario"], "unit")
            self.assertEqual(first_record["row_count"], 4)
            self.assertEqual(first_record["validation_status"], "passed")
            self.assertIsNone(first_record["requested_trace_count"])
            self.assertIsNone(first_record["dataset_seed"])
            self.assertFalse(first_record["dataset_generated"])
            self.assertIsNone(first_record["payload_sizes"])
            self.assertIsNone(first_record["latency_min_ms"])
            self.assertIsNone(first_record["latency_max_ms"])
            self.assertGreater(first_record["rows_per_second"], 0)

    def test_benchmark_runner_can_prepare_generated_trace_count(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            output_root = temp_path / "runs"
            results_file = temp_path / "results.jsonl"
            runner = BenchmarkRunner(
                input_path=temp_path / "unused.csv",
                output_root=output_root,
                results_file=results_file,
                runs=1,
                warmups=0,
                scenario="generated",
                verbose=False,
                trace_count=7,
                dataset_seed=123,
                generated_input_dir=temp_path,
                payload_sizes=(64, 1500),
                latency_min_ms=1.5,
                latency_max_ms=2.5,
            )
            records = runner.run()

            self.assertEqual(len(records), 1)
            self.assertIn("traces_7_seed_123", records[0].input_path)
            self.assertIn("sizes_64-1500", records[0].input_path)
            self.assertIn("lat_1p5-2p5", records[0].input_path)
            self.assertEqual(Path(records[0].input_path).parent, temp_path)
            self.assertEqual(records[0].requested_trace_count, 7)
            self.assertEqual(records[0].dataset_seed, 123)
            self.assertTrue(records[0].dataset_generated)
            self.assertEqual(records[0].payload_sizes, (64, 1500))
            self.assertEqual(records[0].latency_min_ms, 1.5)
            self.assertEqual(records[0].latency_max_ms, 2.5)
            self.assertEqual(records[0].row_count, 7)

            with Path(records[0].input_path).open("r", encoding="utf-8") as f:
                generated_rows = list(csv.DictReader(f))

            payload_values = {int(row["payload_bytes"]) for row in generated_rows}
            latency_values = [float(row["latency_ms"]) for row in generated_rows]
            self.assertLessEqual(payload_values, {64, 1500})
            self.assertTrue(all(1.5 <= value <= 2.5 for value in latency_values))

            lines = results_file.read_text(encoding="utf-8").splitlines()
            first_record = json.loads(lines[0])
            self.assertEqual(first_record["requested_trace_count"], 7)
            self.assertEqual(first_record["dataset_seed"], 123)
            self.assertTrue(first_record["dataset_generated"])
            self.assertEqual(first_record["payload_sizes"], [64, 1500])
            self.assertEqual(first_record["latency_min_ms"], 1.5)
            self.assertEqual(first_record["latency_max_ms"], 2.5)


if __name__ == "__main__":
    unittest.main()
