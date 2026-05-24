# trace-analyzer

Small baseline project for analyzing synthetic Ethernet-like trace data. 
Goal is to iteratively improve analyzing speed in multiple stages.

## Pipeline

The productive code is split into three stages:

```text
Importer -> Analyzer -> Exporter
```

`trace_analyzer.pipeline.Pipeline` orchestrates these stages and returns a
`PipelineResult` with row count, generated artifacts, and per-stage timings.



## Benchmark baseline

The benchmark runner treats the pipeline as code under test. It supports
warmup runs, repeated measured runs, and append-only JSONL recording.

```bash
uv run python -m trace_analyzer.benchmark.runner input_data/data.csv --runs 5 --warmups 1
```

To run against a deterministic generated dataset, set `--trace-count`. Dataset
creation happens before warmups and measured runs, so it is not included in the
pipeline timings.

```bash
uv run python -m trace_analyzer.benchmark.runner --trace-count 1000000 --dataset-seed 42 --runs 5 --warmups 1 --scenario baseline_1m
```

This creates or reuses a deterministic file directly in `input_data/`, for
example `input_data/traces_1000000_seed_42_...csv`.

Generated traces use a fixed set of payload sizes instead of arbitrary values.
The default set is `64,128,256,512,1024,1280,1500`. The latency range and payload size can be overwritten:

```bash
uv run python -m trace_analyzer.benchmark.runner \
  --trace-count 1000000 \
  --payload-sizes 64,128,256,512,1024,1500 \
  --latency-min-ms 0.5 \
  --latency-max-ms 3.0
```

Each measured run writes one record to:

```text
benchmark_results/results.jsonl
```

Each run writes pipeline artifacts under `output_data/benchmark_runs/`. This
directory is ignored by git because it can become large.

Recorded KPIs include:

- total wall time
- import time
- analyze time
- export time
- rows per second
- input size
- requested trace count
- dataset seed
- whether the input dataset was generated for this run
- generated payload sizes
- generated latency range
- output size
- Python allocation peak from `tracemalloc`
- output hash
- structural validation status

`tracemalloc` measures Python allocations. Native memory used by pandas/numpy
is not fully covered, but the metric is still useful as a stable first baseline.

## Benchmark profiles

Benchmark profiles store the full code-under-test setup as JSON: dataset
parameters, pipeline parameters, runs, warmups, and output locations.

Create a profile from CLI options:

```bash
uv run python -m trace_analyzer.benchmark.runner \
  --create-profile runtime_100k \
  --profile-description "Primary profile for comparing code versions" \
  --trace-count 100000 \
  --runs 10 \
  --warmups 2 \
  --payload-sizes 64,128,256,512,1024,1280,1500 \
  --latency-min-ms 0.2 \
  --latency-max-ms 5.0
```

Profiles are written to `benchmark_profiles/<name>.json`.

List profiles:

```bash
uv run python -m trace_analyzer.benchmark.runner --list-profiles
```

Run a profile:

```bash
uv run python -m trace_analyzer.benchmark.runner --profile runtime_100k
```

Included profiles:

- `quick_check_10k`: fast sanity check for the benchmark path, not for performance comparison
- `no_warmup_100k`: one measured run without warmup to observe first-run behavior
- `runtime_100k`: primary profile for comparing code versions locally
- `runtime_500k`: medium-large profile for checking scaling after an optimization
- `runtime_1m`: larger local profile for scalability validation
- `stability_100k`: many repeats on the standard dataset to estimate variance and median stability

The runtime profiles intentionally keep payload sizes, latency range, pipeline
configuration, and seed fixed. The comparison variable should be the code
version, not analyzer-specific input semantics.

## Generate input data

```bash
uv run python script/generate_trace.py input_data/data.csv --rows 100000
```

## Tests

```bash
uv run python -m unittest discover -s tests
```
