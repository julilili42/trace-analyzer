# Trace Analyzer

A Python trace analysis tool for synthetic Ethernet-like data. It imports trace
CSV files, calculates per-stream payload, latency, and busload metrics, exports
structured statistics, and records benchmark runs for performance comparison.

## Why this project exists

This project explores data pipeline performance, repeatable benchmarking,
large synthetic datasets, and iterative optimization of trace analysis code.

## Features

- Importer, analyzer, and exporter pipeline
- Per-stream payload, latency, and busload statistics
- Deterministic trace dataset generation
- Benchmark runner with warmups and repeated measured runs
- Append-only JSONL benchmark results
- Reusable benchmark profiles for runtime and stability checks
- Structural output validation and output hashing

## Tech Stack

- Python 3.10+
- Polars, pandas, NumPy
- psutil, tracemalloc
- uv
- unittest
