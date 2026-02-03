logstat is a cross-platform C++ command-line tool for analyzing HTTP-style CSV log files.
It streams records from disk, aggregates metrics in memory, and produces both human-readable
and JSON reports.

## Features

- Streaming CSV ingestion (no full-file buffering)
- Robust CSV parsing (quoted fields, escaped quotes)
- Per-endpoint aggregation
- HTTP status code bucketing (2xx–5xx)
- Latency percentiles (configurable)
- JSON or text output
- File output support
- Cross-platform build via CMake (Linux / Windows)

## Example

```bash
./logstat ingest \
  --file data/sample.csv \
  --top 3 \
  --percentiles 50,95,99 \
  --format json \
  --out report.json
