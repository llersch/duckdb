# Headless Duck lineitem benchmark

This directory contains a repeatable SQL harness for comparing `.hduck` against Parquet and a single-table `.duckdb` file on TPC-H `lineitem`.

The harness is intentionally not a CI test. It measures local write and scan behavior, so results depend on machine, build type, filesystem cache, and thread count.

## What it measures

- File size for:
  - `lineitem.hduck`
  - `lineitem.parquet`
  - `lineitem.duckdb`
- Write time for each format.
- Read time for:
  - full scan
  - projected aggregate scan
  - filtered aggregate scan
- Correctness across all formats using row counts and aggregate results.

## Run

Build DuckDB with `headless_duck`, `tpch`, and `parquet` available. In this branch, the local extension config already loads `headless_duck` and `tpch`; `parquet` is part of the base extension config.

From the repository root:

```bash
rm -rf /tmp/headless_duck_lineitem_benchmark
mkdir -p /tmp/headless_duck_lineitem_benchmark
build/release/duckdb < extension/headless_duck/benchmark/lineitem.sql
```

The script defaults to TPC-H scale factor 1:

```sql
CALL dbgen(sf=1);
```

For a quick smoke run, edit that line locally to a smaller value such as `sf=0.01`.

## Output

The script prints four result sets:

- `hduck_lineitem_benchmark_file_sizes`
- `hduck_lineitem_benchmark_timings`
- `hduck_lineitem_benchmark_results`
- `hduck_lineitem_benchmark_correctness`

`elapsed_ms` is wall-clock time measured around each SQL statement pair from inside DuckDB. It is useful for local comparisons, but use repeated runs and a quiet machine before drawing performance conclusions.

## Caveats

- `.hduck` currently has no projection or filter pushdown, so projected and filtered scans are expected to be disadvantaged versus Parquet.
- The `.duckdb` comparison writes one table into a separate database file and scans it through an attached read-only database.
- Delete the output directory before rerunning. The `.hduck` writer currently creates new files and will fail if the output file already exists.
