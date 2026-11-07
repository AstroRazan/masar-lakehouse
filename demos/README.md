# Demo: what Parquet and Delta look like on disk

`delta_explainer.py` builds a small Parquet folder and a small Delta table from the first
rows of `data/raw/trips_2026-06-01.csv`, then prints what is on disk after each operation.
Read the output top to bottom. Lines starting with `->` say what to look at.

## Run it

```bash
source .venv/bin/activate
make data                              # once, if data/raw is empty
python demos/delta_explainer.py
```

Takes about 1 to 2 minutes (the first ever run also downloads the Delta jars). It wipes and
rebuilds `lakehouse/demo/` every time, so you can repeat it. To read only the walkthrough
without Spark's start-up lines, add `2>/dev/null`.

## What each step shows

| Step | What happens | Concept |
|---|---|---|
| 1 | Write 2 plain Parquet files; print schema, row-group min/max stats, read two columns only | Parquet is columnar, compressed and self-describing; a folder of files is not a table |
| 2 | Write a Delta table (v0), partitioned by `city`; print the folder and `00000000000000000000.json` | The `_delta_log` holds schema, partitioning and the list of files (`add`) |
| 3 | Append 500 rows (v1) | A commit is one log file; appends only `add` |
| 4 | `MERGE` to fix 3 fares (v2) | Parquet is immutable: update = `remove` old file + `add` rewritten file; the old file stays on disk |
| 5 | Read v0, v1 and current; `DESCRIBE HISTORY` | Time travel = reading the log as of an earlier version |
| 6 | Append a wrong-typed row; add a `CHECK` constraint and append a negative fare | Schema enforcement and constraints reject writes before they commit: no new log file |
| 7 | Drop an unreferenced Parquet file into the table folder; `VACUUM ... DRY RUN` | The log decides what is in the table; orphan files do not count; VACUUM only lists files past the retention window |
| 8 | 20 tiny appends, `DESCRIBE DETAIL`, `OPTIMIZE` | Small-file problem, checkpoints, and compaction as one more commit |

## Output locations

- `lakehouse/demo/plain_parquet/`: the step 1 files
- `lakehouse/demo/delta_trips/`: the Delta table and its `_delta_log/`. Open the JSON files in
  your editor and compare them with what the script printed.

Both are git-ignored.

## Clean up

```bash
rm -rf lakehouse/demo
```
