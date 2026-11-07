"""What do Parquet files and a Delta table look like on disk?

Run from the repo root:   python demos/delta_explainer.py

Everything is written under lakehouse/demo/ (git-ignored) and wiped at the start of
every run, so you can repeat it as often as you like.  Read the console output top to
bottom: each step prints a heading and a few "->" lines saying what to look at.
"""
import json
import os
import shutil
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))          # so the script runs without `make`
# Spark's Python workers must use the same interpreter as this script, even when the
# venv is not activated (otherwise they start whatever `python3` is first on PATH).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

import pandas as pd                             # noqa: E402
import pyarrow as pa                            # noqa: E402
import pyarrow.parquet as pq                    # noqa: E402
from delta import DeltaTable                    # noqa: E402

from masar.config import LAKEHOUSE, RAW         # noqa: E402
from masar.spark import get_spark               # noqa: E402

DEMO = LAKEHOUSE / "demo"
PLAIN = DEMO / "plain_parquet"
DELTA = DEMO / "delta_trips"
LOG = DELTA / "_delta_log"
SQL_PATH = f"delta.`{DELTA}`"                   # how SQL addresses a path-based Delta table
SCHEMA = ("trip_id string, driver_id string, rider_id string, city string, start_ts string, "
          "end_ts string, distance_km double, fare_sar double, status string, "
          "surge_multiplier double, payment_method string, dropoff_geohash string")


# ----------------------------------------------------------------------------- helpers
def heading(title):
    print("\n" + "=" * 78 + f"\n{title}\n" + "=" * 78)


def look(text):
    """A short plain-English pointer: what to look at in the output above/below."""
    print(f"  -> {text}")


def human(n):
    for unit in ("B", "KB", "MB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GB"


def tree(path, limit=14, _prefix=""):
    """Print a folder tree with file sizes (hidden files such as Spark's .crc are skipped)."""
    entries = sorted(e for e in os.listdir(path) if not e.startswith("."))
    shown = entries[:limit]
    for name in shown:
        full = Path(path) / name
        if full.is_dir():
            print(f"{_prefix}{name}/")
            tree(full, limit, _prefix + "    ")
        else:
            print(f"{_prefix}{name}  ({human(full.stat().st_size)})")
    if len(entries) > limit:
        print(f"{_prefix}... and {len(entries) - limit} more")


def commits():
    return sorted(f for f in os.listdir(LOG) if f.endswith(".json"))


def log_listing(last=4):
    c = commits()
    print(f"  _delta_log has {len(c)} commit files; latest: {', '.join(c[-last:])}")


def show_log(version, per_kind=1):
    """Pretty-print one commit file; show at most `per_kind` actions of each kind."""
    f = LOG / f"{version:020d}.json"
    actions = [json.loads(line) for line in f.read_text().splitlines()]
    kinds = Counter(next(iter(a)) for a in actions)
    print(f"{f.name}: {len(actions)} actions -> {dict(kinds)}")
    shown = Counter()
    for a in actions:
        kind, body = next(iter(a.items()))
        if shown[kind] >= per_kind:
            continue
        shown[kind] += 1
        body = dict(body)
        if kind == "metaData":
            fields = json.loads(body["schemaString"])["fields"]
            body["schemaString"] = [f"{x['name']}: {x['type']}" for x in fields]
        if kind == "add" and body.get("stats"):
            s = json.loads(body["stats"])
            body["stats"] = {"numRecords": s["numRecords"],
                             "fare_sar_min": s["minValues"].get("fare_sar"),
                             "fare_sar_max": s["maxValues"].get("fare_sar"),
                             "...": "(trimmed for display)"}
        print(json.dumps({kind: body}, indent=2))
    hidden = {k: n - shown[k] for k, n in kinds.items() if n > shown[k]}
    if hidden:
        print(f"  ... plus more entries not printed: {hidden}")


def error_text(exc):
    """The error as Delta reports it: class, message and the chain of causes, no stack trace."""
    if hasattr(exc, "java_exception"):                  # Py4JJavaError from a failed job
        msg = str(exc.java_exception.toString())
    else:                                               # AnalysisException etc.
        msg = str(exc)
        try:
            cause = exc._origin.getCause()
            while cause is not None:
                msg += f"\ncaused by: {cause.getMessage()}"
                cause = cause.getCause()
        except Exception:                               # noqa: BLE001
            pass
    msg = msg.split("\n\tat ")[0]
    return type(exc).__name__ + ":\n      " + msg.replace("\n", "\n      ")


def row_count():
    return spark.read.format("delta").load(str(DELTA)).count()


def to_spark(pdf, schema=SCHEMA):
    # coalesce(1): one Spark task, so one file per city per write (easier to read in the tree)
    return spark.createDataFrame(pdf, schema).coalesce(1)


def append(pdf):
    to_spark(pdf).write.format("delta").mode("append").save(str(DELTA))


def removed_files(version):
    f = LOG / f"{version:020d}.json"
    return [json.loads(x)["remove"]["path"] for x in f.read_text().splitlines() if '"remove"' in x]


# ----------------------------------------------------------------------------- setup
shutil.rmtree(DEMO, ignore_errors=True)
DEMO.mkdir(parents=True)
trips = pd.read_csv(RAW / "trips_2026-06-01.csv", nrows=2900)
sample = trips.iloc[:2000]            # the table we start with
more_rows = trips.iloc[2000:2500]     # step 3: 500 more rows
small_rows = trips.iloc[2500:2900]    # step 8: 20 appends x 20 rows
csv_bytes = len(sample.to_csv(index=False).encode())

spark = get_spark("delta-explainer")
spark.sparkContext.setLogLevel("OFF")   # failed writes in step 6 are shown by this script

# ----------------------------------------------------------------------------- 1
heading("STEP 1  Plain Parquet: just files, no table, no log")
PLAIN.mkdir()
for i, part in enumerate((sample.iloc[:1000], sample.iloc[1000:])):
    pq.write_table(pa.Table.from_pandas(part, preserve_index=False),
                   PLAIN / f"part-{i:04d}.parquet", row_group_size=500, compression="snappy")
print("Folder tree:")
tree(PLAIN)
total = sum(p.stat().st_size for p in PLAIN.glob("*.parquet"))
print(f"\n2000 rows as CSV: {human(csv_bytes)}   as Parquet (2 files): {human(total)}"
      f"   -> {csv_bytes / total:.1f}x smaller")

f0 = PLAIN / "part-0000.parquet"
print(f"\nSchema stored INSIDE {f0.name} (the file describes itself):")
print(pq.read_schema(f0).remove_metadata().to_string())

print("\nRow-group statistics for fare_sar (written by the writer, free to read):")
meta = pq.ParquetFile(f0).metadata
idx = meta.schema.names.index("fare_sar")
for rg in range(meta.num_row_groups):
    c = meta.row_group(rg).column(idx)
    print(f"  row group {rg}: rows={meta.row_group(rg).num_rows}  "
          f"min={c.statistics.min}  max={c.statistics.max}  nulls={c.statistics.null_count}")
look("A query like `fare_sar > 500` can SKIP a row group whose max is below 500, without reading it.")

wanted = ["city", "fare_sar"]
per_col = {n: 0 for n in meta.schema.names}
for rg in range(meta.num_row_groups):
    for j, n in enumerate(meta.schema.names):
        per_col[n] += meta.row_group(rg).column(j).total_compressed_size
all_bytes = sum(per_col.values())
read_bytes = sum(per_col[c] for c in wanted)
t = pq.read_table(f0, columns=wanted)
print(f"\nReading only {wanted}: got {t.num_rows} rows x {t.num_columns} columns, touching "
      f"{human(read_bytes)} of {human(all_bytes)} column data ({100 * read_bytes / all_bytes:.0f}%).")
look("Columnar layout: the other 10 columns are never read from disk. A CSV would read everything.")
look("But notice what is missing: nothing says these two files belong together. It is just a folder.")

# ----------------------------------------------------------------------------- 2
heading("STEP 2  Delta table, version 0 (same data, partitioned by city)")
to_spark(sample).write.format("delta").partitionBy("city").save(str(DELTA))
print("Folder tree:")
tree(DELTA)
print("\nCommit file for version 0 (one JSON action per line, shown formatted):")
show_log(0)
look("commitInfo = who/what/when: the WRITE operation and its metrics.")
look("metaData   = the table's schema and partitionColumns (['city']): the log owns the schema.")
look("add        = one entry per data file: its path, size, partition value and min/max stats.")
look("The Parquet files are the same kind of file as in step 1; the log is what makes them a table.")

# ----------------------------------------------------------------------------- 3
heading("STEP 3  Append 500 rows -> version 1")
append(more_rows)
show_log(1)
print(f"\nRow count now: {row_count()}  (2000 + 500)")
look("Only `add` entries. Nothing was rewritten: appends just add new immutable files.")

# ----------------------------------------------------------------------------- 4
heading("STEP 4  Correct 3 fares with MERGE -> version 2")
ids = sample.trip_id.iloc[[10, 500, 1500]].tolist()
old = dict(zip(sample.trip_id, sample.fare_sar))
fixes = [(i, round(old[i] + 10.0, 2)) for i in ids]
print("Corrections (trip_id, new fare):", fixes)
files_before = {str(p.relative_to(DELTA)) for p in DELTA.rglob("*.parquet")}
(DeltaTable.forPath(spark, str(DELTA)).alias("t")
 .merge(spark.createDataFrame(fixes, "trip_id string, new_fare double").alias("c"),
        "t.trip_id = c.trip_id")
 .whenMatchedUpdate(set={"fare_sar": "c.new_fare"}).execute())
show_log(2, per_kind=2)
gone = removed_files(2)
print("\nFiles the log says are removed, and whether they are still on disk:")
for p in gone:
    print(f"  {p}  exists on disk: {(DELTA / p).exists()}")
look("Parquet files are immutable, so an UPDATE = `remove` the old file + `add` a rewritten copy.")
look("The old file is still there. Only the log changed which files count. That is what makes "
     "time travel possible (step 5).")
look(f"Files on disk: {len(files_before)} before the merge, "
     f"{len(list(DELTA.rglob('*.parquet')))} after.")

# ----------------------------------------------------------------------------- 5
heading("STEP 5  Time travel")
trip = ids[0]
for v in (0, 1, None):
    r = spark.read.format("delta")
    r = r.option("versionAsOf", v) if v is not None else r
    d = r.load(str(DELTA))
    label = f"version {v}" if v is not None else "current  "
    fare = d.filter(f"trip_id = '{trip}'").first()
    print(f"  {label}: rows={d.count():>5}   fare of {trip} = "
          f"{fare.fare_sar if fare else '(trip not in this version)'}")
look(f"{trip} had fare {old[trip]} before the MERGE and has {dict(fixes)[trip]} now; "
     "both are readable.")
print("\nDESCRIBE HISTORY (the log, summarised):")
(spark.sql(f"DESCRIBE HISTORY {SQL_PATH}")
 .select("version", "timestamp", "operation", "operationMetrics")
 .show(truncate=75))

# ----------------------------------------------------------------------------- 6
heading("STEP 6  Writes that must fail")
print("6a) Append a row where fare_sar is the string 'ten':")
bad = spark.createDataFrame(
    [("T-X", "D-1", "R-1", "Riyadh", "2026-06-01 10:00:00", "2026-06-01 10:30:00",
      5.0, "ten", "completed", 1.0, "cash", "7z2y0h")],
    "trip_id string, driver_id string, rider_id string, city string, start_ts string, "
    "end_ts string, distance_km double, fare_sar string, status string, "
    "surge_multiplier double, payment_method string, dropoff_geohash string")
log_listing()
before = row_count()
try:
    bad.write.format("delta").mode("append").save(str(DELTA))
    print("  !! unexpectedly succeeded")
except Exception as e:                                  # noqa: BLE001 (we want the text)
    print("  ERROR ->", error_text(e))
log_listing()
print(f"  row count: {before} -> {row_count()}")
look("Schema enforcement: rejected BEFORE any file became part of the table. No new commit file.")

print("\n6b) Add CHECK constraint fare_sar > 0, then append a row with fare -5:")
spark.sql(f"ALTER TABLE {SQL_PATH} ADD CONSTRAINT positive_fare CHECK (fare_sar > 0)")
constraint_version = len(commits()) - 1
print("  Adding the constraint is itself a commit:")
show_log(constraint_version)
negative = sample.iloc[[0]].copy()
negative["trip_id"], negative["fare_sar"] = "T-NEG", -5.0
log_listing()
before = row_count()
try:
    append(negative)
    print("  !! unexpectedly succeeded")
except Exception as e:                                  # noqa: BLE001
    print("  ERROR ->", error_text(e))
log_listing()
print(f"  row count: {before} -> {row_count()}")
look("The rule lives in the log (metaData.configuration), so every writer must obey it. Note protocol.minWriterVersion went 2 -> 3: CHECK constraints need a newer writer.")
look("Again no new commit file: a failed write leaves the table exactly as it was.")

# ----------------------------------------------------------------------------- 7
heading("STEP 7  Orphan files: a Parquet file the log does not know about")
orphan_dir = DELTA / "city=Riyadh"
riyadh = sample[sample.city == "Riyadh"].drop(columns="city").head(5)
pq.write_table(pa.Table.from_pandas(riyadh, preserve_index=False),
               orphan_dir / "orphan-handmade.parquet")
print(f"Copied {orphan_dir.name}/orphan-handmade.parquet into the table folder (no commit).")
print(f"  Delta read row count:        {row_count()}   (the orphan is ignored)")
raw_count = spark.read.parquet(str(DELTA)).count()
print(f"  Plain Parquet read of folder: {raw_count}   (counts the orphan AND the files "
      "replaced in step 4)")
look("A table is whatever the log says. Files on disk that the log does not reference do not exist.")
look("This is why a crashed job that left half-written files cannot corrupt a Delta table.")

print("\nVACUUM ... DRY RUN with the default 7-day retention:")
default_run = spark.sql(f"VACUUM {SQL_PATH} DRY RUN").collect()
print(f"  would delete {len(default_run)} files")
look("Nothing: every unreferenced file is younger than 7 days. The safety window protects "
     "readers and time travel.")
print("\nVACUUM ... RETAIN 0 HOURS DRY RUN (retention check switched off for the demo only):")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
dry = spark.sql(f"VACUUM {SQL_PATH} RETAIN 0 HOURS DRY RUN").collect()
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
print(f"  would delete {len(dry)} files:")
for r in dry:
    print("   ", os.path.relpath(r[0].replace("file:", ""), DELTA))
look("Both the orphan and the pre-MERGE files are listed; _delta_log is never touched. "
     "Nothing was deleted (DRY RUN).")

# ----------------------------------------------------------------------------- 8
heading("STEP 8  Maintenance: many small files, then OPTIMIZE")
for i in range(20):
    append(small_rows.iloc[i * 20:(i + 1) * 20])
detail = lambda: spark.sql(f"DESCRIBE DETAIL {SQL_PATH}").select("numFiles", "sizeInBytes").first()
b = detail()
rows_before = row_count()
print(f"After 20 tiny appends: rows={rows_before}  numFiles={b.numFiles}  "
      f"size={human(b.sizeInBytes)}")
log_listing(last=5)
ckpts = sorted(f for f in os.listdir(LOG) if f.endswith(".checkpoint.parquet"))
look(f"Every append was a commit. After 10 commits Delta wrote a checkpoint file {ckpts} (a Parquet "
     "snapshot of the log) so readers need not replay every JSON file.")
print("\nOPTIMIZE:")
spark.sql(f"OPTIMIZE {SQL_PATH}").select("metrics.numFilesAdded", "metrics.numFilesRemoved").show()
a = detail()
print(f"After OPTIMIZE: rows={row_count()}  numFiles={a.numFiles}  size={human(a.sizeInBytes)}")
print("\nThe OPTIMIZE commit:")
show_log(len(commits()) - 1)
look(f"Same {rows_before} rows, {b.numFiles} files -> {a.numFiles}. Small files are `remove`d, "
     "compacted files are `add`ed.")
look("dataChange=false marks it as housekeeping: readers see identical data before and after.")

# ----------------------------------------------------------------------------- summary
heading("WHAT YOU JUST SAW")
print("""
  Step 1  Parquet files are immutable, columnar and self-describing (schema + min/max
          stats inside), but a folder of them is not a table.
  Step 2  Delta = Parquet files + a _delta_log of JSON commits. The log is the table's
          source of truth: schema, partitioning, and exactly which files belong.
  Step 3  A commit is one new log file. Appending = adding `add` entries. The write
          becomes visible all at once, when the log file lands (atomic commit).
  Step 4  Updates never edit a file: `remove` the old, `add` the rewritten one. The old
          file stays on disk, so the previous version is still readable.
  Step 5  Time travel = read the log as of an earlier version.
  Step 6  Schema enforcement and CHECK constraints reject bad writes before they are
          committed: no new log file, no change to the table.
  Step 7  Orphan files do not count: if the log does not reference a file, it is not
          in the table. VACUUM removes such files once they are older than retention.
  Step 8  Many small commits make many small files; OPTIMIZE compacts them in one more
          commit without changing the data.

  Clean up with:  rm -rf lakehouse/demo
""")
spark.stop()
