"""Sanity tests for the data generator. Run: make test  (no Spark needed)."""
import csv
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def gen(tmp, seed=42, trips=2000):
    subprocess.run([sys.executable, str(ROOT / "scripts" / "make_data.py"), "--seed", str(seed),
                    "--trips", str(trips), "--drivers", "100", "--out", str(tmp)], check=True,
                   capture_output=True)
    return tmp


def read(p):
    with open(p, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_deterministic(tmp_path):
    a, b = gen(tmp_path / "a"), gen(tmp_path / "b")
    assert (a / "raw/trips_2026-06-01.csv").read_bytes() == (b / "raw/trips_2026-06-01.csv").read_bytes()
    assert (a / "raw/gps/sample.ndjson").read_bytes() == (b / "raw/gps/sample.ndjson").read_bytes()


def test_trips_shape(tmp_path):
    gen(tmp_path)
    rows = read(tmp_path / "raw/trips_2026-06-01.csv")
    assert len(rows) == 2000
    assert len(rows[0]) == 12                      # 12 raw columns (+2 lineage columns in bronze = 14)
    assert len({r["trip_id"] for r in rows}) == len(rows)
    assert {r["city"] for r in rows} <= {"Riyadh", "Jeddah", "Dammam", "Mecca", "Medina"}
    assert {r["status"] for r in rows} <= {"completed", "cancelled", "in_progress"}
    assert sum(r["rider_id"] == "rider_00042" for r in rows) >= 5
    nulls = sum(r["dropoff_geohash"] == "" for r in rows) / len(rows)
    assert 0.01 < nulls < 0.06                      # a visible "swamp smell"


def test_fares_plausible(tmp_path):
    gen(tmp_path, trips=5000)
    fares = [float(r["fare_sar"]) for r in read(tmp_path / "raw/trips_2026-06-01.csv") if r["status"] == "completed"]
    assert all(f > 0 for f in fares)
    assert 45 < sum(fares) / len(fares) < 80


def test_pings_and_fixtures(tmp_path):
    gen(tmp_path)
    lines = (tmp_path / "raw/gps/sample.ndjson").read_text().splitlines()
    assert len(lines) > 1000
    assert set(json.loads(lines[0])) == {"trip_id", "driver_id", "city", "lat", "lon", "event_ts"}
    import pandas as pd
    bad = pd.read_parquet(tmp_path / "fixtures/dirty_batch_bad_fares.parquet")
    nul = pd.read_parquet(tmp_path / "fixtures/dirty_batch_keys_null.parquet")
    assert (bad["fare_sar"] <= 0).sum() == 120
    assert nul["trip_id"].isna().sum() == 40
    late = read(tmp_path / "fixtures/late_trip.csv")
    assert late[0]["trip_id"] == "T-900001" and late[0]["start_ts"].startswith("2026-05-30")
