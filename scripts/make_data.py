#!/usr/bin/env python
"""Generate the synthetic Masar dataset. Same --seed, same data, on every laptop.

    python scripts/make_data.py --seed 42

Writes (all git-ignored):
  data/raw/drivers.csv                     driver master data
  data/raw/trips_<date>.csv                one file per day (default 1 day, 48,200 trips)
  data/raw/gps/sample.ndjson               GPS pings, newline-delimited JSON, in arrival order
  data/fixtures/dirty_batch_*.parquet      silver-shaped candidate batches with injected faults
  data/fixtures/late_trip.csv              one trip that arrives two days late (raw schema)
  data/fixtures/corrections.csv            fare corrections for the MERGE tutorial
  data/fixtures/erasure_request.json       a PDPL erasure request
"""
import argparse
import csv
import json
import math
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

CITIES = {  # name: (weight, centre lat, centre lon)
    "Riyadh": (0.38, 24.7136, 46.6753),
    "Jeddah": (0.26, 21.4858, 39.1925),
    "Dammam": (0.14, 26.4207, 50.0888),
    "Mecca": (0.12, 21.3891, 39.8579),
    "Medina": (0.10, 24.5247, 39.5692),
}
VEHICLES = ["economy", "economy", "economy", "comfort", "comfort", "xl"]
PAYMENTS = ["card", "card", "cash", "wallet"]
RAW_TRIP_COLUMNS = ["trip_id", "driver_id", "rider_id", "city", "start_ts", "end_ts", "distance_km",
                    "fare_sar", "status", "surge_multiplier", "payment_method", "dropoff_geohash"]
TS = "%Y-%m-%d %H:%M:%S"
# trips per hour of day, relative (morning and evening peaks)
HOUR_W = [1, 1, 1, 1, 1, 2, 4, 7, 8, 6, 5, 5, 6, 6, 5, 5, 6, 8, 9, 9, 8, 6, 4, 2]


def pick_city(rng):
    r, acc = rng.random(), 0.0
    for name, (w, _, _) in CITIES.items():
        acc += w
        if r <= acc:
            return name
    return "Riyadh"


def make_drivers(rng, n):
    rows = []
    for i in range(1, n + 1):
        rows.append({"driver_id": f"D-{i:04d}", "vehicle_type": rng.choice(VEHICLES),
                     "driver_rating": round(min(5.0, max(3.5, rng.gauss(4.7, 0.25))), 2),
                     "city": pick_city(rng)})
    return rows


def geohash_like(rng, city):
    prefix = {"Riyadh": "7z", "Jeddah": "sy", "Dammam": "th", "Mecca": "sy", "Medina": "sg"}[city]
    return prefix + "".join(rng.choice("0123456789bcdefghjkmnpqrstuvwxyz") for _ in range(4))


def make_trips(rng, day, n, drivers, start_index, riders=20000):
    by_city = {}
    for d in drivers:
        by_city.setdefault(d["city"], []).append(d["driver_id"])
    hours = rng.choices(range(24), weights=HOUR_W, k=n)
    rows = []
    for k in range(n):
        city = pick_city(rng)
        km = min(60.0, max(1.0, rng.lognormvariate(math.log(9), 0.55)))
        dur = max(4.0, 6 + km * 2.1 * rng.uniform(0.8, 1.4))
        surge = rng.choices([1.0, 1.2, 1.5, 2.0], weights=[70, 15, 10, 5])[0]
        start = day + timedelta(hours=hours[k], seconds=rng.randrange(3600))
        status = rng.choices(["completed", "cancelled", "in_progress"], weights=[92, 5, 3])[0]
        fare = round((7 + 3.4 * km + 0.35 * dur) * surge, 2)
        rows.append({
            "trip_id": f"T-{start_index + k:06d}",
            "driver_id": rng.choice(by_city.get(city) or [d["driver_id"] for d in drivers]),
            "rider_id": f"rider_{rng.randrange(riders):05d}",
            "city": city,
            "start_ts": start.strftime(TS),
            "end_ts": (start + timedelta(minutes=dur)).strftime(TS),
            "distance_km": round(km, 2),
            "fare_sar": fare,
            "status": status,
            "surge_multiplier": surge,
            "payment_method": rng.choice(PAYMENTS),
            "dropoff_geohash": "" if rng.random() < 0.031 else geohash_like(rng, city),
        })
    return rows


def write_csv(path, rows, columns):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        w.writerows(rows)


def make_pings(rng, trips, n_trips=1000, per_trip=120, step_s=10, late_share=0.01, late_s=240):
    chosen = [t for t in trips if t["status"] == "completed"]
    chosen = rng.sample(chosen, min(n_trips, len(chosen)))
    events = []
    for t in chosen:
        _, lat, lon = CITIES[t["city"]]
        lat += rng.uniform(-0.08, 0.08)
        lon += rng.uniform(-0.08, 0.08)
        start = datetime.strptime(t["start_ts"], TS)
        for i in range(per_trip):
            lat += rng.gauss(0, 0.0007)
            lon += rng.gauss(0, 0.0007)
            ev = start + timedelta(seconds=i * step_s)
            emit = ev + timedelta(seconds=late_s if rng.random() < late_share else 0)
            events.append((emit, {"trip_id": t["trip_id"], "driver_id": t["driver_id"], "city": t["city"],
                                  "lat": round(lat, 6), "lon": round(lon, 6),
                                  "event_ts": ev.strftime("%Y-%m-%dT%H:%M:%SZ")}))
    events.sort(key=lambda e: e[0])
    return [e[1] for e in events]


def write_fixtures(rng, trips, day, out):
    import pandas as pd  # only needed for parquet fixtures
    fx = out / "fixtures"
    fx.mkdir(parents=True, exist_ok=True)

    def silver_like(rows):
        recs = []
        for t in rows:
            if t["status"] != "completed":
                continue
            s, e = datetime.strptime(t["start_ts"], TS), datetime.strptime(t["end_ts"], TS)
            recs.append({"trip_id": t["trip_id"], "driver_id": t["driver_id"], "rider_id": t["rider_id"],
                         "city": t["city"], "started_at": s, "ended_at": e,
                         "distance_km": float(t["distance_km"]), "fare_sar": float(t["fare_sar"]),
                         "duration_min": (e - s).total_seconds() / 60.0, "status": t["status"],
                         "surge_multiplier": float(t["surge_multiplier"])})
        return pd.DataFrame(recs)

    base = silver_like(trips)
    bad = base.copy()
    idx = rng.sample(range(len(bad)), 120)
    bad.loc[bad.index[idx], "fare_sar"] = -bad.loc[bad.index[idx], "fare_sar"].abs()
    bad.to_parquet(fx / "dirty_batch_bad_fares.parquet", index=False)

    nulls = base.copy()
    idx = rng.sample(range(len(nulls)), 40)
    nulls["trip_id"] = nulls["trip_id"].astype(object)
    nulls.loc[nulls.index[idx], "trip_id"] = None
    nulls.to_parquet(fx / "dirty_batch_keys_null.parquet", index=False)

    # one trip that arrives two days after it happened (lookback drill)
    late_start = day - timedelta(days=2) + timedelta(hours=10, minutes=15)
    late = {"trip_id": "T-900001", "driver_id": "D-0001", "rider_id": "rider_00042", "city": "Riyadh",
            "start_ts": late_start.strftime(TS), "end_ts": (late_start + timedelta(minutes=22)).strftime(TS),
            "distance_km": 11.4, "fare_sar": 58.5, "status": "completed", "surge_multiplier": 1.0,
            "payment_method": "card", "dropoff_geohash": "7zabcd"}
    write_csv(fx / "late_trip.csv", [late], RAW_TRIP_COLUMNS)

    done = [t for t in trips if t["status"] == "completed"]
    corr = [{"trip_id": t["trip_id"], "fare_sar": round(float(t["fare_sar"]) + 2.5, 2)} for t in done[:5]]
    write_csv(fx / "corrections.csv", corr, ["trip_id", "fare_sar"])
    (fx / "erasure_request.json").write_text(json.dumps({"rider_id": "rider_00042"}) + "\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--date", default="2026-06-01", help="first day, YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=1)
    ap.add_argument("--trips", type=int, default=48200, help="trips per day")
    ap.add_argument("--drivers", type=int, default=1850)
    ap.add_argument("--out", default=str(ROOT / "data"))
    ap.add_argument("--no-fixtures", action="store_true", help="skip parquet fixtures (no pandas/pyarrow needed)")
    a = ap.parse_args()

    rng = random.Random(a.seed)
    out = Path(a.out)
    rel = lambda p: p.relative_to(ROOT) if p.is_relative_to(ROOT) else p  # noqa: E731
    first = datetime.strptime(a.date, "%Y-%m-%d")

    drivers = make_drivers(rng, a.drivers)
    write_csv(out / "raw" / "drivers.csv", drivers, ["driver_id", "vehicle_type", "driver_rating", "city"])
    print(f"wrote {rel(out / 'raw' / 'drivers.csv')}  {len(drivers):>9,} rows")

    all_trips, nxt = [], 1
    for d in range(a.days):
        day = first + timedelta(days=d)
        rows = make_trips(rng, day, a.trips, drivers, nxt)
        nxt += a.trips
        p = out / "raw" / f"trips_{day.strftime('%Y-%m-%d')}.csv"
        if d == 0:
            # the erasure tutorial needs this rider to exist in the data
            done = [r for r in rows if r["status"] == "completed"]
            for r in rng.sample(done, 5):
                r["rider_id"] = "rider_00042"
            all_trips = rows
        write_csv(p, rows, RAW_TRIP_COLUMNS)
        print(f"wrote {rel(p)}  {len(rows):>9,} rows")

    pings = make_pings(rng, all_trips)
    gp = out / "raw" / "gps" / "sample.ndjson"
    gp.parent.mkdir(parents=True, exist_ok=True)
    with open(gp, "w", encoding="utf-8") as f:
        for p_ in pings:
            f.write(json.dumps(p_) + "\n")
    print(f"wrote {rel(gp)}  {len(pings):>9,} pings")

    if not a.no_fixtures:
        write_fixtures(rng, all_trips, first, out)
        print(f"wrote {rel(out / 'fixtures')}/ (dirty batches, late trip, corrections, erasure request)")
    else:
        print("skipped fixtures (--no-fixtures)")


if __name__ == "__main__":
    main()
