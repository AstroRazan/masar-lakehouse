"""Register path-based bronze Delta tables in the local metastore so dbt can use them as sources.

Run after landing bronze:  python -m masar.register
"""
from masar.config import BRONZE
from masar.spark import get_spark

TABLES = ["trips", "drivers", "gps_pings"]


def register_bronze() -> None:
    spark = get_spark("masar-register")
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    from pathlib import Path
    for t in TABLES:
        if (Path(BRONZE) / t / "_delta_log").exists():
            spark.sql(f"CREATE TABLE IF NOT EXISTS bronze.{t} USING DELTA LOCATION '{BRONZE}/{t}'")
            print(f"registered bronze.{t}")
        else:
            print(f"skipped bronze.{t} (not landed yet)")


if __name__ == "__main__":
    register_bronze()
