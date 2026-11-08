from pyspark.sql import functions as F
from masar.spark import get_spark

BRONZE = "./lakehouse/bronze"


def _lineage(df):
    return (df.withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.input_file_name()))


def land_trips(source_glob: str) -> None:
    spark = get_spark("land-trips")
    raw = (spark.read
           .option("header", True)
           .option("mode", "PERMISSIVE")   # keep malformed rows, do not drop
           .csv(source_glob))
    _lineage(raw).write.format("delta").mode("append").save(f"{BRONZE}/trips")

def land_gps(source_glob: str) -> None:
    spark = get_spark("land-gps")
    raw = spark.read.json(source_glob)   # schema-on-read for nested JSON
    _lineage(raw).write.format("delta").mode("append").save(f"{BRONZE}/gps_pings")
def land_drivers(source_glob: str) -> None:
    spark = get_spark("land-drivers")
    raw = spark.read.option("header", True).csv(source_glob)
    _lineage(raw).write.format("delta").mode("append").save(f"{BRONZE}/drivers")


if __name__ == "__main__":w
    land_trips("data/raw/trips_2026-*.csv")
    land_drivers("data/raw/drivers.csv")
    land_gps("data/raw/gps/*.ndjson")
    print("landed")