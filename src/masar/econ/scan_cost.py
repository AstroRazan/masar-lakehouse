from pyspark.sql import functions as F
from masar.spark import get_spark
from masar.config import BRONZE


def compare_scans(path: str) -> None:
    spark = get_spark("scan-cost")
    df = spark.read.format("delta").load(path)

    wide = df.filter(F.col("city") == "Riyadh")
    narrow = (df.select("trip_id", "city", "fare_sar")
                .filter(F.col("city") == "Riyadh"))

    print("=== WIDE plan (no select) ===")
    wide.explain("formatted")
    print("=== NARROW plan (3 columns) ===")
    narrow.explain("formatted")

    print(f"riyadh trips wide={wide.count():,}  narrow={narrow.count():,}")


if __name__ == "__main__":
    compare_scans(f"{BRONZE}/trips")