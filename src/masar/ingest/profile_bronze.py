from pyspark.sql import functions as F
from masar.spark import get_spark


def profile(table_path: str) -> None:
    spark = get_spark("profile")
    df = spark.read.format("delta").load(table_path)
    n = df.count()
    print(f"rows={n:,}  columns={len(df.columns)}")
    df.select([
        (F.sum(F.col(c).isNull().cast("int")) / F.lit(n)).alias(c)
        for c in df.columns
    ]).show(truncate=False, vertical=True)
    if "trip_id" in df.columns:
        d = df.groupBy("trip_id").count().filter("count > 1").count()
        print(f"duplicate trip_id groups: {d:,}")


if __name__ == "__main__":
    profile("./lakehouse/bronze/trips")