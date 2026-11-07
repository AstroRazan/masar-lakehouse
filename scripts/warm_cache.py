"""Pre-download the Spark/Delta/Kafka jars so the first tutorial run is fast.

Creates no tables, no metastore and no lakehouse/ folder: it starts a bare Spark
session in a temp directory, which makes Ivy cache the jars under ~/.ivy2.
Trainees still build their own session with masar.spark.get_spark() in Tutorial 1.
"""
import tempfile

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

with tempfile.TemporaryDirectory() as tmp:
    builder = (
        SparkSession.builder.appName("warm-cache")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", tmp)
    )
    spark = configure_spark_with_delta_pip(builder, extra_packages=[KAFKA_PACKAGE]).getOrCreate()
    spark.range(1).count()
    spark.stop()
print("Spark, Delta and Kafka jars cached.")
