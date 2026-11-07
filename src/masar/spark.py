"""One place builds Spark with Delta Lake (and optionally Kafka) enabled.

Every tutorial imports get_spark(). The first call downloads the Delta (and Kafka)
jars from Maven Central, so it needs network access once.
"""
import os
import sys

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from masar.config import LAKEHOUSE, ROOT

SPARK_VERSION = "3.5.1"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}"


def get_spark(app_name: str = "masar-lakehouse", kafka: bool = False) -> SparkSession:
    """Spark 3.5 + Delta Lake session for the Masar Lakehouse."""
    # Workers must run the same Python as the driver, even if the venv is not activated.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    LAKEHOUSE.mkdir(parents=True, exist_ok=True)
    metastore = (LAKEHOUSE / "_metastore").as_posix()
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # local stand-in for an object-storage bucket (s3://masar-lakehouse)
        .config("spark.sql.warehouse.dir", LAKEHOUSE.as_posix())
        # shared local metastore so Spark scripts and dbt see the same tables
        .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={metastore};create=true")
        .config("spark.sql.shuffle.partitions", "8")   # laptop-friendly
        .config("spark.ui.showConsoleProgress", "false")
        .enableHiveSupport()
    )
    extra = [KAFKA_PACKAGE] if kafka else []
    return configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()
