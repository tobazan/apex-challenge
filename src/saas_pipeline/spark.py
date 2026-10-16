import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark_session(app_name: str = "SAAS_Medallion_Pipeline") -> SparkSession:
    """
    Returns an active SparkSession on Databricks
    or builds a local Delta-enabled SparkSession for local / CI runner execution
    """
    if is_databricks():
        spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    else:
        # Local laptop or GitHub Actions CI runner
        builder = (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "4")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    return spark
