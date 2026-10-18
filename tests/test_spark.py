from pyspark.sql import SparkSession

from saas_pipeline.common.spark import is_databricks


def test_spark_session_configuration(spark: SparkSession):
    assert not is_databricks()
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled") == "true"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "4"
