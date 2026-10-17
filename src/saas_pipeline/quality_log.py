from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

QUALITY_LOGS_SCHEMA = StructType(
    [
        StructField("_run_id", StringType(), False),
        StructField("_batch_id", StringType(), False),
        StructField("tenant_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_severity", StringType(), False),
        StructField("records_checked", LongType(), False),
        StructField("records_failed", LongType(), False),
        StructField("check_passed", BooleanType(), False),
        StructField("executed_at", TimestampType(), False),
    ]
)


def build_quality_entry(
    run_id: str,
    batch_id: str,
    tenant_id: str,
    layer: str,
    table_name: str,
    check_name: str,
    check_severity: str,
    records_checked: int,
    records_failed: int,
) -> dict:
    "Builds a single quality check log dict"
    return {
        "_run_id": str(run_id),
        "_batch_id": str(batch_id),
        "tenant_id": str(tenant_id).lower(),
        "layer": str(layer).lower(),
        "table_name": str(table_name).lower(),
        "check_name": str(check_name),
        "check_severity": str(check_severity).lower(),
        "records_checked": int(records_checked),
        "records_failed": int(records_failed),
        "check_passed": int(records_failed) == 0,
        "executed_at": datetime.now(timezone.utc),
    }


def write_quality_logs(spark: SparkSession, quality_logs_path: str, entries: list[dict]):
    "Log executed validations to shared Delta quality_logs table"
    if not entries:
        return
    logs_df = spark.createDataFrame(entries, schema=QUALITY_LOGS_SCHEMA)
    logs_df.write.format("delta").mode("append").save(quality_logs_path)
