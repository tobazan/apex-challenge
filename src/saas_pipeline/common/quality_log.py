from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class CriticalDQException(RuntimeError):
    "To raise when fail_on_critical is enabled and a critical DQ check fails"


@dataclass(frozen=True)
class QualityRule:
    name: str
    severity: str  # "critical" | "warning" | "info"
    valid_condition: Column
    quarantine_reason: str | None = None


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


def evaluate_and_log_rules(
    df: DataFrame,
    rules: list[QualityRule],
    run_id: str,
    batch_id: str,
    tenant_id: str,
    layer: str,
    table_name: str,
    quality_logs_path: str,
    fail_on_critical: bool = False,
    extra_entries: list[dict] | None = None,
) -> list[dict]:
    """
    Evaluates declarative QualityRules in a single pass, writes results to quality_logs,
    and raises CriticalDQException if fail_on_critical is True and a critical check fails
    """
    spark = df.sparkSession
    agg_exprs = [F.count("*").alias("_total_rows")]
    for rule in rules:
        agg_exprs.append(F.sum(F.when(~rule.valid_condition, F.lit(1)).otherwise(F.lit(0))).alias(rule.name))

    metrics_row = df.agg(*agg_exprs).first()
    total_rows = int(metrics_row["_total_rows"] or 0)

    entries: list[dict] = list(extra_entries) if extra_entries else []
    failed_critical_checks: list[str] = []

    for rule in rules:
        failed_count = int(metrics_row[rule.name] or 0) if total_rows > 0 else 0
        entries.append(
            build_quality_entry(
                run_id=run_id,
                batch_id=batch_id,
                tenant_id=tenant_id,
                layer=layer,
                table_name=table_name,
                check_name=rule.name,
                check_severity=rule.severity,
                records_checked=total_rows,
                records_failed=failed_count,
            )
        )
        if rule.severity.lower() == "critical" and failed_count > 0:
            failed_critical_checks.append(f"{rule.name} (failed={failed_count})")

    write_quality_logs(spark, quality_logs_path, entries)

    if fail_on_critical and failed_critical_checks:
        raise CriticalDQException(
            f"Critical quality check(s) failed for tenant='{tenant_id}' on {layer}.{table_name}: "
            f"{', '.join(failed_critical_checks)}"
        )

    return entries
