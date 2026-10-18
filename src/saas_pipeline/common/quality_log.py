from dataclasses import dataclass
from datetime import datetime, timezone

from omegaconf import DictConfig
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from saas_pipeline.common.config import get_target
from saas_pipeline.common.delta import write_delta


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


def evaluate_and_log_rules(
    df: DataFrame,
    rules: list[QualityRule],
    cfg: DictConfig,
    run_id: str,
    batch_id: str,
    tenant_id: str,
    layer: str,
    table_name: str,
    extra_entries: list[dict] | None = None,
) -> list[dict]:
    spark = df.sparkSession
    entries: list[dict] = list(extra_entries) if extra_entries else []

    if rules:
        agg_exprs = [F.count(F.lit(1)).alias("_total_records")]
        for idx, rule in enumerate(rules):
            fail_cond = F.when(rule.valid_condition.isNull() | (~rule.valid_condition), F.lit(1)).otherwise(F.lit(0))
            agg_exprs.append(F.sum(fail_cond).alias(f"_fail_{idx}"))

        metrics_row = df.agg(*agg_exprs).collect()[0]
        total_records = int(metrics_row["_total_records"] or 0)

        for idx, rule in enumerate(rules):
            failed_cnt = int(metrics_row[f"_fail_{idx}"] or 0)
            entries.append(
                build_quality_entry(
                    run_id=run_id,
                    batch_id=batch_id,
                    tenant_id=tenant_id,
                    layer=layer,
                    table_name=table_name,
                    check_name=rule.name,
                    check_severity=rule.severity,
                    records_checked=total_records,
                    records_failed=failed_cnt,
                )
            )

    if entries:
        log_df = spark.createDataFrame(entries, schema=QUALITY_LOGS_SCHEMA)
        qlogs_target = get_target(cfg, "quality_logs")
        write_delta(log_df, cfg, qlogs_target, mode="append", merge_schema=True)

    should_abort = bool(
        cfg.get("quality", {}).get("fail_on_critical", False) or cfg.get("execution", {}).get("fail_on_critical", False)
    )
    critical_failures = [e for e in entries if e["check_severity"] == "critical" and e["records_failed"] > 0]
    if should_abort and critical_failures:
        failed_names = ", ".join(f"{e['check_name']} ({e['records_failed']} failed)" for e in critical_failures)
        raise CriticalDQException(
            f"Critical quality check(s) failed on {layer}.{table_name} [tenant={tenant_id}]: {failed_names}"
        )

    return entries
