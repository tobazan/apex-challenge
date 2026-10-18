from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saas_pipeline.common.config import get_target
from saas_pipeline.common.delta import delta_exists, read_delta
from saas_pipeline.common.quality_log import (
    QUALITY_LOGS_SCHEMA,
    CriticalDQException,
    QualityRule,
    build_quality_entry,
    evaluate_and_log_rules,
)
from tests.conftest import build_test_cfg


def test_quality_engine_logs_and_warning(spark: SparkSession, tmp_path: Path):
    cfg = build_test_cfg(tmp_path, fail_on_critical=True)
    data = [
        ("SKU1", 10, "2025-03-15"),
        ("SKU2", 25, None),  # Fails warning rule (missing date), passes critical rule (qty > 0)
    ]
    df = spark.createDataFrame(data, ["material", "qty", "fecha"])

    rules = [
        QualityRule(
            name="positive_qty",
            severity="critical",
            valid_condition=F.col("qty") > 0,
        )
    ]

    rules = [
        QualityRule(
            name="critical_positive_qty",
            severity="critical",
            valid_condition=F.col("qty").isNotNull() & (F.col("qty") > 0),
        ),
        QualityRule(
            name="warning_fecha_present",
            severity="warning",
            valid_condition=F.col("fecha").isNotNull(),
        ),
    ]
    extra = [
        build_quality_entry(
            run_id="run_100",
            batch_id="batch_100",
            tenant_id="sv",
            layer="silver",
            table_name="fact_deliveries",
            check_name="pre_dedup_dropped",
            check_severity="warning",
            records_checked=5,
            records_failed=1,
        )
    ]

    entries = evaluate_and_log_rules(
        df=df,
        rules=rules,
        cfg=cfg,
        run_id="run_100",
        batch_id="batch_100",
        tenant_id="sv",
        layer="silver",
        table_name="fact_deliveries",
        extra_entries=extra,
    )

    assert len(entries) == 3
    qlogs_target = get_target(cfg, "quality_logs")
    assert delta_exists(spark, cfg, qlogs_target)

    logs_df = read_delta(spark, cfg, qlogs_target)
    assert logs_df.schema.fieldNames() == QUALITY_LOGS_SCHEMA.fieldNames()
    assert logs_df.count() == 3

    by_check = {r["check_name"]: r for r in logs_df.collect()}
    assert by_check["critical_positive_qty"]["check_passed"] is True
    assert by_check["critical_positive_qty"]["records_failed"] == 0

    assert by_check["warning_fecha_present"]["check_passed"] is False
    assert by_check["warning_fecha_present"]["records_failed"] == 1

    assert by_check["pre_dedup_dropped"]["check_passed"] is False
    assert by_check["pre_dedup_dropped"]["records_checked"] == 5


def test_quality_engine_aborts_on_critical_when_flag_enabled(spark: SparkSession, tmp_path: Path):
    cfg = build_test_cfg(tmp_path, fail_on_critical=True)
    df = spark.createDataFrame([("SKU1", 10), ("SKU2", -5), ("SKU3", None)], ["material", "qty"])

    rules = [
        QualityRule(
            name="critical_positive_qty",
            severity="critical",
            valid_condition=F.col("qty").isNotNull() & (F.col("qty") > 0),
        )
    ]

    with pytest.raises(CriticalDQException, match="critical_positive_qty"):
        evaluate_and_log_rules(
            df=df,
            rules=rules,
            cfg=cfg,
            run_id="run_crit",
            batch_id="batch_crit",
            tenant_id="sv",
            layer="silver",
            table_name="fact_deliveries",
        )

    # Audit log must still be persisted to Delta before raising
    logs_df = read_delta(spark, cfg, get_target(cfg, "quality_logs"))
    row = logs_df.first()
    assert row["check_passed"] is False
    assert row["records_checked"] == 3
    assert row["records_failed"] == 2
