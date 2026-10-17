from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saas_pipeline.common.quality_log import (
    CriticalDQException,
    QualityRule,
    evaluate_and_log_rules,
)


def test_quality_engine(spark: SparkSession, tmp_path: Path):
    qlogs_path = str(tmp_path / "shared" / "quality_logs")
    df = spark.createDataFrame([("sv", 10), ("sv", -5)], ["tenant_id", "cantidad_normalizada_st"])

    rules = [
        QualityRule(
            name="silver_positive_qty",
            severity="critical",
            valid_condition=F.col("cantidad_normalizada_st") > 0,
        )
    ]

    # TEST 1 - With fail_on_critical=False, logs the failure without raising
    evaluate_and_log_rules(
        df=df,
        rules=rules,
        run_id="r1",
        batch_id="b1",
        tenant_id="sv",
        layer="silver",
        table_name="fact_deliveries",
        quality_logs_path=qlogs_path,
        fail_on_critical=False,
    )

    logged_df = spark.read.format("delta").load(qlogs_path)
    assert logged_df.count() == 1
    row = logged_df.first()
    assert row["check_passed"] is False
    assert row["records_checked"] == 2
    assert row["records_failed"] == 1

    # TEST 2 - With fail_on_critical=True, raises CriticalDQException
    with pytest.raises(CriticalDQException, match="silver_positive_qty"):
        evaluate_and_log_rules(
            df=df,
            rules=rules,
            run_id="r2",
            batch_id="b2",
            tenant_id="sv",
            layer="silver",
            table_name="fact_deliveries",
            quality_logs_path=qlogs_path,
            fail_on_critical=True,
        )
