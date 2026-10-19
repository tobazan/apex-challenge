from datetime import date
from decimal import Decimal
from pathlib import Path

from conftest import build_test_cfg
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from saas_pipeline.common.config import get_target
from saas_pipeline.common.delta import delta_exists, read_delta, write_delta
from saas_pipeline.gold.daily_metrics_by_delivery_type import daily_metrics_by_delivery_type

SILVER_GOLD_TEST_SCHEMA = StructType(
    [
        StructField("tenant_id", StringType(), False),
        StructField("pais", StringType(), False),
        StructField("fecha_proceso", DateType(), False),
        StructField("transporte", StringType(), False),
        StructField("ruta", StringType(), False),
        StructField("tipo_entrega", StringType(), False),
        StructField("material", StringType(), False),
        StructField("material_descripcion", StringType(), True),
        StructField("material_categoria", StringType(), True),
        StructField("material_precio_base", DecimalType(10, 2), True),
        StructField("precio", DecimalType(10, 2), True),
        StructField("cantidad_original", DecimalType(10, 2), True),
        StructField("unidad_original", StringType(), True),
        StructField("cantidad_normalizada_st", DecimalType(10, 2), True),
        StructField("unidad_normalizada", StringType(), True),
        StructField("is_routine_delivery", BooleanType(), True),
        StructField("is_bonus_delivery", BooleanType(), True),
        StructField("_ingestion_timestamp", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_batch_id", StringType(), True),
    ]
)


def test_daily_by_delivery_type(spark: SparkSession, tmp_path: Path):
    cfg = build_test_cfg(tmp_path)
    tenant = "sv"
    silver_target = get_target(cfg, "silver_deliveries", tenant=tenant)

    # Seed mock Silver fact_deliveries
    silver_data = [
        (
            "sv",
            "sv",
            date(2025, 3, 15),
            "T1",
            "R1",
            "ZPRE",
            "SKU1",
            "Cola v1",
            "Soda",
            Decimal("10.00"),
            Decimal("12.50"),
            Decimal("2.00"),
            "CS",
            Decimal("40.00"),
            "ST",
            True,
            False,
            None,
            "f1",
            "b1",
        ),
        (
            "sv",
            "sv",
            date(2025, 3, 15),
            "T2",
            "R2",
            "ZPRE",
            "SKU1",
            "Cola v1",
            "Soda",
            Decimal("10.00"),
            Decimal("12.50"),
            Decimal("3.00"),
            "ST",
            Decimal("3.00"),
            "ST",
            True,
            False,
            None,
            "f1",
            "b1",
        ),
        (
            "sv",
            "sv",
            date(2025, 3, 15),
            "T1",
            "R1",
            "ZVE1",
            "SKU2",
            "Water",
            "Water",
            Decimal("5.00"),
            Decimal("6.00"),
            Decimal("10.00"),
            "ST",
            Decimal("10.00"),
            "ST",
            True,
            False,
            None,
            "f1",
            "b1",
        ),
    ]

    silver_df = spark.createDataFrame(silver_data, schema=SILVER_GOLD_TEST_SCHEMA)
    write_delta(silver_df, cfg, silver_target, mode="overwrite")

    # Run Gold processing
    run_id = daily_metrics_by_delivery_type(spark, tenant, cfg, run_id="gold_test")

    # Verify Gold Aggregations
    gold_target = get_target(cfg, "gold_daily_metrics", tenant=tenant)
    gold_df = read_delta(spark, cfg, gold_target)
    df_filtered = gold_df.filter(F.col("fecha_proceso") == "2025-03-15")
    rows = {r["tipo_entrega"]: r for r in df_filtered.collect()}
    zpre = rows["ZPRE"]
    assert zpre["total_units"] == Decimal("43.00")  # 40.00 + 3.00
    assert zpre["total_revenue"] == Decimal("537.50")  # Revenue: (40.00 * 12.50) + (3.00 * 12.50)
    assert zpre["active_routes"] == 2  # R1 and R2
    assert zpre["active_transports"] == 2  # T1 and T2

    # Verify Quality Logs Generation
    qlogs_target = get_target(cfg, "quality_logs")
    assert delta_exists(spark, cfg, qlogs_target)

    qlogs_df = read_delta(spark, cfg, qlogs_target)
    gold_logs = qlogs_df.filter(
        (F.col("layer") == "gold") & (F.col("_run_id") == run_id) & (F.col("tenant_id") == tenant)
    )

    assert gold_logs.count() == 2
    checks = {r["check_name"]: r for r in gold_logs.collect()}

    assert "gold_metrics_non_empty" in checks
    assert checks["gold_metrics_non_empty"]["check_passed"] is True
    assert checks["gold_metrics_non_empty"]["records_failed"] == 0

    assert "gold_metrics_non_negative_units" in checks
    assert checks["gold_metrics_non_negative_units"]["check_passed"] is True
    assert checks["gold_metrics_non_negative_units"]["records_failed"] == 0
