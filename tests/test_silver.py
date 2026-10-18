from datetime import date
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from saas_pipeline.bronze.deliveries import DELIVERIES_RAW_SCHEMA
from saas_pipeline.bronze.materials import MATERIALS_RAW_SCHEMA
from saas_pipeline.common.config import get_target
from saas_pipeline.common.delta import delta_exists, read_delta, write_delta
from saas_pipeline.silver.deliveries import (
    process_silver_deliveries_for_tenant,
)
from saas_pipeline.silver.materials import (
    merge_dim_materials,
    transform_silver_materials,
)
from tests.conftest import build_test_cfg

BRONZE_MATERIALS_SCHEMA = StructType(
    MATERIALS_RAW_SCHEMA.fields
    + [
        StructField("_ingestion_timestamp", TimestampType(), nullable=True),
        StructField("_source_file", StringType(), nullable=True),
        StructField("_batch_id", StringType(), nullable=True),
    ]
)

BRONZE_DELIVERIES_SCHEMA = StructType(
    DELIVERIES_RAW_SCHEMA.fields
    + [
        StructField("_ingestion_timestamp", TimestampType(), nullable=True),
        StructField("_source_file", StringType(), nullable=True),
        StructField("_tenant_id", StringType(), nullable=True),
        StructField("_batch_id", StringType(), nullable=True),
    ]
)


def test_merge_dim_materials_scd2_and_quarantine(spark: SparkSession, tmp_path: Path):
    cfg = build_test_cfg(tmp_path)

    # Batch 1: 2 valid SKUs + 2 corrupt records (negative price & inverted dates)
    batch1_data = [
        ("SKU1", "Cola v1", "Soda", "10.00", "2024-01-01", "9999-12-31", "true", None, "f1", "b1"),
        ("SKU2", "Water v1", "Water", "5.00", "2024-01-01", "9999-12-31", "true", None, "f1", "b1"),
        ("SKU_BAD1", "Bad Price", "Soda", "-1.00", "2024-01-01", "9999-12-31", "true", None, "f1", "b1"),
        ("SKU_BAD2", "Bad Dates", "Soda", "8.00", "2025-06-01", "2025-01-01", "true", None, "f1", "b1"),
    ]
    df1 = spark.createDataFrame(batch1_data, schema=BRONZE_MATERIALS_SCHEMA)
    valid_df1, q_df1 = transform_silver_materials(df1)
    merge_dim_materials(spark, valid_df1, q_df1, cfg, run_id="r1")

    dim_target = get_target(cfg, "silver_materials")
    q_target = get_target(cfg, "quarantine_materials")

    assert delta_exists(spark, cfg, dim_target)
    assert delta_exists(spark, cfg, q_target)

    dim_df = read_delta(spark, cfg, dim_target)
    q_df = read_delta(spark, cfg, q_target)

    assert dim_df.count() == 2
    assert q_df.count() == 2

    q_reasons = {r["material"]: r["_quarantine_reason"] for r in q_df.collect()}
    assert q_reasons["SKU_BAD1"] == "INVALID_BASE_PRICE"
    assert q_reasons["SKU_BAD2"] == "INVALID_DATE_INTERVAL"

    # Batch 2: SKU1 receives a closed historical window + a new open version (SCD2 update)
    batch2_data = [
        ("SKU1", "Cola v1", "Soda", "10.00", "2024-01-01", "2025-03-31", "false", None, "f2", "b2"),
        ("SKU1", "Cola v2", "Soda", "12.50", "2025-04-01", "9999-12-31", "true", None, "f2", "b2"),
    ]
    df2 = spark.createDataFrame(batch2_data, schema=BRONZE_MATERIALS_SCHEMA)
    valid_df2, q_df2 = transform_silver_materials(df2)
    merge_dim_materials(spark, valid_df2, q_df2, cfg, run_id="r2")

    merged_dim = read_delta(spark, cfg, dim_target)
    assert merged_dim.count() == 3  # 2 versions of SKU1 + 1 version of SKU2
    assert merged_dim.filter(F.col("is_current") == True).count() == 2  # noqa: E712

    sku1_rows = {r["valid_from"]: r for r in merged_dim.filter(F.col("material") == "SKU1").collect()}
    assert sku1_rows[date(2024, 1, 1)]["is_current"] is False
    assert sku1_rows[date(2024, 1, 1)]["valid_to"] == date(2025, 3, 31)
    assert sku1_rows[date(2025, 4, 1)]["is_current"] is True
    assert sku1_rows[date(2025, 4, 1)]["precio_base"] == Decimal("12.50")

    # Verify quality_logs recorded under tenant_id='shared'
    qlogs_df = read_delta(spark, cfg, get_target(cfg, "quality_logs"))
    shared_logs = qlogs_df.filter((F.col("table_name") == "dim_materials") & (F.col("tenant_id") == "shared"))
    assert shared_logs.count() == 6  # 3 checks * 2 runs


def test_silver_deliveries_transformation(spark: SparkSession, tmp_path: Path):
    cfg = build_test_cfg(tmp_path)

    # Seed shared Silver dim_materials
    raw_mat_data = [
        ("SKU1", "Cola v1", "Soda", "10.00", "2024-01-01", "2025-03-31", "false", None, "f1", "b1"),
        ("SKU1", "Cola v2", "Soda", "12.50", "2025-04-01", "9999-12-31", "true", None, "f1", "b1"),
    ]
    valid_mat, q_mat = transform_silver_materials(spark.createDataFrame(raw_mat_data, schema=BRONZE_MATERIALS_SCHEMA))
    merge_dim_materials(spark, valid_mat, q_mat, cfg, run_id="seed_mat")

    # Seed Bronze deliveries for tenant 'sv'
    deliv_data = [
        # 1. Valid routine delivery in March (matches SKU1 v1 @ 10.00, 2 CS -> 40 ST)
        ("sv", "20250315", "101", "201", "ZPRE", "SKU1", "11.00", "2", "CS", None, "f", "sv", "b1"),
        # 2. Exact duplicate of row 1 (dropped silently)
        ("sv", "20250315", "101", "201", "ZPRE", "SKU1", "11.00", "2", "CS", None, "f", "sv", "b1"),
        # 3. Valid bonus delivery in April (matches SKU1 v2 @ 12.50, 5 ST -> 5 ST)
        ("sv", "20250410", "101", "201", "Z04", "SKU1", "13.00", "5", "ST", None, "f", "sv", "b1"),
        # 4. Out-of-scope tipo_entrega COBR (discarded, not quarantined)
        ("sv", "20250315", "101", "201", "COBR", "SKU1", "11.00", "1", "ST", None, "f", "sv", "b1"),
        # 5. Out-of-scope tipo_entrega Z99 (discarded, not quarantined)
        ("sv", "20250315", "101", "201", "Z99", "SKU1", "11.00", "1", "ST", None, "f", "sv", "b1"),
        # 6. Quarantine: Invalid quantity (0)
        ("sv", "20250315", "102", "202", "ZVE1", "SKU1", "11.00", "0", "ST", None, "f", "sv", "b1"),
        # 7. Quarantine: Unmapped material XX999
        ("sv", "20250315", "103", "203", "Z05", "XX999", "11.00", "3", "ST", None, "f", "sv", "b1"),
        # 8. Quarantine: Invalid date 20251332
        ("sv", "20251332", "104", "204", "ZPRE", "SKU1", "11.00", "3", "ST", None, "f", "sv", "b1"),
        # 9. Quarantine: Date outside SKU1 validity window (2023-06-01 < 2024-01-01)
        ("sv", "20230601", "105", "205", "ZPRE", "SKU1", "11.00", "3", "ST", None, "f", "sv", "b1"),
    ]

    bronze_del_df = spark.createDataFrame(deliv_data, BRONZE_DELIVERIES_SCHEMA)
    write_delta(bronze_del_df, cfg, get_target(cfg, "bronze_deliveries", tenant="sv"), mode="overwrite")

    # Execute full tenant Silver processing
    process_silver_deliveries_for_tenant(spark, "sv", cfg, run_id="s_run1")

    silver_target = get_target(cfg, "silver_deliveries", tenant="sv")
    quarantine_target = get_target(cfg, "quarantine_deliveries", tenant="sv")

    assert delta_exists(spark, cfg, silver_target)
    assert delta_exists(spark, cfg, quarantine_target)

    valid_df = read_delta(spark, cfg, silver_target)
    quarantine_df = read_delta(spark, cfg, quarantine_target)

    assert valid_df.count() == 2
    assert quarantine_df.count() == 4

    # Verify temporal SCD2 enrichment & unit normalization
    valid_rows = {r["fecha_proceso"]: r for r in valid_df.collect()}
    march_row = valid_rows[date(2025, 3, 15)]
    assert march_row["cantidad_normalizada_st"] == Decimal("40.00")
    assert march_row["unidad_normalizada"] == "ST"
    assert march_row["is_routine_delivery"] is True
    assert march_row["is_bonus_delivery"] is False
    assert march_row["material_descripcion"] == "Cola v1"
    assert march_row["material_precio_base"] == Decimal("10.00")

    april_row = valid_rows[date(2025, 4, 10)]
    assert april_row["cantidad_normalizada_st"] == Decimal("5.00")
    assert april_row["is_routine_delivery"] is False
    assert april_row["is_bonus_delivery"] is True
    assert april_row["material_descripcion"] == "Cola v2"
    assert april_row["material_precio_base"] == Decimal("12.50")

    # Verify quarantine reasons
    q_reasons = {r["transporte"]: r["_quarantine_reason"] for r in quarantine_df.collect()}
    assert q_reasons["102"] == "INVALID_CANTIDAD"
    assert q_reasons["103"] == "UNMAPPED_MATERIAL"
    assert q_reasons["104"] == "INVALID_FECHA_PROCESO"
    assert q_reasons["105"] == "UNMAPPED_MATERIAL"

    # Idempotency: re-running Silver does not duplicate rows
    process_silver_deliveries_for_tenant(spark, "sv", cfg, run_id="s_run2")
    assert read_delta(spark, cfg, silver_target).count() == 2
