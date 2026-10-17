from datetime import date
from decimal import Decimal
from pathlib import Path

from omegaconf import OmegaConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from saas_pipeline.silver.deliveries import split_and_enrich_deliveries
from saas_pipeline.silver.materials import transform_silver_materials

MAT_COLS = [
    "material",
    "descripcion",
    "categoria",
    "precio_base",
    "valid_from",
    "valid_to",
    "is_current",
    "_ingestion_timestamp",
    "_source_file",
    "_batch_id",
]


def build_test_cfg(tmp_path: Path):
    return OmegaConf.create(
        {
            "environment": "dev",
            "paths": {
                "raw_deliveries": str(tmp_path / "raw" / "deliveries"),
                "raw_materials": str(tmp_path / "raw" / "materials"),
                "bronze": str(tmp_path / "bronze"),
                "silver": str(tmp_path / "silver"),
                "quarantine_root": str(tmp_path),
                "quality_logs": str(tmp_path / "shared" / "quality_logs"),
                "checkpoints": str(tmp_path / "_checkpoints"),
            },
            "execution": {
                "start_date": "2025-01-01",
                "end_date": "2025-06-30",
                "tenant": "all",
                "fail_fast": False,
            },
            "quality": {
                "fail_on_critical": False,
            },
            "registered_tenants": ["sv", "hn", "ec", "jm", "gt", "pe"],
            "rules": {
                "cs_to_st_factor": 20,
                "routine_delivery_types": ["ZPRE", "ZVE1"],
                "bonus_delivery_types": ["Z04", "Z05"],
            },
        }
    )


def string_schema(cols: list[str]) -> StructType:
    """Bronze is all-string: an explicit schema avoids inference failures on all-None columns."""
    return StructType([StructField(c, StringType(), True) for c in cols])


def test_scd2_materials(spark: SparkSession):
    raw_mat_data = [
        # Two historical versions of SKU1: both marked 'true' in raw to verify Silver fixes is_current
        ("SKU1", "Cola v1", "Soda", "10.00", "2024-01-01", "2025-03-31", "true", None, "f1", "b1"),
        ("SKU1", "Cola v2", "Soda", "12.50", "2025-04-01", "9999-12-31", "true", None, "f1", "b1"),
    ]
    bronze_mat_df = spark.createDataFrame(raw_mat_data, string_schema(MAT_COLS))
    silver_mat_df, _ = transform_silver_materials(bronze_mat_df)

    rows = {r["valid_from"]: r for r in silver_mat_df.collect()}
    assert len(rows) == 2
    assert rows[date(2024, 1, 1)]["is_current"] is False
    assert rows[date(2025, 4, 1)]["is_current"] is True
    assert rows[date(2025, 4, 1)]["precio_base"] == Decimal("12.50")


def test_silver_deliveries(spark: SparkSession, tmp_path: Path):
    cfg = build_test_cfg(tmp_path)

    raw_mat_data = [
        # Valid historical version 1
        ("SKU1", "Cola v1", "Soda", "10.00", "2024-01-01", "2025-03-31", "true", None, "f1", "b1"),
        # Valid historical version 2 (should become the ONLY is_current=True)
        ("SKU1", "Cola v2", "Soda", "12.50", "2025-04-01", "9999-12-31", "true", None, "f1", "b1"),
        # Quarantine: Missing SKU
        ("", "Juice", "Beverages", "8.00", "2025-01-01", "9999-12-31", "true", None, "f1", "b1"),
        # Quarantine: Negative price
        ("SKU2", "Water", "Water", "-5.00", "2025-01-01", "9999-12-31", "true", None, "f1", "b1"),
        # Quarantine: Invalid date interval (valid_from > valid_to)
        ("SKU3", "Beer", "Alcohol", "15.00", "2025-05-01", "2025-01-01", "true", None, "f1", "b1"),
    ]
    bronze_mat_df = spark.createDataFrame(raw_mat_data, string_schema(MAT_COLS))
    dim_mat_df, mat_quarantine_df = transform_silver_materials(bronze_mat_df)

    # Materials: quarantine routing
    assert mat_quarantine_df.count() == 3
    mat_q_reasons = {r["descripcion"]: r["_quarantine_reason"] for r in mat_quarantine_df.collect()}
    assert mat_q_reasons["Juice"] == "NULL_SKU"
    assert mat_q_reasons["Water"] == "INVALID_BASE_PRICE"
    assert mat_q_reasons["Beer"] == "INVALID_DATE_INTERVAL"

    # Materials: valid SCD2 logic
    mat_rows = {r["valid_from"]: r for r in dim_mat_df.collect()}
    assert len(mat_rows) == 2
    assert mat_rows[date(2024, 1, 1)]["is_current"] is False
    assert mat_rows[date(2025, 4, 1)]["is_current"] is True
    assert mat_rows[date(2025, 4, 1)]["precio_base"] == Decimal("12.50")

    deliv_cols = [
        "pais",
        "fecha_proceso",
        "transporte",
        "ruta",
        "tipo_entrega",
        "material",
        "precio",
        "cantidad",
        "unidad",
        "_ingestion_timestamp",
        "_source_file",
        "_tenant_id",
        "_batch_id",
    ]
    deliv_data = [
        # 1. Valid routine delivery in March (matches SKU1 v1 @ 10.00, 2 CS -> 40 ST)
        ("sv", "20250315", "101", "201", "ZPRE", "SKU1", "11.00", "2", "CS", None, "f", "sv", "b1"),
        # 2. Exact duplicate of row 1 (should be deduplicated silently)
        ("sv", "20250315", "101", "201", "ZPRE", "SKU1", "11.00", "2", "CS", None, "f", "sv", "b1"),
        # 3. Valid bonus delivery in April (matches SKU1 v2 @ 12.50, 5 ST -> 5 ST)
        ("sv", "20250410", "101", "201", "Z04", "SKU1", "13.00", "5", "ST", None, "f", "sv", "b1"),
        # 4. Out-of-scope tipo_entrega COBR (should be discarded, not quarantined)
        ("sv", "20250315", "101", "201", "COBR", "SKU1", "11.00", "1", "ST", None, "f", "sv", "b1"),
        # 5. Anomaly: Invalid quantity (0) -> Quarantine
        ("sv", "20250315", "102", "202", "ZVE1", "SKU1", "11.00", "0", "ST", None, "f", "sv", "b1"),
        # 6. Anomaly: Unmapped material XX999 -> Quarantine
        ("sv", "20250315", "103", "203", "Z05", "XX999", "11.00", "3", "ST", None, "f", "sv", "b1"),
        # 7. Anomaly: Invalid date 20251332 -> Quarantine
        ("sv", "20251332", "104", "204", "ZPRE", "SKU1", "11.00", "3", "ST", None, "f", "sv", "b1"),
    ]
    bronze_del_df = spark.createDataFrame(deliv_data, string_schema(deliv_cols))

    valid_df, quarantine_df, dup_dropped, discarded_cnt = split_and_enrich_deliveries(bronze_del_df, dim_mat_df, cfg)

    assert dup_dropped == 1
    assert discarded_cnt == 1
    assert valid_df.count() == 2
    assert quarantine_df.count() == 3

    # Deliveries: temporal SCD2 join & unit conversion
    valid_rows = {r["fecha_proceso"]: r for r in valid_df.collect()}
    march_row = valid_rows[date(2025, 3, 15)]
    assert march_row["cantidad_normalizada_st"] == Decimal("40.00")
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

    # Deliveries: quarantine reasons
    q_reasons = {r["transporte"]: r["_quarantine_reason"] for r in quarantine_df.collect()}
    assert q_reasons["102"] == "INVALID_CANTIDAD"
    assert q_reasons["103"] == "UNMAPPED_MATERIAL"
    assert q_reasons["104"] == "INVALID_FECHA_PROCESO"
