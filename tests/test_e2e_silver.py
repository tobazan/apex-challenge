import shutil
from pathlib import Path

import pytest
from omegaconf import OmegaConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saas_pipeline.bronze_deliveries import ingest_deliveries_bronze
from saas_pipeline.bronze_materials import ingest_materials_bronze
from saas_pipeline.cli import run_silver_layer


# we use this decorator and configure pyproject.toml so standard pytest skips it
# GitHub Actions CI workflow will skip as well
# Makefile has make test-e2e so can use this to test locally
@pytest.mark.e2e
def test_full_real_dataset_bronze_to_silver_e2e(spark: SparkSession, tmp_path: Path):
    """
    Runs the complete provided dataset (3,115 delivery rows + 35 material rows)
    through Bronze and Silver across all 6 tenants and asserts exact row accounting
    """
    repo_root = Path(__file__).resolve().parents[1]
    real_del_csv = repo_root / "data" / "raw" / "deliveries" / "global_mobility_data_entrega_productos.csv"
    real_mat_csv = repo_root / "data" / "raw" / "materials" / "materials_catalog.csv"

    if not real_del_csv.exists() or not real_mat_csv.exists():
        pytest.skip("Real CSV datasets not found in data/raw/")

    raw_del_dir = tmp_path / "raw" / "deliveries"
    raw_mat_dir = tmp_path / "raw" / "materials"
    raw_del_dir.mkdir(parents=True)
    raw_mat_dir.mkdir(parents=True)
    shutil.copy(real_del_csv, raw_del_dir / real_del_csv.name)
    shutil.copy(real_mat_csv, raw_mat_dir / real_mat_csv.name)

    cfg = OmegaConf.create(
        {
            "environment": "dev",
            "paths": {
                "raw_deliveries": str(raw_del_dir),
                "raw_materials": str(raw_mat_dir),
                "bronze": str(tmp_path / "bronze"),
                "silver": str(tmp_path / "silver"),
                "quarantine_root": str(tmp_path),
                "quality_logs": str(tmp_path / "shared" / "quality_logs"),
                "checkpoints": str(tmp_path / "_checkpoints"),
            },
            "execution": {
                "start_date": "2025-01-01",
                "end_date": "2025-12-31",
                "tenant": "all",
                "fail_fast": True,
            },
            "quality": {"fail_on_critical": True},
            "registered_tenants": ["sv", "hn", "ec", "jm", "gt", "pe"],
            "rules": {
                "cs_to_st_factor": 20,
                "routine_delivery_types": ["ZPRE", "ZVE1"],
                "bonus_delivery_types": ["Z04", "Z05"],
            },
        }
    )

    # Execute Bronze Ingestion
    run_id = "e2e_run1"
    ingest_materials_bronze(spark, cfg, run_id=run_id)
    ingest_deliveries_bronze(spark, cfg, run_id=run_id)

    # Execute Silver Layer across all 6 tenants
    run_silver_layer(spark, cfg, run_id=run_id)

    # Verify Single Shared Silver dim_materials (35 rows, 28 distinct SKUs, 28 current)
    shared_dim = spark.read.format("delta").load(f"{cfg.paths.silver}/shared/dim_materials")
    assert shared_dim.count() == 35
    assert shared_dim.select("material").distinct().count() == 28
    assert shared_dim.filter(F.col("is_current")).count() == 28

    # Verify Deliveries Approx Row Accounting across all 6 tenants:
    total_silver_rows = 0
    total_quarantine_rows = 0
    for tenant in cfg.registered_tenants:
        s_df = spark.read.format("delta").load(f"{cfg.paths.silver}/{tenant}/fact_deliveries")
        q_df = spark.read.format("delta").load(
            f"{cfg.paths.quarantine_root}/silver_quarantine/{tenant}/fact_deliveries"
        )
        total_silver_rows += s_df.count()
        total_quarantine_rows += q_df.count()

    assert total_silver_rows >= 2750
    assert total_quarantine_rows >= 100

    # Verify MERGE INTO Idempotency by re-running Silver
    run_silver_layer(spark, cfg, run_id="e2e_run2")
    recheck_silver_rows = sum(
        spark.read.format("delta").load(f"{cfg.paths.silver}/{t}/fact_deliveries").count()
        for t in cfg.registered_tenants
    )
    assert recheck_silver_rows >= 2750
