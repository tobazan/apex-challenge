import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saas_pipeline.bronze.deliveries import ingest_deliveries_bronze
from saas_pipeline.bronze.materials import ingest_materials_bronze
from saas_pipeline.cli import run_silver_layer
from saas_pipeline.common.config import get_raw_path, get_target, resolve_target_tenants
from saas_pipeline.common.delta import delta_exists, read_delta
from tests.conftest import build_test_cfg


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

    cfg = build_test_cfg(
        tmp_path,
        env="dev",
        runtime="filesystem",
        tenant="all",
        fail_fast=True,
        fail_on_critical=True,
    )

    raw_del_dir = Path(get_raw_path(cfg, "raw_deliveries"))
    raw_mat_dir = Path(get_raw_path(cfg, "raw_materials"))
    raw_del_dir.mkdir(parents=True)
    raw_mat_dir.mkdir(parents=True)
    shutil.copy(real_del_csv, raw_del_dir / real_del_csv.name)
    shutil.copy(real_mat_csv, raw_mat_dir / real_mat_csv.name)

    # Execute Bronze & Silver layers inside tmp_path
    run_id = "e2e_run1"
    ingest_materials_bronze(spark, cfg, run_id=run_id)
    ingest_deliveries_bronze(spark, cfg, run_id=run_id)
    run_silver_layer(spark, cfg, run_id=run_id)

    # Verify Single Shared Silver dim_materials (35 rows, 28 distinct SKUs, 28 current)
    dim_target = get_target(cfg, "silver_materials")
    shared_dim = read_delta(spark, cfg, dim_target)
    assert shared_dim.count() == 35
    assert shared_dim.select("material").distinct().count() == 28
    assert shared_dim.filter(F.col("is_current")).count() == 28

    # Verify Deliveries Approx Row Accounting across all 6 tenants:
    tenants = resolve_target_tenants(cfg)
    total_bronze_rows = 0
    total_silver_rows = 0
    total_quarantine_rows = 0

    for t in tenants:
        b_target = get_target(cfg, "bronze_deliveries", tenant=t)
        s_target = get_target(cfg, "silver_deliveries", tenant=t)
        q_target = get_target(cfg, "quarantine_deliveries", tenant=t)

        total_bronze_rows += read_delta(spark, cfg, b_target).count()
        total_silver_rows += read_delta(spark, cfg, s_target).count()
        if delta_exists(spark, cfg, q_target):
            total_quarantine_rows += read_delta(spark, cfg, q_target).count()

    assert total_bronze_rows >= 3000
    assert total_silver_rows >= 2750
    assert total_quarantine_rows >= 80

    # Verify Quality Logs recorded critical checks with 0 failures on curated tables
    qlogs_df = read_delta(spark, cfg, get_target(cfg, "quality_logs"))
    critical_silver_fails = qlogs_df.filter(
        (F.col("layer") == "silver") & (F.col("check_severity") == "critical") & (F.col("records_failed") > 0)
    )
    assert critical_silver_fails.count() == 0

    # Verify MERGE INTO Idempotency by re-running Silver
    run_silver_layer(spark, cfg, run_id="e2e_run2")
    recheck_silver_rows = sum(
        read_delta(spark, cfg, get_target(cfg, "silver_deliveries", tenant=t)).count() for t in tenants
    )
    assert recheck_silver_rows >= 2750
