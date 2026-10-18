from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saas_pipeline.bronze.deliveries import ingest_deliveries_bronze
from saas_pipeline.bronze.materials import ingest_materials_bronze
from saas_pipeline.common.config import get_raw_path, get_target
from saas_pipeline.common.delta import delta_exists, read_delta
from tests.conftest import build_test_cfg


def test_bronze_ingestion(spark: SparkSession, tmp_path: Path):
    cfg = build_test_cfg(tmp_path, env="dev", tenant="all")

    raw_mat_dir = Path(get_raw_path(cfg, "raw_materials"))
    raw_del_dir = Path(get_raw_path(cfg, "raw_deliveries"))
    raw_mat_dir.mkdir(parents=True)
    raw_del_dir.mkdir(parents=True)

    (raw_mat_dir / "materials.csv").write_text(
        "material,descripcion,categoria,precio_base,valid_from,valid_to,is_current\n"
        "SKU1,Cola,Soda,10.00,2025-01-01,9999-12-31,true\n"
        "SKU2,Water,Water,5.00,2025-01-01,9999-12-31,true\n",
        encoding="utf-8",
    )
    (raw_del_dir / "deliveries_part1.csv").write_text(
        "pais,fecha_proceso,transporte,ruta,tipo_entrega,material,precio,cantidad,unidad\n"
        "SV,20250315,T1,R1,ZPRE,SKU1,10.00,5,CS\n"
        "sv,20250316,T2,R1,ZVE1,SKU2,5.00,10,ST\n"
        "GT,20250316,T3,R2,Z04,SKU1,10.00,2,ST\n",
        encoding="utf-8",
    )

    # 1. First streaming ingestion run
    ingest_materials_bronze(spark, cfg, run_id="b_run1")
    ingest_deliveries_bronze(spark, cfg, run_id="b_run1")

    mat_target = get_target(cfg, "bronze_materials")
    sv_target = get_target(cfg, "bronze_deliveries", tenant="sv")
    gt_target = get_target(cfg, "bronze_deliveries", tenant="gt")
    ec_target = get_target(cfg, "bronze_deliveries", tenant="ec")

    assert delta_exists(spark, cfg, mat_target)
    assert delta_exists(spark, cfg, sv_target)
    assert delta_exists(spark, cfg, gt_target)
    assert not delta_exists(spark, cfg, ec_target)

    mat_df = read_delta(spark, cfg, mat_target)
    sv_df = read_delta(spark, cfg, sv_target)
    gt_df = read_delta(spark, cfg, gt_target)

    assert mat_df.count() == 2
    assert sv_df.count() == 2
    assert gt_df.count() == 1

    # Verify technical metadata columns
    expected_mat_meta = {"_ingestion_timestamp", "_source_file", "_batch_id"}
    assert expected_mat_meta.issubset(set(mat_df.columns))

    expected_del_meta = {"_ingestion_timestamp", "_source_file", "_tenant_id", "_batch_id"}
    assert expected_del_meta.issubset(set(sv_df.columns))
    assert sv_df.filter(F.col("_tenant_id") != "sv").count() == 0

    # 2. Streaming Checkpoint Idempotency: re-running over same files adds 0 rows
    ingest_materials_bronze(spark, cfg, run_id="b_run2")
    ingest_deliveries_bronze(spark, cfg, run_id="b_run2")

    assert read_delta(spark, cfg, mat_target).count() == 2
    assert read_delta(spark, cfg, sv_target).count() == 2
    assert read_delta(spark, cfg, gt_target).count() == 1

    # 3. Verify quality logs were recorded for Bronze
    qlogs_df = read_delta(spark, cfg, get_target(cfg, "quality_logs"))
    assert qlogs_df.filter(F.col("layer") == "bronze").count() >= 4
