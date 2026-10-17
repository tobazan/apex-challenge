from pathlib import Path

from omegaconf import OmegaConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saas_pipeline.bronze_deliveries import ingest_deliveries_bronze
from saas_pipeline.bronze_materials import ingest_materials_bronze


def build_test_cfg(tmp_path: Path):
    return OmegaConf.create(
        {
            "environment": "dev",
            "paths": {
                "raw_deliveries": str(tmp_path / "raw" / "deliveries"),
                "raw_materials": str(tmp_path / "raw" / "materials"),
                "bronze": str(tmp_path / "bronze"),
                "quality_logs": str(tmp_path / "shared" / "quality_logs"),
                "checkpoints": str(tmp_path / "_checkpoints"),
            },
            "execution": {
                "start_date": "2025-01-01",
                "end_date": "2025-06-30",
                "tenant": "all",
                "fail_fast": False,
            },
        }
    )


def test_bronze_ingestion(spark: SparkSession, tmp_path: Path):
    cfg = build_test_cfg(tmp_path)
    raw_del_dir = Path(cfg.paths.raw_deliveries)
    raw_mat_dir = Path(cfg.paths.raw_materials)
    raw_del_dir.mkdir(parents=True)
    raw_mat_dir.mkdir(parents=True)

    # TEST 1 - Mock raw CSV (Batch 1 --> clean records)
    (raw_del_dir / "batch1.csv").write_text(
        "pais,fecha_proceso,transporte,ruta,tipo_entrega,material,precio,cantidad,unidad\n"
        "SV,20250314,1001,2001,ZPRE,MAT01,10.50,5,CS\n"
        "GT,20250314,1002,2002,Z04,MAT02,8.00,10,ST\n",
        encoding="utf-8",
    )
    (raw_mat_dir / "materials.csv").write_text(
        "material,descripcion,categoria,precio_base,valid_from,valid_to,is_current\n"
        "MAT01,Bebida Cola,Gaseosas,10.00,2025-01-01,9999-12-31,true\n"
        ",Jugo Naranja,Jugos,5.00,2025-01-01,9999-12-31,true\n",
        encoding="utf-8",
    )

    run_id_1 = "run00001"
    ingest_deliveries_bronze(spark, cfg, run_id=run_id_1)

    # Assert SV Bronze deliveries
    sv_df = spark.read.format("delta").load(f"{cfg.paths.bronze}/sv/deliveries")
    assert sv_df.count() == 1
    row = sv_df.first()
    assert row["pais"] == "sv"
    assert row["_tenant_id"] == "sv"
    assert row["fecha_proceso"] == "20250314"
    assert row["_batch_id"].startswith(run_id_1)
    assert row["_source_file"] is not None
    assert row["_ingestion_timestamp"] is not None

    # Verify Bronze all StringType schema
    business_cols = [
        "pais",
        "fecha_proceso",
        "transporte",
        "ruta",
        "tipo_entrega",
        "material",
        "precio",
        "cantidad",
        "unidad",
    ]
    dtypes_map = dict(sv_df.dtypes)
    for col_name in business_cols:
        assert dtypes_map[col_name] == "string"

    # TEST 2 - Schema Evolution + Partition Idempotency + Missing Partition Key
    # Mock CSV file with an extra col 'canal_venta', overwriting 20250314 for SV + adding a missing date row
    (raw_del_dir / "batch2.csv").write_text(
        "pais,fecha_proceso,transporte,ruta,tipo_entrega,material,precio,cantidad,unidad,canal_venta\n"
        "SV,20250314,1001,2001,ZPRE,MAT01,10.50,5,CS,MODERNO\n"
        "SV,,1003,2003,ZVE1,MAT01,10.50,2,ST,TRADICIONAL\n",
        encoding="utf-8",
    )

    run_id_2 = "run_del_02"
    ingest_deliveries_bronze(spark, cfg, run_id=run_id_2)

    sv_evolved_df = spark.read.format("delta").load(f"{cfg.paths.bronze}/sv/deliveries")
    # Partition 20250314 was overwritten idempotently + 1 row in UNKNOWN_DATE = 2 total rows
    assert sv_evolved_df.count() == 2
    assert "canal_venta" in sv_evolved_df.columns
    assert dict(sv_evolved_df.dtypes)["canal_venta"] == "string"

    # TEST 3 - Verify Quality Logs recorded properly across runs
    qlogs_df = spark.read.format("delta").load(cfg.paths.quality_logs)
    assert qlogs_df.count() >= 5

    # In run00001, SV partition key check should pass
    run1_sv_check = qlogs_df.filter(
        (F.col("_run_id") == run_id_1)
        & (F.col("tenant_id") == "sv")
        & (F.col("check_name") == "bronze_partition_key_present")
    ).first()
    assert run1_sv_check["check_passed"] is True
    assert run1_sv_check["records_failed"] == 0

    # In run00002, SV had 1 row with missing fecha_proceso, shouldn't pass
    run2_sv_check = qlogs_df.filter(
        (F.col("_run_id") == run_id_2)
        & (F.col("tenant_id") == "sv")
        & (F.col("check_name") == "bronze_partition_key_present")
    ).first()
    assert run2_sv_check["check_passed"] is False
    assert run2_sv_check["records_failed"] == 1

    # TEST 4 - Assert SV Bronze materials
    run_id_1 = "run00001"
    ingest_materials_bronze(spark, cfg, run_id=run_id_1)

    mat_df = spark.read.format("delta").load(f"{cfg.paths.bronze}/shared/materials_catalog")
    assert mat_df.count() == 2
    assert "_ingestion_timestamp" in mat_df.columns
    assert "_source_file" in mat_df.columns
    assert "_tenant_id" in mat_df.columns
    assert "_batch_id" in mat_df.columns

    dtypes = dict(mat_df.dtypes)
    for col_name in ["material", "descripcion", "categoria", "precio_base", "valid_from", "valid_to", "is_current"]:
        assert dtypes[col_name] == "string"

    # TEST 5 - Verify Quality Logs recorded properly
    qlogs_df = spark.read.format("delta").load(cfg.paths.quality_logs)
    mat_checks = qlogs_df.filter(
        (F.col("_run_id") == run_id_1) & (F.col("table_name") == "materials_catalog") & (F.col("tenant_id") == "shared")
    )
    assert mat_checks.count() == 2

    # Check that 1 missing SKU row was caught as failed
    sku_check = mat_checks.filter(F.col("check_name") == "bronze_material_key_present").first()
    assert sku_check["check_passed"] is False
    assert sku_check["records_checked"] == 2
    assert sku_check["records_failed"] == 1
