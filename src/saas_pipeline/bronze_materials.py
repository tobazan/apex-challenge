import uuid
from pathlib import Path

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from saas_pipeline.bronze_deliveries import discover_all_string_schema
from saas_pipeline.quality_log import build_quality_entry, write_quality_logs

KNOWN_MATERIALS_COLS = [
    "material",
    "descripcion",
    "categoria",
    "precio_base",
    "valid_from",
    "valid_to",
    "is_current",
]


def _process_materials_microbatch(batch_df: DataFrame, batch_id: int, cfg: DictConfig, run_id: str):
    """Writes raw materials catalog to Bronze Delta with mergeSchema and logs quality"""
    spark = batch_df.sparkSession
    batch_uid = f"{run_id}_b{batch_id}"

    if batch_df.isEmpty():
        return

    staged_df = (
        batch_df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_tenant_id", F.lit("shared"))
        .withColumn("_batch_id", F.lit(batch_uid))
    )

    total_rows = staged_df.count()
    missing_sku_rows = staged_df.filter(F.col("material").isNull() | (F.trim(F.col("material")) == "")).count()

    target_path = f"{cfg.paths.bronze}/shared/materials_catalog"
    (staged_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(target_path))

    quality_entries = [
        build_quality_entry(
            run_id=run_id,
            batch_id=batch_uid,
            tenant_id="shared",
            layer="bronze",
            table_name="materials_catalog",
            check_name="bronze_non_empty_batch",
            check_severity="info",
            records_checked=total_rows,
            records_failed=0 if total_rows > 0 else 1,
        ),
        build_quality_entry(
            run_id=run_id,
            batch_id=batch_uid,
            tenant_id="shared",
            layer="bronze",
            table_name="materials_catalog",
            check_name="bronze_material_key_present",
            check_severity="warning",
            records_checked=total_rows,
            records_failed=missing_sku_rows,
        ),
    ]
    write_quality_logs(spark, cfg.paths.quality_logs, quality_entries)


def ingest_materials_bronze(spark: SparkSession, cfg: DictConfig, run_id: str | None = None) -> str:
    """Streams raw materials catalog CSV files into Bronze Delta table"""
    active_run_id = run_id or str(uuid.uuid4())[:8]
    raw_dir = cfg.paths.raw_materials
    if not Path(raw_dir).exists():
        raise FileNotFoundError(f"Raw materials directory not found: {raw_dir}")

    schema = discover_all_string_schema(spark, raw_dir, KNOWN_MATERIALS_COLS)
    checkpoint_dir = f"{cfg.paths.checkpoints}/bronze_materials"

    stream_df = (
        spark.readStream.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(raw_dir)
        .withColumn("_source_file", F.input_file_name())
    )

    query = (
        stream_df.writeStream.foreachBatch(
            lambda df, batch_id: _process_materials_microbatch(df, batch_id, cfg, active_run_id)
        )
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return active_run_id
