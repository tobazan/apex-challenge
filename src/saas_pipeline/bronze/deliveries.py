import uuid

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from saas_pipeline.common.config import (
    get_checkpoint_path,
    get_raw_path,
    get_target,
    resolve_target_tenants,
)
from saas_pipeline.common.delta import read_delta, write_delta
from saas_pipeline.common.quality_log import QualityRule, evaluate_and_log_rules

DELIVERIES_RAW_SCHEMA = StructType(
    [
        StructField("pais", StringType(), nullable=True),
        StructField("fecha_proceso", StringType(), nullable=True),
        StructField("transporte", StringType(), nullable=True),
        StructField("ruta", StringType(), nullable=True),
        StructField("tipo_entrega", StringType(), nullable=True),
        StructField("material", StringType(), nullable=True),
        StructField("precio", StringType(), nullable=True),
        StructField("cantidad", StringType(), nullable=True),
        StructField("unidad", StringType(), nullable=True),
    ]
)


def ingest_deliveries_bronze(spark: SparkSession, cfg: DictConfig, run_id: str | None = None) -> str:
    """Streams raw deliveries, splits by tenant into isolated Bronze Delta tables, and logs DQ."""
    active_run_id = run_id or str(uuid.uuid4())[:8]
    batch_id = f"{active_run_id}_bronze_deliv"
    target_tenants = resolve_target_tenants(cfg)

    raw_path = get_raw_path(cfg, "raw_deliveries")
    checkpoint = get_checkpoint_path(cfg, f"bronze_deliveries_{cfg.execution.tenant}")

    raw_stream = spark.readStream.format("csv").option("header", "true").schema(DELIVERIES_RAW_SCHEMA).load(raw_path)

    enriched_stream = (
        raw_stream.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_tenant_id", F.lower(F.trim(F.col("pais"))))
        .withColumn("_batch_id", F.lit(batch_id))
        .filter(F.col("_tenant_id").isin(target_tenants))
    )

    def _route_microbatch_to_tenants(batch_df: DataFrame, epoch_id: int) -> None:
        if batch_df.isEmpty():
            return
        batch_df.persist()
        try:
            present_tenants = [r["_tenant_id"] for r in batch_df.select("_tenant_id").distinct().collect()]
            for t in present_tenants:
                tenant_df = batch_df.filter(F.col("_tenant_id") == t)
                target = get_target(cfg, "bronze_deliveries", tenant=t)
                write_delta(tenant_df, cfg, target, mode="append", merge_schema=True)
        finally:
            batch_df.unpersist()

    query = (
        enriched_stream.writeStream.foreachBatch(_route_microbatch_to_tenants)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    # Evaluate Bronze DQ rules per tenant
    rules = [
        QualityRule(
            name="bronze_deliveries_has_material",
            severity="warning",
            valid_condition=F.col("material").isNotNull() & (F.trim(F.col("material")) != ""),
        ),
        QualityRule(
            name="bronze_deliveries_has_fecha_proceso",
            severity="warning",
            valid_condition=F.col("fecha_proceso").isNotNull() & (F.trim(F.col("fecha_proceso")) != ""),
        ),
    ]

    for t in target_tenants:
        target = get_target(cfg, "bronze_deliveries", tenant=t)
        try:
            t_df = read_delta(spark, cfg, target)
            evaluate_and_log_rules(
                df=t_df,
                rules=rules,
                cfg=cfg,
                run_id=active_run_id,
                batch_id=batch_id,
                tenant_id=t,
                layer="bronze",
                table_name="deliveries",
            )
        except Exception:
            if cfg.execution.fail_fast:
                raise

    return active_run_id
