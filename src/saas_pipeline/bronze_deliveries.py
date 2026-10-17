import uuid
from pathlib import Path

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from saas_pipeline.quality_log import build_quality_entry, write_quality_logs

KNOWN_DELIVERIES_COLS = [
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


def discover_all_string_schema(spark: SparkSession, raw_dir: str, known_cols: list[str]) -> StructType:
    """
    Schema Evolution: Enforces known columns as StringType and appends
    any newly discovered CSV header columns in order as StringType.
    """
    sample_df = spark.read.option("header", "true").csv(raw_dir).limit(1)
    discovered_cols = [c.strip() for c in sample_df.columns]

    fields = [StructField(col_name.lower(), StringType(), True) for col_name in discovered_cols]

    # Ensure any missing known columns are still present in the schema definition
    discovered_lower = {c.lower() for c in discovered_cols}
    for col_name in known_cols:
        if col_name.lower() not in discovered_lower:
            fields.append(StructField(col_name.lower(), StringType(), True))

    return StructType(fields)


def process_deliveries_microbatch(batch_df: DataFrame, batch_id: int, cfg: DictConfig, run_id: str):
    """Writes microbatch per tenant with replaceWhere idempotency and logs Bronze quality"""
    spark = batch_df.sparkSession
    batch_uid = f"{run_id}_b{batch_id}"

    if batch_df.isEmpty():
        write_quality_logs(
            spark,
            cfg.paths.quality_logs,
            [
                build_quality_entry(
                    run_id=run_id,
                    batch_id=batch_uid,
                    tenant_id=cfg.execution.tenant,
                    layer="bronze",
                    table_name="deliveries",
                    check_name="bronze_non_empty_batch",
                    check_severity="warning",
                    records_checked=0,
                    records_failed=1,
                )
            ],
        )
        return

    # lowercase pais/tenant + add technical/metadata columns
    staged_df = (
        batch_df.withColumn("pais", F.lower(F.trim(F.col("pais"))))
        .withColumn(
            "_tenant_id",
            F.when(F.col("pais").isNull() | (F.col("pais") == ""), F.lit("unknown")).otherwise(F.col("pais")),
        )
        .withColumn("tenant_id", F.col("_tenant_id"))
        .withColumn(
            "fecha_proceso",
            F.when(
                F.col("fecha_proceso").isNull() | (F.trim(F.col("fecha_proceso")) == ""),
                F.lit("UNKNOWN_DATE"),
            ).otherwise(F.trim(F.col("fecha_proceso"))),
        )
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_uid))
    )

    # filter by tenant if a specific single tenant was requested via CLI/config
    target_tenant = cfg.execution.tenant.lower()
    if target_tenant != "all":
        staged_df = staged_df.filter(F.col("_tenant_id") == target_tenant)

    distinct_tenants = [row["_tenant_id"] for row in staged_df.select("_tenant_id").distinct().collect()]

    quality_entries: list[dict] = []

    for tenant in distinct_tenants:
        tenant_df = staged_df.filter(F.col("_tenant_id") == tenant)
        total_rows = tenant_df.count()
        missing_part_rows = tenant_df.filter(
            (F.col("fecha_proceso") == "UNKNOWN_DATE") | (F.col("_tenant_id") == "unknown")
        ).count()

        distinct_dates = [row["fecha_proceso"] for row in tenant_df.select("fecha_proceso").distinct().collect()]
        dates_quoted = ", ".join([f"'{d}'" for d in distinct_dates])
        replace_condition = f"fecha_proceso IN ({dates_quoted}) AND tenant_id = '{tenant}'"
        target_path = f"{cfg.paths.bronze}/{tenant}/deliveries"

        (
            tenant_df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", replace_condition)
            .option("mergeSchema", "true")
            .partitionBy("fecha_proceso", "tenant_id")
            .save(target_path)
        )

        # quality log ingested batch
        quality_entries.extend(
            [
                build_quality_entry(
                    run_id=run_id,
                    batch_id=batch_uid,
                    tenant_id=tenant,
                    layer="bronze",
                    table_name="deliveries",
                    check_name="bronze_non_empty_batch",
                    check_severity="info",
                    records_checked=total_rows,
                    records_failed=0 if total_rows > 0 else 1,
                ),
                build_quality_entry(
                    run_id=run_id,
                    batch_id=batch_uid,
                    tenant_id=tenant,
                    layer="bronze",
                    table_name="deliveries",
                    check_name="bronze_partition_key_present",
                    check_severity="warning",
                    records_checked=total_rows,
                    records_failed=missing_part_rows,
                ),
            ]
        )

    write_quality_logs(spark, cfg.paths.quality_logs, quality_entries)


def ingest_deliveries_bronze(spark: SparkSession, cfg: DictConfig, run_id: str | None = None) -> str:
    """Streams raw deliveries CSV files into per-tenant Bronze Delta tables"""
    active_run_id = run_id or str(uuid.uuid4())[:8]
    raw_dir = cfg.paths.raw_deliveries
    if not Path(raw_dir).exists():
        raise FileNotFoundError(f"Raw deliveries directory not found: {raw_dir}")

    schema = discover_all_string_schema(spark, raw_dir, KNOWN_DELIVERIES_COLS)
    checkpoint_dir = f"{cfg.paths.checkpoints}/bronze_deliveries/{cfg.execution.tenant.lower()}"

    stream_df = (
        spark.readStream.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(raw_dir)
        .withColumn("_source_file", F.input_file_name())
    )

    query = (
        stream_df.writeStream.foreachBatch(
            lambda df, batch_id: process_deliveries_microbatch(df, batch_id, cfg, active_run_id)
        )
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return active_run_id
