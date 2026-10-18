import uuid

from omegaconf import DictConfig
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from saas_pipeline.common.config import get_checkpoint_path, get_raw_path, get_target
from saas_pipeline.common.delta import read_delta, write_stream_delta
from saas_pipeline.common.quality_log import QualityRule, evaluate_and_log_rules

MATERIALS_RAW_SCHEMA = StructType(
    [
        StructField("material", StringType(), nullable=True),
        StructField("descripcion", StringType(), nullable=True),
        StructField("categoria", StringType(), nullable=True),
        StructField("precio_base", StringType(), nullable=True),
        StructField("valid_from", StringType(), nullable=True),
        StructField("valid_to", StringType(), nullable=True),
        StructField("is_current", StringType(), nullable=True),
    ]
)


def ingest_materials_bronze(spark: SparkSession, cfg: DictConfig, run_id: str | None = None) -> str:
    """Streams raw materials catalog into shared Bronze Delta table and runs DQ checks."""
    active_run_id = run_id or str(uuid.uuid4())[:8]
    batch_id = f"{active_run_id}_bronze_mat"

    raw_path = get_raw_path(cfg, "raw_materials")
    target = get_target(cfg, "bronze_materials")
    checkpoint = get_checkpoint_path(cfg, "bronze_materials")

    raw_stream = spark.readStream.format("csv").option("header", "true").schema(MATERIALS_RAW_SCHEMA).load(raw_path)

    enriched_stream = (
        raw_stream.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_id", F.lit(batch_id))
    )

    write_stream_delta(
        stream_df=enriched_stream,
        cfg=cfg,
        target=target,
        checkpoint_path=checkpoint,
        output_mode="append",
    )

    bronze_df = read_delta(spark, cfg, target)
    rules = [
        QualityRule(
            name="bronze_materials_not_empty",
            severity="critical",
            valid_condition=F.col("material").isNotNull() & (F.trim(F.col("material")) != ""),
        ),
        QualityRule(
            name="bronze_materials_has_base_price",
            severity="warning",
            valid_condition=F.col("precio_base").isNotNull(),
        ),
    ]

    evaluate_and_log_rules(
        df=bronze_df,
        rules=rules,
        cfg=cfg,
        run_id=active_run_id,
        batch_id=batch_id,
        tenant_id="shared",
        layer="bronze",
        table_name="materials_catalog",
    )

    return active_run_id
