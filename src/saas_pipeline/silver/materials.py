import uuid

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from saas_pipeline.common.config import get_target
from saas_pipeline.common.delta import (
    delta_exists,
    get_delta_table,
    read_delta,
    write_delta,
)
from saas_pipeline.common.quality_log import (
    QualityRule,
    build_quality_entry,
    evaluate_and_log_rules,
)


def transform_silver_materials(bronze_mat_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Cleans, validates, and splits materials catalog into valid SCD2 rows and quarantined rows."""
    parsed_df = (
        bronze_mat_df.withColumn(
            "_clean_material",
            F.when(
                F.col("material").isNotNull() & (F.trim(F.col("material")) != ""),
                F.trim(F.col("material")),
            ).otherwise(F.lit(None)),
        )
        .withColumn("_parsed_price", F.col("precio_base").cast(DecimalType(10, 2)))
        .withColumn("_parsed_from", F.to_date(F.trim(F.col("valid_from")), "yyyy-MM-dd"))
        .withColumn("_parsed_to", F.to_date(F.trim(F.col("valid_to")), "yyyy-MM-dd"))
    )

    raw_reasons = F.array(
        F.when(F.col("_clean_material").isNull(), F.lit("NULL_SKU")),
        F.when(
            F.col("_parsed_price").isNull() | (F.col("_parsed_price") <= 0),
            F.lit("INVALID_BASE_PRICE"),
        ),
        F.when(
            F.col("_parsed_from").isNull()
            | F.col("_parsed_to").isNull()
            | (F.col("_parsed_from") > F.col("_parsed_to")),
            F.lit("INVALID_DATE_INTERVAL"),
        ),
    )
    compact_reasons = F.array_compact(raw_reasons)

    evaluated_df = parsed_df.withColumn(
        "_quarantine_reason",
        F.when(F.size(compact_reasons) > 0, F.concat_ws("|", compact_reasons)).otherwise(F.lit(None)),
    )

    quarantine_cols = [c for c in bronze_mat_df.columns] + ["_quarantine_reason"]
    quarantine_df = evaluated_df.filter(F.col("_quarantine_reason").isNotNull()).select(*quarantine_cols)

    valid_candidates_df = (
        evaluated_df.filter(F.col("_quarantine_reason").isNull())
        .select(
            F.col("_clean_material").alias("material"),
            F.trim(F.col("descripcion")).alias("descripcion"),
            F.trim(F.col("categoria")).alias("categoria"),
            F.col("_parsed_price").alias("precio_base"),
            F.col("_parsed_from").alias("valid_from"),
            F.col("_parsed_to").alias("valid_to"),
            F.col("_ingestion_timestamp"),
            F.col("_source_file"),
            F.col("_batch_id"),
        )
        .dropDuplicates(["material", "valid_from"])
    )

    # Enforce strictly 1 is_current=True per SKU
    w_latest = Window.partitionBy("material").orderBy(F.col("valid_from").desc())
    valid_df = (
        valid_candidates_df.withColumn("_rn", F.row_number().over(w_latest))
        .withColumn(
            "is_current",
            (F.col("_rn") == 1) & (F.col("valid_to") == F.to_date(F.lit("9999-12-31"))),
        )
        .drop("_rn")
    )

    return valid_df, quarantine_df


def merge_dim_materials(
    spark: SparkSession,
    valid_mat_df: DataFrame,
    quarantine_mat_df: DataFrame,
    cfg: DictConfig,
    run_id: str,
) -> None:
    "SCD2 MERGE INTO, quarantine and logs quality for dim_materials"
    batch_uid = f"{run_id}_dim_mat_shared"

    quarantine_target = get_target(cfg, "quarantine_materials")
    if not quarantine_mat_df.isEmpty():
        write_delta(quarantine_mat_df, cfg, quarantine_target, mode="overwrite", merge_schema=True)

    dim_target = get_target(cfg, "silver_materials")
    if delta_exists(spark, cfg, dim_target):
        delta_tbl = get_delta_table(spark, cfg, dim_target)
        (
            delta_tbl.alias("target")
            .merge(
                valid_mat_df.alias("source"),
                "target.material = source.material AND target.valid_from = source.valid_from",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        merged_df = read_delta(spark, cfg, dim_target)
        w_latest = Window.partitionBy("material").orderBy(F.col("valid_from").desc())
        reconciled_df = (
            merged_df.withColumn("_rn", F.row_number().over(w_latest))
            .withColumn(
                "is_current",
                (F.col("_rn") == 1) & (F.col("valid_to") == F.to_date(F.lit("9999-12-31"))),
            )
            .drop("_rn")
        )
        write_delta(reconciled_df, cfg, dim_target, mode="overwrite")
    else:
        write_delta(valid_mat_df, cfg, dim_target, mode="overwrite")

    total_eval = valid_mat_df.count() + quarantine_mat_df.count()
    q_fails = quarantine_mat_df.count()

    pre_entries = [
        build_quality_entry(
            run_id=run_id,
            batch_id=batch_uid,
            tenant_id="shared",
            layer="silver",
            table_name="dim_materials",
            check_name="silver_quarantined_invalid_materials",
            check_severity="warning",
            records_checked=total_eval,
            records_failed=q_fails,
        )
    ]

    final_df = read_delta(spark, cfg, dim_target)
    rules = [
        QualityRule(
            name="silver_dim_materials_valid_interval",
            severity="critical",
            valid_condition=F.col("valid_from").isNotNull()
            & F.col("valid_to").isNotNull()
            & (F.col("valid_from") <= F.col("valid_to")),
        ),
        QualityRule(
            name="silver_dim_materials_positive_base_price",
            severity="critical",
            valid_condition=F.col("precio_base").isNotNull() & (F.col("precio_base") > 0),
        ),
    ]
    evaluate_and_log_rules(
        df=final_df,
        rules=rules,
        cfg=cfg,
        run_id=run_id,
        batch_id=batch_uid,
        tenant_id="shared",
        layer="silver",
        table_name="dim_materials",
        extra_entries=pre_entries,
    )


def process_silver_materials(spark: SparkSession, cfg: DictConfig, run_id: str | None = None) -> str:
    """Loads Bronze materials catalog, splits anomalies, and merges SCD2 dim_materials per tenant"""
    active_run_id = run_id or str(uuid.uuid4())[:8]
    bronze_target = get_target(cfg, "bronze_materials")
    bronze_mat_df = read_delta(spark, cfg, bronze_target)

    valid_df, quarantine_df = transform_silver_materials(bronze_mat_df)
    merge_dim_materials(spark, valid_df, quarantine_df, cfg, active_run_id)
    return active_run_id
