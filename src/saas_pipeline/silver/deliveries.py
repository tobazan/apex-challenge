from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, LongType

from saas_pipeline.common.config import get_target
from saas_pipeline.common.delta import (
    delta_exists,
    get_delta_table,
    read_delta,
    write_delta,
)
from saas_pipeline.common.quality_log import (
    build_quality_entry,
    evaluate_and_log_rules,
)
from saas_pipeline.silver.delivery_quality_rules import (
    build_delivery_quarantine_reason,
    build_silver_delivery_quality_rules,
)

BUSINESS_DEDUP_COLS = [
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
PRICE_TYPE = DecimalType(18, 2)
QUANTITY_PARSE_TYPE = DecimalType(18, 4)
QUANTITY_TYPE = IntegerType()


def split_and_enrich_deliveries(
    bronze_df: DataFrame,
    dim_materials_df: DataFrame,
    cfg: DictConfig,
) -> tuple[DataFrame, DataFrame, int, int]:
    """
    Applies Silver rules to a tenant's Bronze deliveries:
    1. Dedup rows based on BUSINESS_DEDUP_COLS
    2. Discards tipo_entrega (COBR, Z99)
    3. Evaluates anomalies and temp SCD2 join against dim_materials
    Returns: (valid_enriched_df, quarantine_df, dups_dropped, discarded_count)
    """
    dedup_df = bronze_df.dropDuplicates(BUSINESS_DEDUP_COLS)

    routine_types = list(cfg.rules.routine_delivery_types)
    bonus_types = list(cfg.rules.bonus_delivery_types)
    valid_delivery_types = routine_types + bonus_types

    kept_df = dedup_df.filter(F.trim(F.col("tipo_entrega")).isin(valid_delivery_types))
    counts = (
        bronze_df.select(F.lit("raw").alias("_stage"))
        .unionByName(dedup_df.select(F.lit("deduplicated").alias("_stage")))
        .unionByName(kept_df.select(F.lit("kept").alias("_stage")))
        .groupBy("_stage")
        .count()
        .collect()
    )
    count_by_stage = {row["_stage"]: int(row["count"]) for row in counts}
    raw_count = count_by_stage.get("raw", 0)
    after_dedup_count = count_by_stage.get("deduplicated", 0)
    after_discard_count = count_by_stage.get("kept", 0)
    duplicates_dropped = raw_count - after_dedup_count
    discarded_count = after_dedup_count - after_discard_count

    # Parse candidate types for validation
    parsed_df = (
        kept_df.withColumn(
            "_parsed_date",
            F.when(
                F.col("fecha_proceso").rlike(r"^\d{8}$"),
                F.to_date(F.col("fecha_proceso"), "yyyyMMdd"),
            ).otherwise(F.lit(None)),
        )
        .withColumn("_parsed_qty_decimal", F.col("cantidad").cast(QUANTITY_PARSE_TYPE))
        .withColumn("_parsed_price", F.col("precio").cast(PRICE_TYPE))
    )

    # Temporal join with SCD2 dim_materials using broadcast
    dim_alias = dim_materials_df.select(
        F.col("material").alias("_dim_material"),
        F.col("descripcion").alias("material_descripcion"),
        F.col("categoria").alias("material_categoria"),
        F.col("precio_base").alias("material_precio_base"),
        F.col("valid_from").alias("_dim_valid_from"),
        F.col("valid_to").alias("_dim_valid_to"),
    )

    sku_match_cond = F.trim(parsed_df["material"]) == dim_alias["_dim_material"]
    date_range_cond = parsed_df["_parsed_date"].between(dim_alias["_dim_valid_from"], dim_alias["_dim_valid_to"])
    temporal_join_cond = sku_match_cond & date_range_cond

    joined_df = parsed_df.join(
        F.broadcast(dim_alias),
        on=temporal_join_cond,
        how="left",
    )

    # Check existence in catalog across all validity windows
    distinct_skus = dim_materials_df.select(F.col("material").alias("_cat_sku")).distinct()

    sku_catalog_cond = F.trim(joined_df["material"]) == distinct_skus["_cat_sku"]

    joined_df = joined_df.join(
        F.broadcast(distinct_skus),
        on=sku_catalog_cond,
        how="left",
    )

    evaluated_df = joined_df.withColumn(
        "_quarantine_reason",
        build_delivery_quarantine_reason(),
    )

    # Split Quarantine / Valid
    quarantine_cols = [c for c in bronze_df.columns] + ["_quarantine_reason"]
    quarantine_df = evaluated_df.filter(F.col("_quarantine_reason").isNotNull()).select(*quarantine_cols)

    cs_factor = int(cfg.rules.cs_to_st_factor)
    valid_df = (
        evaluated_df.filter(F.col("_quarantine_reason").isNull())
        .withColumn("tenant_id", F.col("_tenant_id"))
        .withColumn("fecha_proceso", F.col("_parsed_date"))
        .withColumn("transporte", F.col("transporte").cast(LongType()))
        .withColumn("ruta", F.col("ruta").cast(LongType()))
        .withColumn("tipo_entrega", F.trim(F.col("tipo_entrega")))
        .withColumn("material", F.trim(F.col("material")))
        .withColumn("precio", F.col("_parsed_price"))
        .withColumn("cantidad_original", F.col("_parsed_qty_decimal").cast(QUANTITY_TYPE))
        .withColumn("unidad_original", F.upper(F.trim(F.col("unidad"))))
        .withColumn(
            "cantidad_normalizada_st",
            F.when(
                F.col("unidad_original") == "CS",
                (F.col("cantidad_original") * F.lit(cs_factor)).cast(QUANTITY_TYPE),
            ).otherwise(F.col("cantidad_original")),
        )
        .withColumn("unidad_normalizada", F.lit("ST"))
        .withColumn("is_routine_delivery", F.col("tipo_entrega").isin(routine_types))
        .withColumn("is_bonus_delivery", F.col("tipo_entrega").isin(bonus_types))
        .select(
            "tenant_id",
            "fecha_proceso",
            "transporte",
            "ruta",
            "tipo_entrega",
            "material",
            "material_descripcion",
            "material_categoria",
            "material_precio_base",
            "precio",
            "cantidad_original",
            "unidad_original",
            "cantidad_normalizada_st",
            "unidad_normalizada",
            "is_routine_delivery",
            "is_bonus_delivery",
            "_ingestion_timestamp",
            "_source_file",
            "_tenant_id",
            "_batch_id",
        )
    )

    # Keep one deterministic record per Silver merge key
    # Makes sense even when temporal join with SCD2 dim_materials is used?
    delivery_key = ["tenant_id", "fecha_proceso", "transporte", "ruta", "material", "tipo_entrega"]
    delivery_rank = Window.partitionBy(*delivery_key).orderBy(
        F.col("_ingestion_timestamp").desc_nulls_last(),
        F.col("_source_file").desc_nulls_last(),
        F.col("_batch_id").desc_nulls_last(),
        F.sha2(F.to_json(F.struct(*[F.col(column) for column in valid_df.columns])), 256).desc(),
    )
    valid_df = (
        valid_df.withColumn("_source_rank", F.row_number().over(delivery_rank))
        .filter(F.col("_source_rank") == 1)
        .drop("_source_rank")
    )

    return valid_df, quarantine_df, duplicates_dropped, discarded_count


def process_silver_deliveries_for_tenant(spark: SparkSession, tenant: str, cfg: DictConfig, run_id: str) -> None:
    "Processes Bronze -> Silver fact_deliveries and quarantine for a single tenant"
    bronze_target = get_target(cfg, "bronze_deliveries", tenant=tenant)
    dim_target = get_target(cfg, "silver_materials")
    quarantine_target = get_target(cfg, "quarantine_deliveries", tenant=tenant)
    silver_target = get_target(cfg, "silver_deliveries", tenant=tenant)

    bronze_df = read_delta(spark, cfg, bronze_target)
    dim_materials_df = read_delta(spark, cfg, dim_target)

    # Filter Bronze by configured start_date and end_date
    # start_ymd = cfg.execution.start_date.replace("-", "")
    # end_ymd = cfg.execution.end_date.replace("-", "")
    # bronze_filtered_df = bronze_df.filter(
    #     (~F.col("fecha_proceso").rlike(r"^\d{8}$"))
    #     | (F.col("fecha_proceso") < "20000101")
    #     | (F.col("fecha_proceso") > "20991231")
    #     | ((F.col("fecha_proceso") >= start_ymd) & (F.col("fecha_proceso") <= end_ymd))
    # )

    # if bronze_filtered_df.isEmpty():
    #     return

    valid_df, quarantine_df, dup_dropped, discarded_cnt = split_and_enrich_deliveries(
        # bronze_filtered_df, dim_materials_df, cfg
        bronze_df,
        dim_materials_df,
        cfg,
    )

    if not quarantine_df.isEmpty():
        write_delta(quarantine_df, cfg, quarantine_target, mode="overwrite", merge_schema=True)

    # MERGE INTO Silver fact_deliveries
    merge_condition = (
        "target.tenant_id = source.tenant_id AND "
        "target.fecha_proceso = source.fecha_proceso AND "
        "target.transporte = source.transporte AND "
        "target.ruta = source.ruta AND "
        "target.material = source.material AND "
        "target.tipo_entrega = source.tipo_entrega"
    )

    if delta_exists(spark, cfg, silver_target):
        delta_tbl = get_delta_table(spark, cfg, silver_target)
        (
            delta_tbl.alias("target")
            .merge(valid_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        write_delta(
            valid_df,
            cfg,
            silver_target,
            mode="overwrite",
            partition_by=["fecha_proceso"],
        )

    # Evaluate and log Silver DQ checks, count anomalies found during quarantine routing
    evaluation_metrics = (
        valid_df.select(
            F.lit("valid").alias("_classification"),
            F.lit(0).cast("long").alias("_invalid_dates"),
            F.lit(0).cast("long").alias("_invalid_quantities"),
            F.lit(0).cast("long").alias("_unmapped_materials"),
        )
        .unionByName(
            quarantine_df.select(
                F.lit("quarantine").alias("_classification"),
                F.col("_quarantine_reason").contains("INVALID_FECHA_PROCESO").cast("long").alias("_invalid_dates"),
                F.col("_quarantine_reason").contains("INVALID_CANTIDAD").cast("long").alias("_invalid_quantities"),
                F.col("_quarantine_reason").contains("UNMAPPED_MATERIAL").cast("long").alias("_unmapped_materials"),
            )
        )
        .groupBy("_classification")
        .agg(
            F.count(F.lit(1)).alias("records"),
            F.sum("_invalid_dates").alias("invalid_dates"),
            F.sum("_invalid_quantities").alias("invalid_quantities"),
            F.sum("_unmapped_materials").alias("unmapped_materials"),
        )
        .collect()
    )
    metrics_by_classification = {row["_classification"]: row for row in evaluation_metrics}
    valid_count = int(metrics_by_classification.get("valid")["records"]) if "valid" in metrics_by_classification else 0
    quarantine_metrics = metrics_by_classification.get("quarantine")
    total_quarantined = int(quarantine_metrics["records"]) if quarantine_metrics else 0
    total_evaluated = valid_count + total_quarantined
    q_date_fails = int(quarantine_metrics["invalid_dates"] or 0) if quarantine_metrics else 0
    q_qty_fails = int(quarantine_metrics["invalid_quantities"] or 0) if quarantine_metrics else 0
    q_mat_fails = int(quarantine_metrics["unmapped_materials"] or 0) if quarantine_metrics else 0

    batch_uid = f"{run_id}_silver_{tenant}"
    pre_merge_entries = [
        build_quality_entry(
            run_id=run_id,
            batch_id=batch_uid,
            tenant_id=tenant,
            layer="silver",
            table_name="fact_deliveries",
            check_name="silver_exact_duplicates_deduplicated",
            check_severity="info",
            records_checked=total_evaluated + dup_dropped + discarded_cnt,
            records_failed=dup_dropped,
        ),
        build_quality_entry(
            run_id=run_id,
            batch_id=batch_uid,
            tenant_id=tenant,
            layer="silver",
            table_name="fact_deliveries",
            check_name="silver_out_of_scope_delivery_types_discarded",
            check_severity="info",
            records_checked=total_evaluated + discarded_cnt,
            records_failed=discarded_cnt,
        ),
        build_quality_entry(
            run_id=run_id,
            batch_id=batch_uid,
            tenant_id=tenant,
            layer="silver",
            table_name="fact_deliveries",
            check_name="silver_quarantined_invalid_date",
            check_severity="warning",
            records_checked=total_evaluated,
            records_failed=q_date_fails,
        ),
        build_quality_entry(
            run_id=run_id,
            batch_id=batch_uid,
            tenant_id=tenant,
            layer="silver",
            table_name="fact_deliveries",
            check_name="silver_quarantined_invalid_quantity",
            check_severity="warning",
            records_checked=total_evaluated,
            records_failed=q_qty_fails,
        ),
        build_quality_entry(
            run_id=run_id,
            batch_id=batch_uid,
            tenant_id=tenant,
            layer="silver",
            table_name="fact_deliveries",
            check_name="silver_quarantined_unmapped_material",
            check_severity="warning",
            records_checked=total_evaluated,
            records_failed=q_mat_fails,
        ),
    ]

    # Critical post-merge DQ checks on Silver
    silver_final_df = read_delta(spark, cfg, silver_target)
    routine_types = list(cfg.rules.routine_delivery_types)
    bonus_types = list(cfg.rules.bonus_delivery_types)
    critical_silver_rules = build_silver_delivery_quality_rules(
        routine_delivery_types=routine_types,
        bonus_delivery_types=bonus_types,
    )

    evaluate_and_log_rules(
        df=silver_final_df,
        rules=critical_silver_rules,
        cfg=cfg,
        run_id=run_id,
        batch_id=batch_uid,
        tenant_id=tenant,
        layer="silver",
        table_name="fact_deliveries",
        extra_entries=pre_merge_entries,
    )
