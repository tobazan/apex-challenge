from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType

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
    raw_count = bronze_df.count()
    dedup_df = bronze_df.dropDuplicates(BUSINESS_DEDUP_COLS)
    after_dedup_count = dedup_df.count()
    duplicates_dropped = raw_count - after_dedup_count

    routine_types = list(cfg.rules.routine_delivery_types)
    bonus_types = list(cfg.rules.bonus_delivery_types)
    valid_delivery_types = routine_types + bonus_types

    kept_df = dedup_df.filter(F.trim(F.col("tipo_entrega")).isin(valid_delivery_types))
    after_discard_count = kept_df.count()
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
        .withColumn("_parsed_qty", F.col("cantidad").cast(DecimalType(12, 2)))
        .withColumn("_parsed_price", F.col("precio").cast(DecimalType(12, 2)))
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

    # Build quarantine reasons array
    raw_reasons = F.array(
        F.when(F.col("_parsed_date").isNull(), F.lit("INVALID_FECHA_PROCESO")),
        F.when(
            F.col("_parsed_qty").isNull() | (F.col("_parsed_qty") <= 0),
            F.lit("INVALID_CANTIDAD"),
        ),
        F.when(
            F.col("_parsed_price").isNull() | (F.col("_parsed_price") <= 0),
            F.lit("INVALID_PRECIO"),
        ),
        F.when(
            F.col("_cat_sku").isNull() | (F.col("_parsed_date").isNotNull() & F.col("_dim_material").isNull()),
            F.lit("UNMAPPED_MATERIAL"),
        ),
    )
    compact_reasons = F.array_compact(raw_reasons)

    evaluated_df = joined_df.withColumn(
        "_quarantine_reason",
        F.when(F.size(compact_reasons) > 0, F.concat_ws("|", compact_reasons)).otherwise(F.lit(None)),
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
        .withColumn("cantidad_original", F.col("_parsed_qty"))
        .withColumn("unidad_original", F.upper(F.trim(F.col("unidad"))))
        .withColumn(
            "cantidad_normalizada_st",
            F.when(
                F.col("unidad_original") == "CS",
                (F.col("_parsed_qty") * F.lit(cs_factor)).cast(DecimalType(12, 2)),
            ).otherwise(F.col("_parsed_qty")),
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
        # Deduplicate on composite key in case source had same key with different metadata
        .dropDuplicates(["tenant_id", "fecha_proceso", "transporte", "ruta", "material", "tipo_entrega"])
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
    total_evaluated = valid_df.count() + quarantine_df.count()
    q_date_fails = quarantine_df.filter(F.col("_quarantine_reason").contains("INVALID_FECHA_PROCESO")).count()
    q_qty_fails = quarantine_df.filter(F.col("_quarantine_reason").contains("INVALID_CANTIDAD")).count()
    q_mat_fails = quarantine_df.filter(F.col("_quarantine_reason").contains("UNMAPPED_MATERIAL")).count()

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
    critical_silver_rules = [
        QualityRule(
            name="silver_valid_fecha_proceso_not_null",
            severity="critical",
            valid_condition=F.col("fecha_proceso").isNotNull(),
        ),
        QualityRule(
            name="silver_positive_normalized_quantity_st",
            severity="critical",
            valid_condition=F.col("cantidad_normalizada_st").isNotNull()
            & (F.col("cantidad_normalizada_st") > 0)
            & (F.col("unidad_normalizada") == "ST"),
        ),
        QualityRule(
            name="silver_scd2_catalog_referential_integrity",
            severity="critical",
            valid_condition=F.col("material_descripcion").isNotNull() & F.col("material_precio_base").isNotNull(),
        ),
        QualityRule(
            name="silver_valid_delivery_type_flags",
            severity="critical",
            valid_condition=F.col("tipo_entrega").isin(["ZPRE", "ZVE1", "Z04", "Z05"])
            & (F.col("is_routine_delivery") != F.col("is_bonus_delivery")),
        ),
    ]

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
