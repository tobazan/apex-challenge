import uuid

from omegaconf import DictConfig
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saas_pipeline.common.config import get_target
from saas_pipeline.common.delta import read_delta, write_delta
from saas_pipeline.common.quality_log import QUALITY_LOGS_SCHEMA, build_quality_entry


def daily_metrics_by_delivery_type(spark: SparkSession, tenant: str, cfg: DictConfig, run_id: str | None = None) -> str:
    """Computes daily delivery metrics by delivery type for a tenant and writes to Gold Delta table"""
    active_run_id = run_id or str(uuid.uuid4())[:8]
    batch_id = f"{active_run_id}_gold_metrics_{tenant}"

    silver_target = get_target(cfg, "silver_deliveries", tenant=tenant)
    gold_target = get_target(cfg, "gold_daily_metrics", tenant=tenant)

    # Compute transaction revenue per row
    silver_df = read_delta(spark, cfg, silver_target)
    enriched_df = silver_df.withColumn("_row_revenue", F.col("cantidad_normalizada_st") * F.col("precio"))

    # Actual daily aggregations
    agg_df = enriched_df.groupBy(
        "tenant_id",
        "fecha_proceso",
        "tipo_entrega",
    ).agg(
        F.sum("cantidad_normalizada_st").alias("total_units"),
        F.sum("_row_revenue").alias("total_revenue"),
        F.countDistinct("ruta").alias("active_routes"),
        F.countDistinct("transporte").alias("active_transports"),
    )

    # Determine date boundaries for spine generation
    start_date_str = cfg.execution.get("start_date")
    end_date_str = cfg.execution.get("end_date")

    if start_date_str and end_date_str:
        bounds_df = spark.sql(f"""
            SELECT
                CAST('{start_date_str}' AS DATE) as min_date,
                CAST('{end_date_str}' AS DATE) as max_date
        """)
    else:
        bounds_df = agg_df.select(
            F.min("fecha_proceso").alias("min_date"),
            F.max("fecha_proceso").alias("max_date"),
        )

    bounds = bounds_df.collect()[0]
    min_d, max_d = bounds["min_date"], bounds["max_date"]

    if min_d and max_d:
        # Generate date spine dataframe
        spine_df = spark.sql(f"""
            SELECT EXPLODE(SEQUENCE(CAST('{min_d}' AS DATE), CAST('{max_d}' AS DATE))) AS spine_date
        """)

        # Create full grid: dates x delivery types
        cfg_delivery_types = list(cfg.get("domain", {}).get("tipo_entrega", ["ZPRE", "ZVE1", "ZBON", "ZCOM"]))
        dims_data = [(tenant, te) for te in cfg_delivery_types]
        dims_df = spark.createDataFrame(dims_data, ["tenant_id", "tipo_entrega"])
        grid_df = spine_df.crossJoin(dims_df).withColumnRenamed("spine_date", "fecha_proceso")

        # Left join actuals onto grid and fill missing with zeros
        gold_df = grid_df.join(
            agg_df,
            on=["tenant_id", "fecha_proceso", "tipo_entrega"],
            how="left",
        ).select(
            F.col("tenant_id"),
            F.col("fecha_proceso"),
            F.col("tipo_entrega"),
            F.coalesce(F.col("total_units"), F.lit(0)).alias("total_units"),
            F.coalesce(F.col("total_revenue"), F.lit(0.0)).alias("total_revenue"),
            F.coalesce(F.col("active_routes"), F.lit(0)).alias("active_routes"),
            F.coalesce(F.col("active_transports"), F.lit(0)).alias("active_transports"),
        )
    else:
        gold_df = agg_df

    write_delta(
        df=gold_df,
        cfg=cfg,
        target=gold_target,
        mode="overwrite",
        partition_by=["fecha_proceso"],
        merge_schema=True,
    )

    # Validation checks
    total_rows = gold_df.count()
    negative_units_count = gold_df.filter(F.col("total_units") < 0).count()

    quality_entries = [
        build_quality_entry(
            run_id=active_run_id,
            batch_id=batch_id,
            tenant_id=tenant,
            layer="gold",
            table_name="daily_metrics_by_delivery_type",
            check_name="gold_metrics_non_empty",
            check_severity="critical",
            records_checked=total_rows,
            records_failed=0 if total_rows >= 0 else 1,
        ),
        build_quality_entry(
            run_id=active_run_id,
            batch_id=batch_id,
            tenant_id=tenant,
            layer="gold",
            table_name="daily_metrics_by_delivery_type",
            check_name="gold_metrics_non_negative_units",
            check_severity="critical",
            records_checked=total_rows,
            records_failed=negative_units_count,
        ),
    ]

    log_df = spark.createDataFrame(quality_entries, schema=QUALITY_LOGS_SCHEMA)
    qlogs_target = get_target(cfg, "quality_logs")
    write_delta(log_df, cfg, qlogs_target, mode="append", merge_schema=True)

    return active_run_id
