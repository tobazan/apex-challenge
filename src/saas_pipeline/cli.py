import uuid

import click
from omegaconf import DictConfig
from pyspark.sql import SparkSession

from saas_pipeline.bronze.deliveries import ingest_deliveries_bronze
from saas_pipeline.bronze.materials import ingest_materials_bronze
from saas_pipeline.common.config import load_config, resolve_target_tenants
from saas_pipeline.common.spark import get_spark_session
from saas_pipeline.gold.daily_metrics_by_delivery_type import daily_metrics_by_delivery_type
from saas_pipeline.silver.deliveries import process_silver_deliveries_for_tenant
from saas_pipeline.silver.materials import process_silver_materials


def run_bronze_layer(spark: SparkSession, cfg: DictConfig, run_id: str) -> None:
    click.echo(f"[BRONZE] Ingesting materials catalog (run_id={run_id})...")
    ingest_materials_bronze(spark, cfg, run_id=run_id)
    click.echo(f"[BRONZE] Ingesting deliveries (run_id={run_id})...")
    ingest_deliveries_bronze(spark, cfg, run_id=run_id)


def run_silver_layer(spark: SparkSession, cfg: DictConfig, run_id: str) -> None:
    click.echo(f"[SILVER] Processing shared dim_materials (run_id={run_id})...")
    process_silver_materials(spark, cfg, run_id=run_id)

    tenants = resolve_target_tenants(cfg)
    errors: list[tuple[str, Exception]] = []
    for tenant in tenants:
        try:
            click.echo(f"[SILVER] Processing deliveries for tenant='{tenant}' (run_id={run_id})...")
            process_silver_deliveries_for_tenant(spark, tenant, cfg, run_id)
        except Exception as exc:
            click.echo(f"[SILVER] ERROR on tenant='{tenant}': {exc}", err=True)
            if cfg.execution.fail_fast:
                raise
            errors.append((tenant, exc))

    if errors:
        failed_list = ", ".join([f"{t}: {e}" for t, e in errors])
        raise RuntimeError(f"Silver layer completed with errors on tenant(s): {failed_list}")


def run_gold_layer(spark: SparkSession, cfg: DictConfig, run_id: str) -> None:
    tenants = resolve_target_tenants(cfg)
    errors: list[tuple[str, Exception]] = []
    for tenant in tenants:
        try:
            click.echo(f"[GOLD] Computing daily metrics for tenant='{tenant}' (run_id={run_id})...")
            daily_metrics_by_delivery_type(spark, tenant, cfg, run_id=run_id)
        except Exception as exc:
            click.echo(f"[GOLD] ERROR on tenant='{tenant}': {exc}", err=True)
            if cfg.execution.fail_fast:
                raise
            errors.append((tenant, exc))

    if errors:
        failed_list = ", ".join([f"{t}: {e}" for t, e in errors])
        raise RuntimeError(f"Gold layer completed with errors on tenant(s): {failed_list}")


@click.command()
@click.option("--env", default="dev", type=click.Choice(["dev", "qa", "main"]), help="Target environment")
@click.option(
    "--layer", default="bronze", type=click.Choice(["bronze", "silver", "gold", "all"]), help="Layer to execute"
)
@click.option(
    "--runtime",
    default="filesystem",
    type=click.Choice(["filesystem", "databricks"]),
    help="Storage runtime mode - local 'filesystem' or Unity Catalog 'databricks'",
)
@click.option("--tenant", default="all", help="Tenant code (e.g., sv, gt) or 'all'")
@click.option("--start-date", default=None, help="Start date (YYYY-MM-DD)")
@click.option("--end-date", default=None, help="End date (YYYY-MM-DD)")
@click.option("--fail-fast/--no-fail-fast", default=None, help="Halt exec immediately on the 1st failed tenant")
@click.option("--fail-on-critical/--no-fail-on-critical", default=None, help="Abort exec if a critical DQ check fails")
def main(
    env: str,
    runtime: str,
    tenant: str,
    start_date: str | None,
    end_date: str | None,
    layer: str,
    fail_fast: bool | None,
    fail_on_critical: bool | None,
):
    # Entrypoint for the pipeline
    cfg = load_config(
        env=env,
        runtime=runtime,
        tenant=tenant,
        start_date=start_date,
        end_date=end_date,
        fail_fast=fail_fast,
        fail_on_critical=fail_on_critical,
    )

    spark = get_spark_session(f"SaaS_Pipeline_{env.upper()}_{layer.upper()}")
    run_id = str(uuid.uuid4())[:8]

    click.echo(f"""[STARTING PIPELINE]
        Config loaded for:
        - runtime={cfg.runtime}
        - env={cfg.environment}
        - tenant={cfg.execution.tenant}
        - layer={layer}
        - run_id={run_id}
    """)

    if layer in ("bronze", "all"):
        run_bronze_layer(spark, cfg, run_id)
    if layer in ("silver", "all"):
        run_silver_layer(spark, cfg, run_id)
    if layer in ("gold", "all"):
        run_gold_layer(spark, cfg, run_id)


if __name__ == "__main__":
    main()
