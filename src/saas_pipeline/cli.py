import uuid

import click
from omegaconf import DictConfig
from pyspark.sql import SparkSession

from saas_pipeline.bronze_deliveries import ingest_deliveries_bronze
from saas_pipeline.bronze_materials import ingest_materials_bronze
from saas_pipeline.config import load_config
from saas_pipeline.silver_deliveries import process_silver_deliveries_for_tenant
from saas_pipeline.silver_materials import process_silver_materials
from saas_pipeline.spark import get_spark_session


def resolve_target_tenants(cfg: DictConfig) -> list[str]:
    """Returns the list of tenants to process based on execution.tenant."""
    target = cfg.execution.tenant.lower()
    if target == "all":
        return [str(t).lower() for t in cfg.registered_tenants]
    return [target]


def run_silver_layer(spark: SparkSession, cfg: DictConfig, run_id: str) -> None:
    """Executes Silver layer (dim_materials SCD2 + fact_deliveries + quarantine + DQ) across tenants"""
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


@click.command()
@click.option("--env", default="dev", type=click.Choice(["dev", "qa", "main"]), help="Target environment")
@click.option("--tenant", default="all", help="Tenant code (e.g., sv, gt) or 'all'")
@click.option("--start-date", default=None, help="Start date (YYYY-MM-DD)")
@click.option("--end-date", default=None, help="End date (YYYY-MM-DD)")
@click.option(
    "--layer", default="bronze", type=click.Choice(["bronze", "silver", "gold", "all"]), help="Layer to execute"
)
@click.option("--fail-fast/--no-fail-fast", default=None, help="Halt exec immediately on the 1st failed tenant")
@click.option("--fail-on-critical/--no-fail-on-critical", default=None, help="Abort exec if a critical DQ check fails")
def main(
    env: str,
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
        tenant=tenant,
        start_date=start_date,
        end_date=end_date,
        fail_fast=fail_fast,
        fail_on_critical=fail_on_critical,
    )
    click.echo(f"[CONFIG LOADED] for env={cfg.environment}, tenant={cfg.execution.tenant}...")

    spark = get_spark_session()
    run_id = str(uuid.uuid4())[:8]

    if layer in ("bronze", "all"):
        click.echo(f"[BRONZE] Starting streaming ingestion (run_id={run_id})")
        ingest_materials_bronze(spark, cfg, run_id=run_id)
        ingest_deliveries_bronze(spark, cfg, run_id=run_id)
        click.echo("[BRONZE] Ingestion and quality logging complete.")

    if layer in ("silver", "all"):
        click.echo(f"[SILVER] Starting Silver transformations (run_id={run_id})")
        run_silver_layer(spark, cfg, run_id=run_id)
        click.echo("[SILVER] Transformations, quarantine routing and DQ checks complete")


if __name__ == "__main__":
    main()
