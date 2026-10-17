import uuid

import click

from saas_pipeline.bronze_deliveries import ingest_deliveries_bronze
from saas_pipeline.bronze_materials import ingest_materials_bronze
from saas_pipeline.config import load_config
from saas_pipeline.spark import get_spark_session


@click.command()
@click.option("--env", default="dev", type=click.Choice(["dev", "qa", "main"]), help="Target environment")
@click.option("--tenant", default="all", help="Tenant code (e.g., sv, gt) or 'all'")
@click.option("--start-date", default=None, help="Start date (YYYY-MM-DD)")
@click.option("--end-date", default=None, help="End date (YYYY-MM-DD)")
@click.option(
    "--layer",
    default="bronze",
    type=click.Choice(["bronze", "silver", "gold", "all"]),
    help="Medallion layer to execute",
)
def main(env, tenant, start_date, end_date, layer):
    # Entrypoint for the pipeline
    cfg = load_config(env=env, tenant=tenant, start_date=start_date, end_date=end_date)
    click.echo(f"[CONFIG LOADED] for env={cfg.environment}, tenant={cfg.execution.tenant}...")

    spark = get_spark_session()
    run_id = str(uuid.uuid4())[:8]

    if layer in ("bronze", "all"):
        click.echo(f"[BRONZE] Starting streaming ingestion (run_id={run_id})")
        ingest_materials_bronze(spark, cfg, run_id=run_id)
        ingest_deliveries_bronze(spark, cfg, run_id=run_id)
        click.echo("[BRONZE] Ingestion and quality logging complete.")


if __name__ == "__main__":
    main()
