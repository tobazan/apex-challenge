import click

from saas_pipeline.config import load_config


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
def main(env, tenant, start_date, end_date): # Add medallion layer param later
    # Entrypoint for the pipeline
    cfg = load_config(env=env, tenant=tenant, start_date=start_date, end_date=end_date)
    click.echo(f"[CONFIG LOADED] for env={cfg.environment}, tenant={cfg.execution.tenant}...")

if __name__ == "__main__":
    main()
