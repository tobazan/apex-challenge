from pathlib import Path
from typing import Optional

from omegaconf import DictConfig, OmegaConf


def load_config(
    env: str = "dev",
    tenant: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    config_dir: str | Path = "config",
    fail_fast: bool | None = None,
    fail_on_critical: bool | None = None,
) -> DictConfig:
    """Loads hierarchical OmegaConf merging base, env, tenant, and CLI overrides"""
    base_path = Path(config_dir)
    base_file = base_path / "base.yaml"
    if not base_file.exists():
        raise FileNotFoundError(f"Base config file not found at {base_file}")

    cfg = OmegaConf.load(base_file)

    # Merge env config
    env_file = base_path / "env" / f"{env}.yaml"
    if not env_file.exists():
        raise FileNotFoundError(f"Environment config file not found at {env_file}")
    env_cfg = OmegaConf.load(env_file)
    cfg = OmegaConf.merge(cfg, env_cfg)

    # Merge tenant config if specific single tenant
    effective_tenant = (tenant or cfg.execution.tenant).lower()
    if effective_tenant != "all":
        tenant_file = base_path / "tenants" / f"{effective_tenant}.yaml"
        if tenant_file.exists():
            tenant_cfg = OmegaConf.load(tenant_file)
            cfg = OmegaConf.merge(cfg, tenant_cfg)

    # Apply CLI overrides
    cli_overrides: dict = {"execution": {}}
    if tenant is not None:
        cli_overrides["execution"]["tenant"] = effective_tenant
    if start_date is not None:
        cli_overrides["execution"]["start_date"] = start_date
    if end_date is not None:
        cli_overrides["execution"]["end_date"] = end_date
    if fail_fast is not None:
        cli_overrides["execution"]["fail_fast"] = fail_fast
    if fail_on_critical is not None:
        cli_overrides["quality"]["fail_on_critical"] = fail_on_critical

    if cli_overrides["execution"]:
        cfg = OmegaConf.merge(cfg, OmegaConf.create(cli_overrides))

    return cfg
