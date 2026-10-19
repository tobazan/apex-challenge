from pathlib import Path

from omegaconf import DictConfig, OmegaConf


def load_config(
    env: str = "dev",
    runtime: str = "filesystem",
    tenant: str = "all",
    start_date: str | None = None,
    end_date: str | None = None,
    fail_fast: bool | None = None,
    fail_on_critical: bool | None = None,
    config_root: Path | None = None,
) -> DictConfig:
    """Loads and merges base.yaml -> environments/{env}.yaml -> tenants/{tenant}.yaml."""
    root = config_root or Path(__file__).resolve().parents[3] / "config"

    base_cfg = OmegaConf.load(root / "base.yaml")
    env_path = root / "env" / f"{env}.yaml"
    env_cfg = OmegaConf.load(env_path) if env_path.exists() else OmegaConf.create({})

    tenant_cfg = OmegaConf.create({})
    if tenant != "all":
        t_path = root / "tenants" / f"{tenant.lower()}.yaml"
        if t_path.exists():
            tenant_cfg = OmegaConf.load(t_path)

    cfg = OmegaConf.merge(base_cfg, env_cfg, tenant_cfg)

    # Apply CLI overrides
    cfg.environment = env
    cfg.runtime = runtime
    cfg.execution.tenant = tenant
    if start_date is not None:
        cfg.execution.start_date = start_date
    if end_date is not None:
        cfg.execution.end_date = end_date
    if fail_fast is not None:
        cfg.execution.fail_fast = fail_fast
    if fail_on_critical is not None:
        cfg.quality.fail_on_critical = fail_on_critical

    return cfg


def resolve_target_tenants(cfg: DictConfig) -> list[str]:
    """Returns the list of tenants to process based on execution.tenant."""
    if cfg.execution.tenant == "all":
        return [str(t).lower() for t in cfg.registered_tenants]
    return [str(cfg.execution.tenant).lower()]


def get_raw_path(cfg: DictConfig, source_key: str) -> str:
    """Returns raw_deliveries or raw_materials path for the active runtime."""
    return str(cfg.storage[cfg.runtime][source_key])


def get_checkpoint_path(cfg: DictConfig, name: str) -> str:
    """Returns structured streaming checkpoint path for the active runtime."""
    base = str(cfg.storage[cfg.runtime].checkpoints)
    return f"{base}/{name}"


def get_target(cfg: DictConfig, target_key: str, tenant: str | None = None) -> str:
    """
    Resolves a shared or tenant-scoped target (filesystem path or UC 3-level table name).
    - Shared keys: 'quality_logs', 'bronze_materials', 'silver_materials', 'quarantine_materials'
    - Tenant keys: 'bronze_deliveries', 'silver_deliveries', 'quarantine_deliveries', 'gold_daily_metrics'
    """
    targets = cfg.storage[cfg.runtime].targets
    if tenant is None or tenant == "shared":
        return str(targets.shared[target_key])
    template = str(targets.tenant[target_key])
    return template.format(tenant=tenant.lower())
