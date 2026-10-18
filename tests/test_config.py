from pathlib import Path

from saas_pipeline.common.config import (
    get_checkpoint_path,
    get_raw_path,
    get_target,
    load_config,
    resolve_target_tenants,
)
from tests.conftest import build_test_cfg


def test_filesystem_target_resolution_in_tmp_path(tmp_path: Path):
    cfg = build_test_cfg(tmp_path, env="dev", runtime="filesystem", tenant="sv")

    assert get_raw_path(cfg, "raw_deliveries") == str(tmp_path / "raw" / "deliveries")
    assert get_raw_path(cfg, "raw_materials") == str(tmp_path / "raw" / "materials")
    assert get_checkpoint_path(cfg, "bronze_mat") == str(tmp_path / "dev" / "_checkpoints" / "bronze_mat")
    assert get_target(cfg, "quality_logs") == str(tmp_path / "dev" / "shared" / "quality_logs")
    assert get_target(cfg, "bronze_materials") == str(tmp_path / "dev" / "bronze" / "shared" / "materials_catalog")
    assert get_target(cfg, "silver_materials") == str(tmp_path / "dev" / "silver" / "shared" / "dim_materials")
    assert get_target(cfg, "quarantine_materials") == str(
        tmp_path / "dev" / "silver_quarantine" / "shared" / "dim_materials"
    )
    assert get_target(cfg, "bronze_deliveries", tenant="SV") == str(tmp_path / "dev" / "bronze" / "sv" / "deliveries")
    assert get_target(cfg, "silver_deliveries", tenant="SV") == str(
        tmp_path / "dev" / "silver" / "sv" / "fact_deliveries"
    )
    assert get_target(cfg, "quarantine_deliveries", tenant="SV") == str(
        tmp_path / "dev" / "silver_quarantine" / "sv" / "fact_deliveries"
    )
    assert get_target(cfg, "gold_daily_metrics", tenant="SV") == str(
        tmp_path / "dev" / "gold" / "sv" / "daily_metrics_by_delivery_type"
    )


# def test_databricks_unity_catalog_target_resolution(tmp_path: Path):
#     cfg = build_test_cfg(tmp_path, env="qa", runtime="databricks", tenant="gt")

#     assert (
#         get_raw_path(cfg, "raw_deliveries")
#         == "abfss://landing@stsaasdatalakeqa.dfs.core.windows.net/raw/deliveries"
#     )
#     assert (
#         get_checkpoint_path(cfg, "bronze_mat")
#         == "abfss://checkpoints@stsaasdatalakeqa.dfs.core.windows.net/qa/bronze_mat"
#     )
#     assert get_target(cfg, "quality_logs") == "saas_qa.governance.quality_logs"
#     assert get_target(cfg, "bronze_materials") == "saas_qa.bronze_shared.materials_catalog"
#     assert get_target(cfg, "silver_materials") == "saas_qa.silver_shared.dim_materials"
#     assert (
#         get_target(cfg, "quarantine_materials")
#         == "saas_qa.silver_quarantine_shared.dim_materials"
#     )
#     assert get_target(cfg, "bronze_deliveries", tenant="gt") == "saas_qa.bronze_gt.deliveries"
#     assert (
#         get_target(cfg, "silver_deliveries", tenant="gt")
#         == "saas_qa.silver_gt.fact_deliveries"
#     )
#     assert (
#         get_target(cfg, "quarantine_deliveries", tenant="gt")
#         == "saas_qa.silver_quarantine_gt.fact_deliveries"
#     )
#     assert (
#         get_target(cfg, "gold_daily_metrics", tenant="gt")
#         == "saas_qa.gold_gt.daily_metrics_by_delivery_type"
#     )


def test_hierarchical_yaml_merge_and_cli_overrides(tmp_path: Path):
    cfg_root = tmp_path / "config"
    (cfg_root / "environments").mkdir(parents=True)
    (cfg_root / "tenants").mkdir(parents=True)

    (cfg_root / "base.yaml").write_text(
        "runtime: filesystem\n"
        "environment: dev\n"
        "execution:\n  tenant: all\n  fail_fast: false\n"
        "quality:\n  fail_on_critical: false\n"
        "registered_tenants: [sv, gt]\n"
        "rules:\n  cs_to_st_factor: 20\n",
        encoding="utf-8",
    )
    (cfg_root / "environments" / "qa.yaml").write_text(
        "environment: qa\nexecution:\n  fail_fast: true\nquality:\n  fail_on_critical: true\n",
        encoding="utf-8",
    )
    (cfg_root / "tenants" / "sv.yaml").write_text(
        "rules:\n  cs_to_st_factor: 24\n",
        encoding="utf-8",
    )

    # Load QA with tenant override (SV overrides cs_to_st_factor from 20 -> 24)
    qa_sv_cfg = load_config(env="qa", runtime="databricks", tenant="sv", config_root=cfg_root)
    assert qa_sv_cfg.environment == "qa"
    assert qa_sv_cfg.runtime == "databricks"
    assert qa_sv_cfg.execution.fail_fast is True
    assert qa_sv_cfg.quality.fail_on_critical is True
    assert qa_sv_cfg.rules.cs_to_st_factor == 24
    assert resolve_target_tenants(qa_sv_cfg) == ["sv"]

    # Explicit CLI flags override YAML defaults
    override_cfg = load_config(
        env="qa",
        tenant="all",
        fail_fast=False,
        fail_on_critical=False,
        config_root=cfg_root,
    )
    assert override_cfg.execution.fail_fast is False
    assert override_cfg.quality.fail_on_critical is False
    assert resolve_target_tenants(override_cfg) == ["sv", "gt"]
