from pathlib import Path

import pytest
from omegaconf import DictConfig, OmegaConf
from pyspark.sql import SparkSession

from saas_pipeline.common.spark import get_spark_session


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return get_spark_session("SaaS_Pipeline_Test_Suite")


def build_test_cfg(
    tmp_path: Path,
    env: str = "dev",
    runtime: str = "filesystem",
    tenant: str = "all",
    fail_fast: bool = False,
    fail_on_critical: bool = False,
) -> DictConfig:
    """
    Constructs an isolated OmegaConf DictConfig rooted inside pytest's tmp_path
    Matches the exact schema expected by config.py and delta.py helpers without
    touching the real project data/ directory.
    """
    raw_root = str(tmp_path / "raw")
    env_root = str(tmp_path / env)

    return OmegaConf.create(
        {
            "project": {"name": "saas-medallion-pipeline-test"},
            "runtime": runtime,
            "environment": env,
            "execution": {
                "tenant": tenant,
                "start_date": "2025-01-01",
                "end_date": "2025-06-30",
                "fail_fast": fail_fast,
            },
            "quality": {
                "fail_on_critical": fail_on_critical,
            },
            "registered_tenants": ["sv", "hn", "ec", "jm", "gt", "pe"],
            "rules": {
                "cs_to_st_factor": 20,
                "routine_delivery_types": ["ZPRE", "ZVE1"],
                "bonus_delivery_types": ["Z04", "Z05"],
            },
            "storage": {
                "filesystem": {
                    "raw_root": raw_root,
                    "env_root": env_root,
                    "raw_deliveries": f"{raw_root}/deliveries",
                    "raw_materials": f"{raw_root}/materials",
                    "checkpoints": f"{env_root}/_checkpoints",
                    "targets": {
                        "shared": {
                            "quality_logs": f"{env_root}/shared/quality_logs",
                            "bronze_materials": f"{env_root}/bronze/shared/materials_catalog",
                            "silver_materials": f"{env_root}/silver/shared/dim_materials",
                            "quarantine_materials": f"{env_root}/silver_quarantine/shared/dim_materials",
                        },
                        "tenant": {
                            "bronze_deliveries": f"{env_root}/bronze/{{tenant}}/deliveries",
                            "silver_deliveries": f"{env_root}/silver/{{tenant}}/fact_deliveries",
                            "quarantine_deliveries": f"{env_root}/silver_quarantine/{{tenant}}/fact_deliveries",
                            "gold_daily_metrics": f"{env_root}/gold/{{tenant}}/daily_metrics_by_delivery_type",
                        },
                    },
                },
            },
        }
    )
