from saas_pipeline.config import load_config


def test_omegaconf_loading_and_overrides():
    # TEST 1 - Default dev load
    cfg_dev = load_config(env="dev")
    assert cfg_dev.environment == "dev"
    assert cfg_dev.paths.bronze == "data/bronze"
    assert cfg_dev.execution.tenant == "all"
    assert cfg_dev.quality.fail_on_critical is False
    assert set(cfg_dev.registered_tenants) == {"sv", "hn", "ec", "jm", "gt", "pe"}

    # TEST 2 - QA env + tenant override + CLI date overrides
    cfg_qa = load_config(
        env="qa",
        tenant="JM",
        start_date="2025-03-01",
        end_date="2025-03-31",
    )
    assert cfg_qa.environment == "qa"
    assert cfg_qa.paths.bronze == "data/qa/bronze"
    assert cfg_qa.quality.fail_on_critical is True
    assert cfg_qa.execution.tenant == "jm"
    assert cfg_qa.execution.start_date == "2025-03-01"
    assert cfg_qa.execution.end_date == "2025-03-31"
    assert cfg_qa.rules.cs_to_st_factor == 20
