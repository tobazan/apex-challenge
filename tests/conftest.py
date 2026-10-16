import pytest

from src.saas_pipeline.spark import get_spark_session


@pytest.fixture(scope="session")
def spark():
    """Single shared local SparkSession across the entire pytest run"""
    session = get_spark_session(app_name="SAAS_Pipeline_Tests")
    yield session
    session.stop()
