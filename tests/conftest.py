import pytest
from pathlib import Path


BASE_DATA = Path(__file__).parent.resolve().joinpath("data")
CONFIG_YAML = BASE_DATA.joinpath("config.yaml")

@pytest.fixture(scope="session")
def test_config():
    return CONFIG_YAML    
