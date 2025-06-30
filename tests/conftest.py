import pytest
import os
from unittest.mock import patch, MagicMock
from pathlib import Path

import geopandas as gpd
from aoietl.data_types import DataConfig, TierRoots, DirectoryType

BASE_DATA = Path(__file__).parent.resolve().joinpath("data")
CONFIG_YAML = BASE_DATA.joinpath("config.yaml")

@pytest.fixture(scope="session")
def test_config():
    return CONFIG_YAML

@pytest.fixture
def mock_setup_azure_filesystem():
    """Mock setup_azure_filesystem to return local paths instead of Azure paths"""
    
    def mock_setup(config: DataConfig) -> TierRoots:
        """
        Mock version that returns local paths pointing to tests/data/{tier}/
        """
        tier_roots = TierRoots()
        for directory_type in DirectoryType:
            # Point to local test data directory
            base_path = BASE_DATA.joinpath(directory_type.value)
            setattr(tier_roots, directory_type.value, base_path)
        return tier_roots
    
    with patch('aoietl.data_types.setup_azure_filesystem', side_effect=mock_setup):
        yield mock_setup

@pytest.fixture(scope="session")
def mock_azure_env():
    """Mock Azure environment variables"""
    with patch.dict(os.environ, {
        'AZURE_ACCOUNT_NAME': 'test_account',
        'AZURE_ACCOUNT_KEY': 'test_key'
    }):
        yield

@pytest.fixture
def mock_azure_env_unset():
    """Unset Azure environment variables"""
    with patch.dict(os.environ, {}, clear=True):
        yield


@pytest.fixture
def azure_blob():
    # used for testing Azure Blob Storage interactions
    return BASE_DATA


@pytest.fixture
def config_wrong_date(tmp_path):
    yaml_path = tmp_path / "wrong_date.yaml"
    yaml_path.write_text(
        """
dataConfig:
  date: not-a-date
  azureRoot: ./
  aoi: aoi.shp
  directories: []
"""
    )
    return yaml_path

@pytest.fixture
def config_missing_azure_root(tmp_path):
    yaml_path = tmp_path / "missing_azure_root.yaml"
    yaml_path.write_text(
        """
dataConfig:
  date: 2025-03-19
  aoi: aoi.shp
  directories: []
"""
    )
    return yaml_path

@pytest.fixture
def config_missing_aoi(tmp_path):
    yaml_path = tmp_path / "missing_aoi.yaml"
    yaml_path.write_text(
        """
dataConfig:
  date: 2025-03-19
  azureRoot: ./
  directories: []
"""
    )
    return yaml_path

@pytest.fixture
def config_missing_directories(tmp_path):
    yaml_path = tmp_path / "missing_directories.yaml"
    yaml_path.write_text(
        """
dataConfig:
  date: 2025-03-19
  azureRoot: ./
  aoi: aoi.shp
"""
    )
    return yaml_path

@pytest.fixture
def aoi_gdf():
    return gpd.read_file(BASE_DATA.joinpath("aoi", "aoi_test.gpkg"))

@pytest.fixture
def point_gpkg():
    return "bronze/random_test_points/random_points.gpkg"