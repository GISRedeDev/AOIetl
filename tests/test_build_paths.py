import pytest

from aoietl.build_paths import build_config, list_rasters_for_date
import aoietl.data_types as dt


def test_build_config(test_config):
    config = build_config(test_config)
    assert isinstance(config, dt.DataConfig)
    assert dt.DirectoryType.BRONZE in config.directories
    assert dt.DirectoryType.SILVER in config.directories
    assert dt.DirectoryType.GOLD in config.directories
    assert config.aoi is not None
    assert config.azureRoot is not None


def test_build_config_with_wrong_date(config_wrong_date):
    with pytest.raises(ValueError):
        build_config(config_wrong_date)


def test_build_config_missing_azure_root(config_missing_azure_root):
    with pytest.raises(ValueError):
        build_config(config_missing_azure_root)


def test_build_config_missing_aoi(config_missing_aoi):
    with pytest.raises(ValueError):
        build_config(config_missing_aoi)


def test_build_config_missing_directories(config_missing_directories):
    with pytest.raises(ValueError):
        build_config(config_missing_directories)

@pytest.mark.parametrize("dataset_name", [
    dt.RasterType.SENTINEL2.value,
    dt.RasterType.LANDSAT8.value
])
def test_list_rasters_for_date(test_config, azure_blob, dataset_name):
    config = build_config(test_config)
    root = azure_blob.joinpath(config.azureRoot)
    rasters = list_rasters_for_date(
        root_path=root,
        tier=dt.DirectoryType.BRONZE.value,
        dataset_name=dataset_name,
        config_date=config.date
    )
    assert isinstance(rasters, list)
    assert len(rasters) == 25
    if dataset_name == dt.RasterType.SENTINEL2.value:
        assert "S2A_MSIL2A_20250401T015631_R117_T51LWC_20250401T043813_tile00.tif" in [r.name for r in rasters]
        assert "S2A_MSIL2A_20250402T015631_R117_T51LWC_20250402T043813_tile12.tif" not in [r.name for r in rasters]
    else:
        assert "LC08_L2SP_120034_20250401_02_T1_tile00.tif" in [r.name for r in rasters]
        assert "LC08_L2SP_120034_20250402_02_T1_tile12.tif" not in [r.name for r in rasters]