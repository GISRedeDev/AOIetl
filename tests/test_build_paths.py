import pytest
import geopandas as gpd


from aoietl.build_paths import build_config, list_rasters_for_date, build_tile_index, read_vector_subset
import aoietl.data_types as dt


def test_build_config(test_config):
    config = build_config(test_config)
    assert isinstance(config, dt.DataConfig)
    assert dt.DirectoryType.BRONZE in config.directories
    assert dt.DirectoryType.SILVER in config.directories
    assert dt.DirectoryType.GOLD in config.directories
    assert config.aoi is not None
    assert config.azureRoot is not None
    assert config.output_base is not None


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


def test_build_tile_index(test_config, azure_blob):
    config = build_config(test_config)
    root = azure_blob.joinpath(config.azureRoot)
    rasters = list_rasters_for_date(
        root_path=root,
        tier=dt.DirectoryType.BRONZE.value,
        dataset_name=dt.RasterType.SENTINEL2.value,
        config_date=config.date
    )
    tile_index = build_tile_index(rasters)
    assert isinstance(tile_index, gpd.GeoDataFrame)
    assert not tile_index.empty
    assert all(tile_index.geometry.type == 'Polygon')
    assert len(tile_index) == len(rasters)
    tile_index_paths = tile_index['path'].tolist()
    assert sorted(tile_index_paths) == sorted([str(r) for r in rasters])


def test_filter_tiles_by_aoi(test_config, azure_blob, aoi_gdf):
    config = build_config(test_config)
    root = azure_blob.joinpath(config.azureRoot)
    rasters = list_rasters_for_date(
        root_path=root,
        tier=dt.DirectoryType.BRONZE.value,
        dataset_name=dt.RasterType.SENTINEL2.value,
        config_date=config.date
    )
    tile_index = build_tile_index(rasters)
    
    filtered_tiles = tile_index[tile_index.intersects(aoi_gdf.union_all())]
    
    assert not filtered_tiles.empty
    assert all(filtered_tiles.geometry.type == 'Polygon')
    assert len(filtered_tiles) <= len(tile_index)


def test_read_vector_subset(test_config, azure_blob, aoi_gdf, point_gpkg):
    config = build_config(test_config)
    root = azure_blob.joinpath(config.azureRoot)
    random_points = gpd.read_file(root.joinpath(point_gpkg))
    points_subset = read_vector_subset(
        vector_path=root.joinpath(
            dt.DirectoryType.BRONZE.value, "vector", "random_points.gpkg"
            ),        
        aoi_gdf=aoi_gdf
    )
    assert isinstance(points_subset, gpd.GeoDataFrame)
    assert len(points_subset) < len(random_points)
    assert len(random_points[random_points.intersects(aoi_gdf.union_all())]) == len(points_subset)