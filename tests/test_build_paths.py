import pytest
import geopandas as gpd
from unittest.mock import Mock, patch
from datetime import datetime
from pathlib import Path
import os
import tempfile
import numpy as np
from shapely.geometry import Polygon


from aoietl.build_paths import (
    build_config,
    list_rasters_for_date,
    build_tile_index,
    read_vector_subset,
    list_hdf_for_date,
    build_hdf_tile_index,
)
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
def test_list_rasters_for_date(test_config, azure_blob, dataset_name, mock_setup_azure_filesystem):
    config = build_config(test_config)
    root = config.tier_roots.bronze
    rasters = list_rasters_for_date(
        root_path=root,
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


def test_build_tile_index(test_config, azure_blob, mock_setup_azure_filesystem):
    config = build_config(test_config)
    root = config.tier_roots.bronze
    rasters = list_rasters_for_date(
        root_path=root,
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


def test_filter_tiles_by_aoi(test_config, azure_blob, aoi_gdf, mock_setup_azure_filesystem):
    config = build_config(test_config)
    root = config.tier_roots.bronze
    rasters = list_rasters_for_date(
        root_path=root,
        dataset_name=dt.RasterType.SENTINEL2.value,
        config_date=config.date
    )
    tile_index = build_tile_index(rasters)
    
    filtered_tiles = tile_index[tile_index.intersects(aoi_gdf.union_all())]
    
    assert not filtered_tiles.empty
    assert all(filtered_tiles.geometry.type == 'Polygon')
    assert len(filtered_tiles) <= len(tile_index)

@pytest.mark.parametrize("vector_path", [
    "random_test_points/random_points.gpkg",
    "random_test_points/random_points.parquet"
])
def test_read_vector_subset(test_config, azure_blob, aoi_gdf, point_gpkg, vector_path):
    config = build_config(test_config)
    root = azure_blob.joinpath(config.azureRoot)
    random_points = gpd.read_file(root.joinpath(point_gpkg))
    points_subset = read_vector_subset(
        vector_path=root.joinpath(
            dt.DirectoryType.BRONZE.value, vector_path
            ),        
        aoi_gdf=aoi_gdf
    )
    assert isinstance(points_subset, gpd.GeoDataFrame)
    assert len(points_subset) < len(random_points)
    assert len(random_points[random_points.intersects(aoi_gdf.union_all())]) == len(points_subset)


def test_list_hdf_for_date_with_temp_files():
    """Test list_hdf_for_date function with actual temporary files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        root_path = Path(temp_dir)
        dataset_name = "test_dataset"
        dataset_dir = root_path / dataset_name
        dataset_dir.mkdir()
        
        # Create test files - 5 matching date 2022-04-25, 5 not matching
        matching_files = [
            "processed_ATL03_20220425090937_05081508_006_02.nc",
            "ATL08_20220425123456_12345678_005_01.hdf",
            "data_20220425_processed.nc",
            "ATL06_20220425000000_87654321_003_02.hdf",
            "test_20220425_final.nc"
        ]
        
        non_matching_files = [
            "processed_ATL03_20220426090937_05081508_006_02.nc",  # different date
            "ATL08_20220424123456_12345678_005_01.hdf",          # different date
            "data_20211225_processed.nc",                        # different date
            "ATL06_20230425000000_87654321_003_02.hdf",          # different date
            "test_file_no_date.nc",                              # no date
            "some_file.txt"                                      # wrong extension
        ]
        
        # Create the files
        all_files = matching_files + non_matching_files
        for filename in all_files:
            (dataset_dir / filename).touch()
        
        config_date = datetime(2022, 4, 25)
        
        # Test the function
        result = list_hdf_for_date(root_path, dataset_name, config_date)
        
        assert len(result) == 5
        result_names = [f.name for f in result]
        assert sorted(result_names) == sorted(matching_files)
        
        # Verify all returned files actually exist and have correct extensions
        for file_path in result:
            assert file_path.exists()
            assert file_path.suffix in ['.hdf', '.nc']



def test_build_hdf_tile_index():
    """Test build_hdf_tile_index function with mocked HDF5 files."""
    
    # Create mock HDF paths
    hdf_paths = [
        Path(f"/path/to/file_{i}.hdf") for i in range(9)
    ]
    
    # Define 9 different tile extents (3x3 grid covering roughly lat: 40-50, lon: -110 to -100)
    tile_extents = [
        # Row 1
        {"lats": np.array([40.0, 40.0, 43.33, 43.33]), "lons": np.array([-110.0, -106.67, -106.67, -110.0])},
        {"lats": np.array([40.0, 40.0, 43.33, 43.33]), "lons": np.array([-106.67, -103.33, -103.33, -106.67])},
        {"lats": np.array([40.0, 40.0, 43.33, 43.33]), "lons": np.array([-103.33, -100.0, -100.0, -103.33])},
        # Row 2
        {"lats": np.array([43.33, 43.33, 46.67, 46.67]), "lons": np.array([-110.0, -106.67, -106.67, -110.0])},
        {"lats": np.array([43.33, 43.33, 46.67, 46.67]), "lons": np.array([-106.67, -103.33, -103.33, -106.67])},
        {"lats": np.array([43.33, 43.33, 46.67, 46.67]), "lons": np.array([-103.33, -100.0, -100.0, -103.33])},
        # Row 3
        {"lats": np.array([46.67, 46.67, 50.0, 50.0]), "lons": np.array([-110.0, -106.67, -106.67, -110.0])},
        {"lats": np.array([46.67, 46.67, 50.0, 50.0]), "lons": np.array([-106.67, -103.33, -103.33, -106.67])},
        {"lats": np.array([46.67, 46.67, 50.0, 50.0]), "lons": np.array([-103.33, -100.0, -100.0, -103.33])},
    ]
    
    def mock_h5py_file_side_effect(path_str, mode):
        """Create a mock HDF5 file based on the path."""
        # Extract index from path
        path_obj = Path(path_str)
        file_index = int(path_obj.stem.split('_')[-1])  # Extract number from file_X.hdf
        
        mock_file = Mock()
        
        # Set up the mock datasets to return the appropriate lat/lon arrays
        extent = tile_extents[file_index]
        
        # Create mock datasets that properly handle array access
        mock_dataset_lat = Mock()
        mock_dataset_lat.__getitem__ = Mock(return_value=extent["lats"])
        
        mock_dataset_lon = Mock()
        mock_dataset_lon.__getitem__ = Mock(return_value=extent["lons"])
        
        # Handle the nested path access pattern
        mock_file.__getitem__ = Mock(side_effect=lambda key: {
            "orbit_info/bounding_polygon_lat1": mock_dataset_lat,
            "orbit_info/bounding_polygon_lon1": mock_dataset_lon
        }[key])
        
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=None)
        
        return mock_file
    
    # Patch h5py.File to return our mock
    with patch('aoietl.build_paths.h5py.File', side_effect=mock_h5py_file_side_effect):
        # Test the function
        result = build_hdf_tile_index(hdf_paths)
    
    # Verify the result
    assert isinstance(result, gpd.GeoDataFrame)
    assert len(result) == 9
    assert result.crs == "EPSG:4326"
    assert all(result.geometry.type == 'Polygon')
    
    # Verify all paths are included
    result_paths = result['path'].tolist()
    expected_paths = [str(p) for p in hdf_paths]
    assert sorted(result_paths) == sorted(expected_paths)
    
    # Verify geometries are valid polygons
    for idx, geom in enumerate(result.geometry):
        assert isinstance(geom, Polygon)
        assert geom.is_valid
        
        # Check that the polygon bounds match the expected extent
        bounds = geom.bounds  # (minx, miny, maxx, maxy)
        extent = tile_extents[idx]
        
        expected_minx = float(np.min(extent["lons"]))
        expected_maxx = float(np.max(extent["lons"]))
        expected_miny = float(np.min(extent["lats"]))
        expected_maxy = float(np.max(extent["lats"]))
        
        assert abs(bounds[0] - expected_minx) < 1e-6  # minx
        assert abs(bounds[1] - expected_miny) < 1e-6  # miny
        assert abs(bounds[2] - expected_maxx) < 1e-6  # maxx
        assert abs(bounds[3] - expected_maxy) < 1e-6  # maxy


def test_build_hdf_tile_index_with_fs():
    """Test build_hdf_tile_index function with filesystem argument."""
    
    hdf_paths = [Path("/path/to/file_0.hdf")]
    
    # Mock filesystem
    mock_fs = Mock()
    mock_file_handle = Mock()
    mock_fs.open.return_value.__enter__ = Mock(return_value=mock_file_handle)
    mock_fs.open.return_value.__exit__ = Mock(return_value=None)
    
    # Mock the HDF5 file
    mock_hdf_file = Mock()
    
    # Set up test data
    test_lats = np.array([40.0, 40.0, 45.0, 45.0])
    test_lons = np.array([-110.0, -105.0, -105.0, -110.0])
    
    # Create mock datasets that properly handle array access
    mock_dataset_lat = Mock()
    mock_dataset_lat.__getitem__ = Mock(return_value=test_lats)
    
    mock_dataset_lon = Mock()
    mock_dataset_lon.__getitem__ = Mock(return_value=test_lons)
    
    # Handle the nested path access pattern
    mock_hdf_file.__getitem__ = Mock(side_effect=lambda key: {
        "orbit_info/bounding_polygon_lat1": mock_dataset_lat,
        "orbit_info/bounding_polygon_lon1": mock_dataset_lon
    }[key])
    
    mock_hdf_file.__enter__ = Mock(return_value=mock_hdf_file)
    mock_hdf_file.__exit__ = Mock(return_value=None)
    
    with patch('aoietl.build_paths.h5py.File', return_value=mock_hdf_file):
        result = build_hdf_tile_index(hdf_paths, fs=mock_fs)
    
    assert len(result) == 1
    assert result.crs == "EPSG:4326"
    mock_fs.open.assert_called_once()


def test_build_hdf_tile_index_mismatched_arrays():
    """Test that function raises error when lat/lon arrays have different lengths."""
    
    hdf_paths = [Path("/path/to/file_0.hdf")]
    
    def mock_h5py_file_side_effect(path_str, mode):
        mock_file = Mock()
        
        # Create mock datasets that properly handle array access
        mock_dataset_lat = Mock()
        mock_dataset_lat.__getitem__ = Mock(return_value=np.array([40.0, 40.0, 45.0]))  # 3 elements
        
        mock_dataset_lon = Mock()
        mock_dataset_lon.__getitem__ = Mock(return_value=np.array([-110.0, -105.0]))    # 2 elements
        
        # Handle the nested path access pattern
        mock_file.__getitem__ = Mock(side_effect=lambda key: {
            "orbit_info/bounding_polygon_lat1": mock_dataset_lat,
            "orbit_info/bounding_polygon_lon1": mock_dataset_lon
        }[key])
        
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=None)
        
        return mock_file
    
    with patch('aoietl.build_paths.h5py.File', side_effect=mock_h5py_file_side_effect):
        with pytest.raises(ValueError, match="have different lengths"):
            build_hdf_tile_index(hdf_paths)