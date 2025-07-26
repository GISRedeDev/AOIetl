from pathlib import Path
import re
import shutil
import yaml
import warnings
from upath import UPath
warnings.filterwarnings("ignore")
import fsspec
import geopandas as gpd
import h5py
import numpy as np
import rasterio
from shapely.geometry import box, Polygon
import tempfile


from .data_types import DataConfig

def build_config(config_yaml_path: str | Path) -> DataConfig:
    """
    Build a DataConfig object from a given path.

    Args:
        path (str | Path): The path to the data directory.

    Returns:
        DataConfig: An instance of DataConfig with the specified path.

    Raises:
        ValueError: If the config file is invalid or missing required elements.
    """
    with open(config_yaml_path, 'r') as file:
        config = yaml.safe_load(file)

    if data_config := config.get('dataConfig'):
        return DataConfig(**data_config)

    raise ValueError("Invalid config format (no `dataConfig` element in yaml). Please check the yaml structure.")


def list_rasters_for_date(root_path: Path | UPath, dataset_name: str, config_date) -> list[Path]:
    raster_files = [x for x in root_path.joinpath(dataset_name).iterdir() if x.is_file() and x.suffix == '.tif']

    matching_files = []
    target_date_str = config_date.strftime("%Y%m%d")

    for f in raster_files:
        name = f.name
        # Extract date depending on dataset
        if "S2" in name:
            # Sentinel-2
            match = re.search(r"S2.\w+_(\d{8})T\d{6}_", name)
        elif "LC" in name:
            # Landsat
            match = re.search(r"LC.._L2SP_\d{6}_(\d{8})_", name)
        else:
            match = None

        if match:
            date_str = match.group(1)
            if date_str == target_date_str:
                matching_files.append(f)

    return matching_files

def make_tile_bounds_geom(src: rasterio.io.DatasetReader) -> Polygon:
    """
    Create a bounds polygon from a rasterio dataset.
    
    Args:
        src (rasterio.io.DatasetReader): The rasterio dataset reader object.
    
    Returns:
        shapely.geometry.Polygon: The bounds polygon of the raster.
    """
    bounds = src.bounds
    return box(bounds.left, bounds.bottom, bounds.right, bounds.top)


def build_tile_index(raster_paths: list[Path | UPath], fs: fsspec.AbstractFileSystem | None = None) -> gpd.GeoDataFrame:
    """
    Build a GeoDataFrame with bounds polygons for each raster path.
    """
    records = []
    raster_crs: str | None = None
    for path in raster_paths:
        if fs:
            with fs.open(path) as f:
                with rasterio.open(f) as src:
                    if not raster_crs:
                        raster_crs = src.crs
                    records.append({"geometry": make_tile_bounds_geom(src), "path": str(path)})
        else:
            with rasterio.open(path) as src:
                if not raster_crs:
                    raster_crs = src.crs
                records.append({"geometry": make_tile_bounds_geom(src), "path": str(path)})

    gdf = gpd.GeoDataFrame(records, crs=raster_crs)  # Assuming rasters already in 4326
    if raster_crs != "EPSG:4326":
        # Reproject to WGS84 if not already in that CRS
        try:
            gdf = gdf.to_crs("EPSG:4326")
        except Exception as e:
            raise ValueError(f"Failed to reproject tile index to EPSG:4326: {e}")
    gdf = gdf.to_crs(4326)
    return gdf


def build_hdf_tile_index(hdf_paths: list[Path | UPath], fs: fsspec.AbstractFileSystem | None = None) -> gpd.GeoDataFrame:
    """
    Build a GeoDataFrame with bounds polygons for each HDF path.
    """
    records = []
    for path in hdf_paths:
        if fs:
            with fs.open(path) as f:
                with h5py.File(f, "r") as hdf_file:
                    lats = f["orbit_info/bounding_polygon_lat1"][:]
                    lons = f["orbit_info/bounding_polygon_lon1"][:]
        else:
            with h5py.File(path, "r") as hdf_file:
                lats = hdf_file["orbit_info/bounding_polygon_lat1"][:]
                lons = hdf_file["orbit_info/bounding_polygon_lon1"][:]
        if len(lats) != len(lons):
            raise ValueError(f"Latitude and longitude arrays in {path} have different lengths.")
        coords = list(zip(lons, lats))
        polygon = Polygon(coords)
        records.append({"geometry": polygon, "path": str(path)})

    return gpd.GeoDataFrame(records, crs="EPSG:4326")


# === 3. Filter tiles by AOI ===

def filter_tiles_by_aoi(tile_index_gdf: gpd.GeoDataFrame, aoi_gdf: gpd.GeoDataFrame, config: DataConfig) -> list[str] | None:
    """
    Return tile index rows that intersect the AOI.
    """
    aoi_geom = aoi_gdf.union_all()
    filtered_gdf = tile_index_gdf[tile_index_gdf.intersects(aoi_geom)]
    if config.azureRoot:
        paths = [Path(x) for x in filtered_gdf['path'].tolist()]
    else:
        paths = [UPath(x) for x in filtered_gdf['path'].tolist()]
    return paths if paths else None

# === 4. Read vector subset ===

def read_vector_subset(vector_path: Path, aoi_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Read only vector features intersecting the AOI.
    """
    aoi_geom = aoi_gdf.union_all()

    if vector_path.suffix == '.gpkg':
        gdf = gpd.read_file(vector_path, bbox=aoi_geom.bounds)
    elif vector_path.suffix == '.parquet':
        gdf = gpd.read_parquet(vector_path, bbox=aoi_geom.bounds)
    else:
        raise ValueError(f"Unsupported vector file type: {vector_path.suffix}. Only .gpkg and .parquet are supported.")
    gdf = gdf[gdf.intersects(aoi_geom)]
    return gdf


def copy_vector_data_from_azure(vector_path: UPath, aoi_gdf: gpd.GeoDataFrame, config: DataConfig) -> None:
    """
    Copy vector files from Azure to local destination.
    """
    aoi_geom = aoi_gdf.union_all()
    # TODO WE NEED TO HANDLE GEOPARQUET FILES HERE
    suffix = vector_path.suffix.lower()
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        # Download blob to temp file
        with config.fs.open(str(vector_path), "rb") as remote_file:
            shutil.copyfileobj(remote_file, tmp)

        tmp_path = tmp.name

    if suffix == '.gpkg':
        gdf = gpd.read_file(tmp_path, bbox=aoi_geom.bounds)
    elif suffix == '.parquet':
        gdf = gpd.read_parquet(tmp_path, bbox=aoi_geom.bounds)

    Path(tmp_path).unlink()
    return gdf[gdf.intersects(aoi_geom)]