from pathlib import Path
import re
import yaml
import warnings
warnings.filterwarnings("ignore")
import geopandas as gpd
import rasterio
from shapely.geometry import box

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


def list_rasters_for_date(root_path: Path, tier: str, dataset_name: str, config_date) -> list[Path]:
    search_path = Path(root_path) / tier / dataset_name
    raster_files = list(search_path.glob("*.tif"))

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


def build_tile_index(raster_paths: list[Path]) -> gpd.GeoDataFrame:
    """
    Build a GeoDataFrame with bounds polygons for each raster path.
    """
    records = []

    for path in raster_paths:
        with rasterio.open(path) as src:
            bounds = src.bounds
            geom = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
            records.append({"geometry": geom, "path": str(path)})

    gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")  # Assuming rasters already in 4326
    return gdf

# === 3. Filter tiles by AOI ===

def filter_tiles_by_aoi(tile_index_gdf: gpd.GeoDataFrame, aoi_gdf: gpd.GeoDataFrame) -> list[str] | None:
    """
    Return tile index rows that intersect the AOI.
    """
    aoi_geom = aoi_gdf.union_all()
    filtered_gdf = tile_index_gdf[tile_index_gdf.intersects(aoi_geom)]
    return filtered_gdf['path'].tolist() if not filtered_gdf.empty else None

# === 4. Read vector subset ===

def read_vector_subset(vector_path: Path, aoi_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Read only vector features intersecting the AOI.
    """
    aoi_geom = aoi_gdf.union_all()

    # Read only within bbox first (efficient)
    gdf = gpd.read_file(vector_path, bbox=aoi_geom.bounds)

    # Filter precisely by AOI
    gdf = gdf[gdf.intersects(aoi_geom)]
    return gdf