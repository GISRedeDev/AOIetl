# Workflow
# 1. Load the configuration from a YAML file.
# 2. Validate the directories in the configuration.
# 3. Open the AOI as a GeoDataFrame.
# 4. If output dir doesn't exist, create it.
# 5. For each tier (bronze, silver, gold, platinum):
    # - for each raster type:
        #   - List raster files for a specific date for each raster type.
        #   - Build a tile index for the raster files.
        #   - Filter the tile index by the Area of Interest (AOI).
        #   - Copy files from the source directories to the output directory based on the configuration.
    # - For each vector type:
        # - Read vector file, subset it by AOI, and copy to output directory.
# TODO HDF parquet and geoparquet
from pathlib import Path
import structlog
import shutil
import warnings
warnings.filterwarnings("ignore")
import geopandas as gpd

from .build_paths import (
    build_config,
    list_rasters_for_date,
    build_tile_index,
    filter_tiles_by_aoi,
)

from .data_types import DataConfig, DirectoryType, RasterType
from .validation import validate_directories


logger = structlog.get_logger(__name__)


def process(config_path: Path, azure_blob: Path, local_dir: Path) -> None:
    """
    Main processing function to handle the ETL workflow based on the provided configuration.

    Args:
        config_path (Path): Path to the YAML configuration file.
        azure_blob (Path): Path to the Azure Blob storage root directory (where it is mounted).
        local_dir (Path): Local directory where output files will be copied.
    """
    config = build_config(config_path)
    validate_directories(config)
    BASE_DIR = azure_blob.joinpath(config.azureRoot)
    BASE_OUT_DIR = local_dir.joinpath(config.output_base)
    aoi_gdf = gpd.read_file(BASE_DIR.joinpath(config.aoi))
    for tier, directory_content in config.directories.items():
        logger.info("Processing tier", tier=tier)
        if directory_content.raster:
            for raster_type in directory_content.raster:
                rasters = list_rasters_for_date(
                    root_path=BASE_DIR,
                    tier=tier.value,
                    dataset_name=raster_type,
                    config_date=config.date
                )
                if rasters:
                    tile_index = build_tile_index(rasters)
                    filtered_tiles = filter_tiles_by_aoi(tile_index, aoi_gdf)
                    if filtered_tiles:
                        logger.info("Copying files to output", output_base=config.output_base)
                        copy_raster_files(filtered_tiles, BASE_OUT_DIR, tier, raster_type)
                else:
                    logger.warning(
                        "No rasters found",
                        raster_type=raster_type,
                        date=config.date,
                        tier=tier
                    )


def copy_raster_files(files: list[Path | str], output_dir: Path, tier: DirectoryType, raster_type: str) -> None:
    for f in files:
        dest = output_dir.joinpath(tier.value, raster_type, Path(f).name)
        if not dest.parent.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(f, dest)