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
from upath import UPath

from .build_paths import (
    build_config,
    list_rasters_for_date,
    build_tile_index,
    filter_tiles_by_aoi,
    read_vector_subset,
)

from .data_types import DataConfig, DirectoryType, DirectoryContent
from .validation import validate_directories


logger = structlog.get_logger(__name__)


def process(config_path: Path, azure_blob: Path, local_dir: Path, error_for_missing_files: bool = False) -> None:
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
        BASE_AZURE_DIR = getattr(config.tier_roots, tier.value, None)      
        if directory_content.raster:
            process_rasters(directory_content, tier, aoi_gdf, BASE_AZURE_DIR, BASE_OUT_DIR, config)
        if directory_content.vector:
            process_vectors(
                directory_content,
                tier,
                aoi_gdf,
                BASE_AZURE_DIR,
                BASE_OUT_DIR,
                error_for_missing_files
            )

def process_vectors(
        directory_content: DirectoryContent,
        tier: DirectoryType,
        aoi_gdf: gpd.GeoDataFrame,
        BASE_DIR: Path,
        BASE_OUT_DIR: Path,
        error_for_missing_files: bool = False
) -> None:
    """
    Process vector files by reading them, subsetting by AOI, and copying to output directory.

    Args:
        directory_content (DirectoryContent): Content of the directory for the current tier.
        tier (DirectoryType): The current processing tier.
        aoi_gdf (gpd.GeoDataFrame): Area of Interest as a GeoDataFrame.
        BASE_DIR (Path): Base directory where vector files are located.
        BASE_OUT_DIR (Path): Base output directory where processed files will be saved.
        config (DataConfig): Configuration object containing processing parameters.
        error_for_missing_files (bool): If True, raise an error if vector files are missing.
    """
    for vector_file in directory_content.vector:
        vector_path = BASE_DIR.joinpath(tier.value, vector_file.name)
        if vector_path.exists():
            logger.info("Reading vector file", vector_file=vector_file.name, tier=tier.value)
            gdf = read_vector_subset(
                vector_path=vector_path,
                aoi_gdf=aoi_gdf
            )
            if gdf.empty:
                logger.warning(
                    "Vector file is empty after AOI filtering",
                    vector_file=vector_file.name,
                    tier=tier.value
                )
            output_vector_path = BASE_OUT_DIR.joinpath(tier.value, vector_file.name)
            output_vector_path.parent.mkdir(parents=True, exist_ok=True)
            if vector_path.suffix == '.gpkg':
                gdf.to_file(output_vector_path, driver='GPKG')
            elif vector_path.suffix == '.parquet':
                gdf.to_parquet(output_vector_path, engine='pyarrow')
            logger.info("Vector file copied", output_vector=output_vector_path)
        else:
            if error_for_missing_files:
                logger.error(
                    "Vector file not found",
                    vector_file=vector_file.name,
                    tier=tier.value
                )
                raise FileNotFoundError(f"Vector file {vector_file.name} not found in {tier.value} tier.")
            logger.warning(
                "Vector file not found",
                vector_file=vector_file.name,
                tier=tier.value
            )

def process_rasters(
        directory_content: DirectoryContent,
        tier: DirectoryType,
        aoi_gdf: gpd.GeoDataFrame,
        BASE_DIR: UPath | Path,
        BASE_OUT_DIR: UPath | Path,
        config: DataConfig,
        error_for_missing_files: bool = False
) -> None:
    """
    Process raster files by listing them for a specific date, building a tile index,
    filtering by AOI, and copying to output directory.
    Args:
        directory_content (DirectoryContent): Content of the directory for the current tier.
        tier (DirectoryType): The current processing tier.
        aoi_gdf (gpd.GeoDataFrame): Area of Interest as a GeoDataFrame.
        BASE_DIR (UPath | Path): Base directory where raster files are located.
        BASE_OUT_DIR (UPath | Path): Base output directory where processed files will be saved.
        config (DataConfig): Configuration object containing processing parameters.
        error_for_missing_files (bool): If True, raise an error if raster files are missing
    """
    for raster_type in directory_content.raster:
        rasters = list_rasters_for_date(
            root_path=BASE_DIR,
            tier=tier.value,
            dataset_name=raster_type,
            config_date=config.date
        )
        if rasters:
            tile_index = build_tile_index(rasters, config.fs)
            filtered_tiles = filter_tiles_by_aoi(tile_index, aoi_gdf)
            if filtered_tiles:
                logger.info("Copying files to output", output_base=config.output_base)
                copy_raster_files(filtered_tiles, BASE_OUT_DIR, tier, raster_type)
        else:
            if error_for_missing_files:
                logger.error(
                    "No raster files found for the specified date and type.",
                    raster_type=raster_type,
                    date=config.date,
                    tier=tier
                )
                raise FileNotFoundError(
                    f"No raster files found for {raster_type} on {config.date} in tier {tier.value}."
                )
            logger.warning(
                "No rasters found",
                raster_type=raster_type,
                date=config.date,
                tier=tier
            )

def copy_raster_files(files: list[UPath | Path | str], output_dir: Path, tier: DirectoryType, raster_type: str) -> None:
    for f in files:
        dest = output_dir.joinpath(tier.value, raster_type, Path(f).name)
        if not dest.parent.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
        with f.open('rb') as src, open(dest, 'wb') as dest:
            shutil.copyfileobj(src, dest)
        if raster_type == "landsat":
            dest_json = dest.with_suffix('.json')
            try:
                with f.with_suffix(".json").open('rb') as src_json, open(dest_json, 'wb') as dest_json:
                    shutil.copyfileobj(src_json, dest_json)
            except FileNotFoundError:
                logger.warning(
                    "JSON file not found for raster",
                    raster_file=f,
                    json_file=dest_json,
                    tier=tier.value,
                    raster_type=raster_type
                )