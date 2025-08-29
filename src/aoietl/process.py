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
import os
import structlog
import shutil
import warnings
warnings.filterwarnings("ignore")
import geopandas as gpd
import pandas as pd
from upath import UPath
from azure.storage.blob import BlobServiceClient

from .build_paths import (
    build_config,
    list_rasters_for_date,
    build_tile_index,
    filter_tiles_by_aoi,
    read_vector_subset,
    copy_vector_data_from_azure,
    list_hdf_for_date,
    build_hdf_tile_index
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
    print("AZURE BLOB", azure_blob)
    print("LOCAL DIR", local_dir)
    BASE_OUT_DIR = local_dir.joinpath(config.output_base)
    aoi_gdf = gpd.read_file(local_dir.joinpath(config.aoi))
    for tier, directory_content in config.directories.items():
        logger.info("Processing tier", tier=tier)
        #BASE_AZURE_DIR = getattr(config.tier_roots, tier.value, None) 
        BASE_TIER_DIR = azure_blob.joinpath(config.azureRoot, tier.value)     
        if directory_content.raster:
            process_rasters_using_paths(directory_content, tier, aoi_gdf, BASE_TIER_DIR, BASE_OUT_DIR, config)
        if directory_content.vector:
            process_vectors(
                directory_content,
                tier,
                aoi_gdf,
                BASE_TIER_DIR,
                BASE_OUT_DIR,
                config,
                error_for_missing_files
            )
        if directory_content.hdf:
            process_hdf_files_using_paths(
                directory_content,
                tier,
                aoi_gdf,
                BASE_TIER_DIR,
                BASE_OUT_DIR,
                config,
                error_for_missing_files
            )
        if directory_content.parquet:
            copy_parquet_files(
                directory_content,
                BASE_TIER_DIR,
                BASE_OUT_DIR,
                tier,
                config
            )
        if directory_content.table:
            copy_csv_files(
                directory_content,
                BASE_TIER_DIR,
                BASE_OUT_DIR,
                tier,
                config
            )
    copy_reference_blob_to_local(local_dir.joinpath("reference"))

def process_fsspec(config_path: Path, local_dir: Path, error_for_missing_files: bool = False) -> None:
    """
    Process function for fsspec-based configuration.

    Args:
        config_path (Path): Path to the YAML configuration file.
        local_dir (Path): Local directory where output files will be copied.
        error_for_missing_files (bool): If True, raise an error if files are missing.
    """
    config = build_config(config_path)
    validate_directories(config)
    #BASE_DIR = config.azureRoot
    BASE_OUT_DIR = local_dir.joinpath(config.output_base)
    aoi_gdf = gpd.read_file(local_dir.joinpath(config.aoi))
    for tier, directory_content in config.directories.items():
        logger.info("Processing tier", tier=tier)
        #BASE_TIER_DIR = BASE_DIR.joinpath(tier.value)
        BASE_TIER_DIR = getattr(config.tier_roots, tier.value, None) 
        if directory_content.raster:
            process_rasters_using_paths(
                directory_content,
                tier,
                aoi_gdf,
                BASE_TIER_DIR,
                BASE_OUT_DIR,
                config,
                error_for_missing_files
            )
        if directory_content.vector:
            process_vectors(
                directory_content,
                tier,
                aoi_gdf,
                BASE_TIER_DIR,
                BASE_OUT_DIR,
                config,
                error_for_missing_files
            )
        if directory_content.hdf:
            pass  # TODO: Implement HDF processing
        if directory_content.parquet:
            pass  # TODO: Implement parquet processing
        if directory_content.table:
            pass  # TODO: Implement table processing

def process_vectors(
        directory_content: DirectoryContent,
        tier: DirectoryType,
        aoi_gdf: gpd.GeoDataFrame,
        BASE_DIR: Path | UPath,
        BASE_OUT_DIR: Path,
        config: DataConfig,
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
        vector_path = BASE_DIR.joinpath(vector_file.name)
        if vector_path.exists():
            logger.info("Reading vector file", vector_file=vector_file.name, tier=tier.value)
            if config.azureRoot:
                gdf = read_vector_subset(
                    vector_path=vector_path,
                    aoi_gdf=aoi_gdf
                )
            else:
                gdf = copy_vector_data_from_azure(vector_path, aoi_gdf, config)
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

def process_rasters_using_paths(
        directory_content: DirectoryContent,
        tier: DirectoryType,
        aoi_gdf: gpd.GeoDataFrame,
        BASE_DIR: Path,
        BASE_OUT_DIR: Path,
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
    rasters: list[Path | str] = []
    for raster_type in directory_content.raster:
        try:
            rasters = list_rasters_for_date(
                root_path=BASE_DIR,
                dataset_name=raster_type,
                config_date=config.date
            )
        except FileNotFoundError as e:
            logger.error(
                "Error listing rasters for date",
                raster_type=raster_type,
                date=config.date,
                tier=tier.value,
                error=str(e)
            )
            if error_for_missing_files:
                raise e
        if rasters:
            tile_index = build_tile_index(rasters)
            filtered_tiles = filter_tiles_by_aoi(tile_index, aoi_gdf, config)
            if filtered_tiles:
                logger.info("Copying files to output", output_base=config.output_base)
                copy_raster_files(filtered_tiles, BASE_OUT_DIR, tier, raster_type, config)
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

def copy_raster_files(files: list[UPath | Path | str], output_dir: Path, tier: DirectoryType, raster_type: str, config: DataConfig) -> None:
    for f in files:
        dest = output_dir.joinpath(tier.value, raster_type, Path(f).name)
        if not dest.parent.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
        if config.azureRoot:
            with f.open('rb') as src, open(dest, 'wb') as dest_file:
                shutil.copyfileobj(src, dest_file)
            if raster_type == "landsat":
                dest_json = f.with_suffix('.json')
                try:
                    with f.with_suffix(".json").open('rb') as src_json, open(dest_json, 'wb') as dest_json_file:
                        shutil.copyfileobj(src_json, dest_json_file)
                except FileNotFoundError:
                    logger.warning(
                        "JSON file not found for raster",
                        raster_file=f,
                        json_file=dest_json,
                        tier=tier.value,
                        raster_type=raster_type
                    )
        else:
            with config.fs.open(str(f), 'rb') as src, open(dest, 'wb') as dest_file:
                shutil.copyfileobj(src, dest_file)
            if raster_type == "landsat":
                dest_json = f.with_suffix('.json')
                try:
                    with config.fs.open(str(f.with_suffix(".json")), 'rb') as src_json, open(dest_json, 'wb') as dest_json_file:
                        shutil.copyfileobj(src_json, dest_json_file)
                except FileNotFoundError:
                    logger.warning(
                        "JSON file not found for raster",
                        raster_file=f,
                        json_file=dest_json,
                        tier=tier.value,
                        raster_type=raster_type
                    )
        logger.info("Raster file copied", raster_file=f, output_path=dest, tier=tier.value, raster_type=raster_type)

def process_hdf_files_using_paths(
        directory_content: DirectoryContent,
        tier: DirectoryType,
        aoi_gdf: gpd.GeoDataFrame,
        BASE_DIR: Path,
        BASE_OUT_DIR: Path,
        config: DataConfig,
        error_for_missing_files: bool = False
        ) -> None:
    hdf_tiles = []
    for hdf_type in directory_content.hdf:
        try:
            hdf_tiles = list_hdf_for_date(
                root_path=BASE_DIR,
                dataset_name=hdf_type,
                config_date=config.date
            )
        except FileNotFoundError as e:
            logger.error(
                "Error listing HDF files for date",
                hdf_type=hdf_type,
                date=config.date,
                tier=tier.value,
                error=str(e)
            )
            if error_for_missing_files:
                raise e
        if hdf_tiles:
            tile_index = build_hdf_tile_index(hdf_tiles, config.fs)
            filtered_tiles = filter_tiles_by_aoi(tile_index, aoi_gdf, config)
            if filtered_tiles:
                logger.info("Copying HDF files to output", output_base=config.output_base)
                copy_raster_files(filtered_tiles, BASE_OUT_DIR, tier, hdf_type, config)
        else:
            if error_for_missing_files:
                logger.error(
                    "No HDF files found for the specified date and type.",
                    hdf_type=hdf_type,
                    date=config.date,
                    tier=tier
                )
                raise FileNotFoundError(
                    f"No HDF files found for {hdf_type} on {config.date} in tier {tier.value}."
                )
            logger.warning(
                "No HDF files found",
                hdf_type=hdf_type,
                date=config.date,
                tier=tier
            )

def copy_csv_files(
        directory_content: DirectoryContent,
        BASE_DIR: Path,
        BASE_OUT_DIR: Path,
        tier: DirectoryType,
        config: DataConfig
) -> None:
    """
    Copy CSV files from the source directories to the output directory based on the configuration.
    """
    for csv_file in directory_content.table:
        # Get the actual file path from the TabularFilename object
        if config.azureRoot:
            #BASE_TIER_DIR = Path(config.azureRoot).joinpath(tier.value)
            source_path = BASE_DIR.joinpath(csv_file.name)
        else:
            BASE_TIER_DIR = getattr(config.tier_roots, tier.value, None)
            source_path = BASE_TIER_DIR.joinpath(csv_file.name)
        
        csv_path = BASE_OUT_DIR.joinpath(tier.value, csv_file.name)
        if not csv_path.parent.exists():
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            
        if config.azureRoot:            
            shutil.copy(source_path, csv_path)            
        else:
            with config.fs.open(str(source_path), 'rb') as src, open(csv_path, 'wb') as dest_file:
                shutil.copyfileobj(src, dest_file)
        logger.info("CSV file copied", csv_file=csv_file.name, output_path=csv_path, tier=tier.value)


def copy_parquet_files(
        directory_content: DirectoryContent,
        BASE_DIR: Path,
        BASE_OUT_DIR: Path,
        tier: DirectoryType,
        config: DataConfig
) -> None:
    """
    Copy Parquet files from the source directories to the output directory based on the configuration.

    Args:
        directory_content (DirectoryContent): Content of the directory for the current tier.
        BASE_OUT_DIR (Path): Base output directory where processed files will be saved.
        tier (DirectoryType): The current processing tier.
        config (DataConfig): Configuration object containing processing parameters.
    """
    for parquet_file in directory_content.parquet:
        # Get the actual file path from the ParquetFileName object
        if config.azureRoot:
            # For local/mounted Azure, construct the path
            #BASE_TIER_DIR = Path(config.azureRoot).joinpath(tier.value)
            source_path = BASE_DIR.joinpath(parquet_file.name)
        else:
            # For fsspec Azure, use tier_roots
            BASE_TIER_DIR = getattr(config.tier_roots, tier.value, None)
            source_path = BASE_TIER_DIR.joinpath(parquet_file.name)
        
        parquet_path = BASE_OUT_DIR.joinpath(tier.value, parquet_file.name)
        if not parquet_path.parent.exists():
            parquet_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Copy the file first
        if config.azureRoot:
            shutil.copy(source_path, parquet_path)
        else:
            with config.fs.open(str(source_path), 'rb') as src, open(parquet_path, 'wb') as dest_file:
                shutil.copyfileobj(src, dest_file)
        
        # Filter by date after copying
        try:
            target_date = config.date.strftime("%Y-%m-%d")
            date_filter = [('date', '==', target_date)]
            df = pd.read_parquet(parquet_path, engine='pyarrow', filters=date_filter)
            df.to_parquet(parquet_path, engine='pyarrow', index=False)
            
            logger.info("Parquet file copied and filtered", 
                       parquet_file=parquet_file.name, 
                       output_path=parquet_path, 
                       tier=tier.value,
                       filtered_rows=len(df))
        except Exception as e:
            logger.warning(
                "Failed to filter Parquet file by date",
                parquet_file=parquet_file.name,
                config_date=config.date,
                error=str(e),
                tier=tier.value
            )


def copy_reference_blob_to_local(local_reference_dir: Path):
    """
    Recursively copy all files from the 'reference' blob container to a local directory.

    Args:
        local_reference_dir (Path): Local directory to copy blobs into.
    """
    account_name = os.getenv("AZURE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_ACCOUNT_KEY")
    container_name = "reference"

    if not account_name or not account_key:
        raise ValueError("AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY must be set")

    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={account_name};"
        f"AccountKey={account_key};"
        f"EndpointSuffix=core.windows.net"
    )
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service.get_container_client(container_name)

    logger.info("Copying all blobs from 'reference' container to local directory", local_dir=str(local_reference_dir))

    blobs = container_client.list_blobs()
    for blob in blobs:
        local_path = local_reference_dir / blob.name
        parent = local_path.parent
        if parent.exists() and not parent.is_dir():
            logger.error(f"Cannot create directory {parent}: a file with that name already exists.")
            raise FileExistsError(f"Cannot create directory {parent}: a file with that name already exists.")
        parent.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading blob", blob_name=blob.name, local_path=str(local_path))
        with open(local_path, "wb") as file:
            download_stream = container_client.download_blob(blob)
            file.write(download_stream.readall())

    logger.info("✅ All reference blobs copied to local directory", local_dir=str(local_reference_dir))
