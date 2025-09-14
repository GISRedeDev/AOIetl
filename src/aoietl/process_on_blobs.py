from datetime import date
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
import fsspec

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
from .utils import (
    get_blob_service_client,
    get_container_client,
    download_config_and_aoi_from_blob,
    setup_azure_filesystem,
    copy_blobs_to_staging,
)

logger = structlog.get_logger(__name__)

def process_on_blobs(config_blob_path: str, aoi_blob_path: str, error_for_missing_files: bool = False) -> None:
    """
    Main processing function to handle the ETL workflow based on the provided configuration.

    Args:
        config_path (str): Path to the YAML configuration file in the staging-data blob.
        aoi_path (str): Path to the AOI geopackage file in the staging-data blob.
        error_for_missing_files (bool): Whether to raise an error if expected files are missing.
    """
    staging_container = "staging-data"
    config_path = Path("/tmp/config.yaml")
    aoi_gpkg = Path("/tmp/aoi.gpkg")
    blob_client = get_blob_service_client()
    clear_staging_tiers(blob_client, staging_container, ["bronze", "silver", "gold", "platinum"])
    download_config_and_aoi_from_blob(staging_container, config_blob_path, aoi_blob_path, blob_client)
    config = build_config(config_path)
    aoi_gdf = gpd.read_file(aoi_gpkg)
    validate_directories(config)
    for tier, directory_content in config.directories.items():
        logger.info("Processing tier", tier=tier)
        fs, root_path = setup_azure_filesystem(config, tier)
        # if directory_content.raster:
        #     for raster_type in directory_content.raster:
        #         process_rasters_in_blob(
        #             tier,
        #             raster_type,
        #             aoi_gdf,
        #             root_path,
        #             fs,
        #             config,
        #             blob_client,
        #         )
        # if directory_content.vector:
        #     process_vectors_and_tables_in_blob(directory_content.vector, blob_client, tier)
        # if directory_content.table:
        #     process_vectors_and_tables_in_blob(directory_content.table, blob_client, tier)
        if directory_content.hdf:
            for hdf_type in directory_content.hdf:
                process_hdf_in_blob(
                    tier,
                    hdf_type,
                    aoi_gdf,
                    root_path,
                    fs,
                    config,
                    blob_client,
                )
    aoi_gpkg.unlink(missing_ok=True)
    config_path.unlink(missing_ok=True)


def blob_prefix_exists(blob_service_client: BlobServiceClient, container_name: str, prefix: str) -> bool:
    """
    Returns True if any blob exists under the given prefix in the container.
    """
    container_client = get_container_client(container_name, blob_service_client)
    blobs = container_client.list_blobs(name_starts_with=prefix)
    return any(True for _ in blobs)


def clear_staging_tiers(blob_service_client: BlobServiceClient, container_name: str, tiers: list[str]):
    """
    Delete all blobs under each tier prefix in the specified container.
    """
    container_client = get_container_client(container_name, blob_service_client)
    for tier in tiers:
        prefix = f"{tier}/"
        logger.info(f"Clearing blobs under {container_name}/{prefix}")
        blobs = list(container_client.list_blobs(name_starts_with=prefix))
        if blobs:
            blobs.sort(key=lambda b: len(b.name), reverse=True)
            for blob in blobs:
                logger.info(f"Deleting blob {blob.name}")
                container_client.delete_blob(blob.name)


def process_rasters_in_blob(
        tier: str,
        raster_type: str,
        aoi_gdf: gpd.GeoDataFrame,
        root_path: UPath,
        fs: fsspec.AbstractFileSystem,
        config: DataConfig,
        blob_service_client: BlobServiceClient,

) -> None:
    raster_files = list_rasters_for_date(root_path, raster_type, config.date)
    logger.info(f"Found {len(raster_files)} rasters for {raster_type} in tier {tier}")
    if not raster_files:
        logger.warning(f"No rasters found for {raster_type} in tier {tier}")
        return None
    # Build tile index
    tile_index_gdf = build_tile_index(raster_files, fs=fs)
    # Filter by AOI
    filtered_paths = filter_tiles_by_aoi(tile_index_gdf, aoi_gdf, config)
    logger.info(f"Filtered to {len(filtered_paths) if filtered_paths else 0} rasters intersecting AOI")
    copy_json = False
    if raster_type == "landsat":
        copy_json = True
    if filtered_paths:
        source_blob_paths = [str(p.relative_to(p.anchor)) if hasattr(p, "anchor") else str(p) for p in filtered_paths]
        copy_blobs_to_staging(
            blob_service_client=blob_service_client,
            source_container=tier,  # e.g. "bronze"
            dest_container="staging-data",
            source_blob_paths=source_blob_paths,
            dest_prefix=f"{tier.value}/{raster_type}",
            copy_json=copy_json
        )

def process_vectors_and_tables_in_blob(
        directory_content: list,
        blob_service_client: BlobServiceClient,
        tier: DirectoryType
) -> None:
    for v in directory_content:
        logger.info(f"Processing vector data: {v.name}")
        if "/" in v.name:
            dest_prefix = f"{tier.value}/" + "/".join(v.name.split("/")[:-1])
        else:
            dest_prefix = f"{tier.value}"
        copy_blobs_to_staging(
            blob_service_client=blob_service_client,
            source_container=tier,  # e.g. "bronze"
            dest_container="staging-data",
            source_blob_paths=[str(v.name)],
            dest_prefix=dest_prefix
        )


def process_hdf_in_blob(
        tier: str,
        hdf_type: str,
        aoi_gdf: gpd.GeoDataFrame,
        root_path: UPath,
        fs: fsspec.AbstractFileSystem,
        config: DataConfig,
        blob_service_client: BlobServiceClient,

) -> None:
    hdf_files = list_hdf_for_date(root_path, hdf_type, config.date)
    logger.info(f"Found {len(hdf_files)} HDF files for {hdf_type} in tier {tier}")
    if not hdf_files:
        logger.warning(f"No HDF files found for {hdf_type} in tier {tier}")
        return None
    # Build tile index
    tile_index_gdf = build_hdf_tile_index(hdf_files, fs=fs)
    # Filter by AOI
    filtered_paths = filter_tiles_by_aoi(tile_index_gdf, aoi_gdf, config)
    logger.info(f"Filtered to {len(filtered_paths) if filtered_paths else 0} HDF files intersecting AOI")
    if filtered_paths:
        source_blob_paths = [str(p.relative_to(p.anchor)) if hasattr(p, "anchor") else str(p) for p in filtered_paths]
        copy_blobs_to_staging(
            blob_service_client=blob_service_client,
            source_container=tier,
            dest_container="staging-data",
            source_blob_paths=source_blob_paths,
            dest_prefix=f"{tier.value}/{hdf_type}",
            copy_json=False
        )
