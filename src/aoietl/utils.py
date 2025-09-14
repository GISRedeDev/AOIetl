from pathlib import Path
import os
from typing import Union
import structlog

import fsspec
from upath import UPath
from azure.storage.blob import BlobServiceClient

from .data_types import DataConfig, DirectoryType

logger = structlog.get_logger(__name__)


def setup_azure_filesystem(config: DataConfig, container: DirectoryType) -> tuple[Union[fsspec.AbstractFileSystem, None], UPath]:
    """
    Setup Azure filesystem and return filesystem object and base path.
    """
    azure_account_name = os.getenv("AZURE_ACCOUNT_NAME")
    azure_account_key = os.getenv("AZURE_ACCOUNT_KEY")
    if azure_account_name and azure_account_key:
        fs = fsspec.filesystem(
            "az",
            account_name=azure_account_name,
            account_key=azure_account_key
        )
        base_path = UPath(
            f"az://{container.value}",
            protocol="az",
            account_name=azure_account_name,
            account_key=azure_account_key
        )
        return fs, base_path
    else:
        raise ValueError(
            "Azure account name and key must be set in environment variables AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY."
        )
    

def get_blob_service_client() -> BlobServiceClient:
    """
    Create and return a BlobServiceClient using environment variables.
    """
    azure_account_name = os.getenv("AZURE_ACCOUNT_NAME")
    azure_account_key = os.getenv("AZURE_ACCOUNT_KEY")
    if azure_account_name and azure_account_key:
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={azure_account_name};AccountKey={azure_account_key};EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        return blob_service_client
    else:
        raise ValueError(
            "Azure account name and key must be set in environment variables AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY."
        )
    

def get_container_client(container_name: str, blob_service_client: BlobServiceClient):
    """
    Get and return a ContainerClient for the specified container.
    """
    try:
        container_client = blob_service_client.get_container_client(container_name)
        return container_client
    except Exception as e:
        raise ValueError(f"Error accessing container {container_name}: {e}")
    

def download_config_and_aoi_from_blob(
    container_name: str,
    config_blob_path: str,
    aoi_blob_path: str,
    blob_service_client: BlobServiceClient,
    tmp_dir: Path = Path("/tmp")
):
    """
    Download config.yaml and AOI geopackage from Azure Blob Storage to a temporary directory.

    Args:
        container_name (str): Name of the blob container (e.g., "staging-data").
        config_blob_path (str): Path to config.yaml in the blob (e.g., "config/config.yaml").
        aoi_blob_path (str): Path to AOI geopackage in the blob (e.g., "config/aoi_test.gpkg").
        tmp_dir (Path): Local directory to save files (default: /tmp).

    Returns:
        Tuple of local paths to config.yaml and AOI geopackage.
    """
    container_client = get_container_client(container_name, blob_service_client)

    tmp_dir.mkdir(parents=True, exist_ok=True)
    config_local = tmp_dir / "config.yaml"
    aoi_local = tmp_dir / "aoi.gpkg"

    # Download config.yaml
    with open(config_local, "wb") as f:
        f.write(container_client.get_blob_client(str(config_blob_path)).download_blob().readall())

    # Download AOI geopackage
    with open(aoi_local, "wb") as f:
        f.write(container_client.get_blob_client(str(aoi_blob_path)).download_blob().readall())
    logger.info(f"Downloaded config to {config_local} and AOI to {aoi_local}")

def copy_blobs_to_staging(
    blob_service_client: BlobServiceClient,
    source_container: str,
    dest_container: str,
    source_blob_paths: list[str],
    dest_prefix: str,
    copy_json: bool = False
):
    for src_blob_path in source_blob_paths:
        src_blob_path = src_blob_path.replace(f"az:/{source_container.value}", "")
        src_blob_client = blob_service_client.get_blob_client(container=source_container, blob=src_blob_path)
        dest_blob_name = f"{dest_prefix}/{os.path.basename(src_blob_path)}"
        dest_blob_client = blob_service_client.get_blob_client(container=dest_container, blob=dest_blob_name)
        logger.info(f"Copying {src_blob_path} to {dest_container}/{dest_blob_name}")
        copy_source_url = src_blob_client.url
        dest_blob_client.start_copy_from_url(copy_source_url)
        if copy_json:
            json_blob_path = src_blob_path.rsplit('.', 1)[0] + '_MTL.json'
            json_blob_client = blob_service_client.get_blob_client(container=source_container, blob=json_blob_path)
            if json_blob_client.exists():
                dest_json_blob_name = f"{dest_prefix}/{os.path.basename(json_blob_path)}"
                dest_json_blob_client = blob_service_client.get_blob_client(container=dest_container, blob=dest_json_blob_name)
                logger.info(f"Copying {json_blob_path} to {dest_container}/{dest_json_blob_name}")
                copy_source_json_url = json_blob_client.url
                dest_json_blob_client.start_copy_from_url(copy_source_json_url)
            else:
                logger.warning(f"JSON sidecar file {json_blob_path} does not exist; skipping.")
