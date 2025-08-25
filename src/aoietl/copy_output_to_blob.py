from azure.storage.fileshare import ShareServiceClient
from azure.storage.blob import BlobServiceClient
import os
from io import BytesIO
import structlog

logger = structlog.get_logger(__name__)

TIERS = {"bronze", "silver", "gold", "platinum"}

def copy_fileshare_output_to_blob():
    account_name = os.getenv("AZURE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_ACCOUNT_KEY")
    share_name = os.getenv("AZURE_SHARE_NAME", "gisrede-test")

    if not account_name or not account_key:
        raise ValueError("AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY must be set")

    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={account_name};"
        f"AccountKey={account_key};"
        f"EndpointSuffix=core.windows.net"
    )
    share_service = ShareServiceClient.from_connection_string(conn_str)
    share_client = share_service.get_share_client(share_name)
    output_dir_client = share_client.get_directory_client("output")

    # List all tier directories in the output dir
    for item in output_dir_client.list_directories_and_files():
        if item['is_directory'] and item['name'] in TIERS:
            tier = item['name']
            logger.info(f"Processing tier: {tier}")
            tier_dir_client = share_client.get_directory_client(f"output/{tier}")

            # Connect to the corresponding blob container
            blob_service = BlobServiceClient.from_connection_string(conn_str)
            container_client = blob_service.get_container_client(tier)

            def upload_dir(dir_client, prefix=""):
                for subitem in dir_client.list_directories_and_files():
                    subitem_path = f"{prefix}/{subitem['name']}" if prefix else subitem['name']
                    if subitem['is_directory']:
                        sub_dir_client = share_client.get_directory_client(f"output/{tier}/{subitem_path}")
                        upload_dir(sub_dir_client, subitem_path)
                    else:
                        logger.info(f"Uploading {tier}/{subitem_path} from fileshare to blob...")
                        file_client = share_client.get_file_client(f"output/{tier}/{subitem_path}")
                        stream = BytesIO()
                        data = file_client.download_file().readall()
                        stream.write(data)
                        stream.seek(0)
                        # Upload to blob, preserving the directory structure under the tier
                        container_client.upload_blob(name=subitem_path, data=stream, overwrite=True)
                        logger.info(f"Uploaded {subitem_path} to blob container {tier}")

            upload_dir(tier_dir_client)

    logger.info("✅ All tier files copied from fileshare output to corresponding blobs.")