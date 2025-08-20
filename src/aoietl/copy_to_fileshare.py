from azure.storage.fileshare import ShareServiceClient
from pathlib import Path
import os
import structlog


logger = structlog.get_logger(__name__)

def clear_azure_file_share(share_client):
    """Delete all files and directories from the Azure File Share."""
    logger.info("🗑️  Clearing Azure File Share...")
    
    def delete_directory_contents(dir_client, dir_path=""):
        """Recursively delete all contents of a directory."""
        try:
            # List all items in the directory
            items = dir_client.list_directories_and_files()
            
            for item in items:
                item_path = f"{dir_path}/{item['name']}" if dir_path else item['name']
                
                if item['is_directory'] and item['name'] not in ["reference", "repos"]:
                    # It's a directory - recursively delete its contents first
                    looger.warning(f"****************DELETING {item['name']")
                    sub_dir_client = share_client.get_directory_client(item_path)
                    delete_directory_contents(sub_dir_client, item_path)
                    
                    # Then delete the empty directory
                    try:
                        sub_dir_client.delete_directory()
                        logger.info(f"   Deleted directory: {item_path}")
                    except Exception as e:
                        logger.info(f"Failed to delete directory {item_path}: {e}")
                else:
                    # It's a file - delete it
                    try:
                        file_client = share_client.get_file_client(item_path)
                        file_client.delete_file()
                        logger.info(f"Deleted file: {item_path}")
                    except Exception as e:
                        logger.error(f"Failed to delete file {item_path}: {e}")
                        
        except Exception as e:
            logger.error(f"Failed to list contents of {dir_path}: {e}")
    
    try:
        # Start deletion from the root directory
        root_dir_client = share_client.get_directory_client("")
        delete_directory_contents(root_dir_client)
        logger.info("✅ Azure File Share cleared successfully")
        
    except Exception as e:
        logger.error(f"Failed to clear Azure File Share: {e}")

def upload_file_to_share(local_folder: Path):
    """Upload files to Azure File Share with proper authentication."""
    account_name = os.getenv("AZURE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_ACCOUNT_KEY")
    share_name = os.getenv("AZURE_SHARE_NAME", "gisrede-test")
    
    # Debug: Check if environment variables are set
    if not account_name:
        raise ValueError("AZURE_ACCOUNT_NAME environment variable not set")
    if not account_key:
        raise ValueError("AZURE_ACCOUNT_KEY environment variable not set")
    
    logger.info(f"Using account: {account_name}")
    
    try:
        # Create service client with connection string (more reliable)
        connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={account_name};"
            f"AccountKey={account_key};"
            f"EndpointSuffix=core.windows.net"
        )
        
        service = ShareServiceClient.from_connection_string(connection_string)
        share_client = service.get_share_client(share_name)
        
        # Test connection by trying to get share properties
        share_properties = share_client.get_share_properties()
        logger.info(f"✅ Connected to share: {share_properties.name}")

        clear_azure_file_share(share_client)

        local_folder = Path(local_folder).resolve()
        logger.info(f"📁 Starting upload from: {local_folder}")
        
        if not local_folder.exists():
            logger.error(f"❌ Local folder does not exist: {local_folder}")
            return
        
        file_count = 0
        for file_path in local_folder.rglob("*"):
            if file_path.is_file():
                file_count += 1
                relative_path = file_path.relative_to(local_folder)
                remote_path = str(relative_path).replace("\\", "/")
                
                logger.info(f"📤 Uploading {file_path.name} ({file_path.stat().st_size:,} bytes)")
                
                try:
                    # Create directories
                    parent_dir = str(Path(remote_path).parent)
                    if parent_dir != ".":
                        create_directories_recursive(share_client, parent_dir)
                    
                    # Get file client and upload
                    file_client = share_client.get_file_client(remote_path)
                    
                    with open(file_path, "rb") as data:
                        file_client.upload_file(data)
                    
                    logger.info(f"✅ Success: {remote_path}")
                    
                except Exception as e:
                    logger.error(f"❌ Error uploading {remote_path}: {e}")
                    continue
        
        logger.info(f"🎉 Upload complete! {file_count} files uploaded.")
        
    except Exception as e:
        logger.error(f"❌ Failed to connect to Azure: {e}")
        return


def create_directories_recursive(share_client, dir_path):
    """Create directory structure recursively."""
    parts = dir_path.split("/")
    current = ""
    
    for part in parts:
        current = f"{current}/{part}" if current else part
        try:
            dir_client = share_client.get_directory_client(current)
            dir_client.create_directory()
        except Exception as e:
            logger.error(f"Failed to create directory {current}: {e} - skipping")
