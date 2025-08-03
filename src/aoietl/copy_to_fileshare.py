from azure.storage.fileshare import ShareServiceClient
from pathlib import Path
import os

def clear_azure_file_share(share_client):
    """Delete all files and directories from the Azure File Share."""
    print("🗑️  Clearing Azure File Share...")
    
    def delete_directory_contents(dir_client, dir_path=""):
        """Recursively delete all contents of a directory."""
        try:
            # List all items in the directory
            items = dir_client.list_directories_and_files()
            
            for item in items:
                item_path = f"{dir_path}/{item['name']}" if dir_path else item['name']
                
                if item['is_directory']:
                    # It's a directory - recursively delete its contents first
                    sub_dir_client = share_client.get_directory_client(item_path)
                    delete_directory_contents(sub_dir_client, item_path)
                    
                    # Then delete the empty directory
                    try:
                        sub_dir_client.delete_directory()
                        print(f"   Deleted directory: {item_path}")
                    except Exception as e:
                        print(f"   ❌ Failed to delete directory {item_path}: {e}")
                else:
                    # It's a file - delete it
                    try:
                        file_client = share_client.get_file_client(item_path)
                        file_client.delete_file()
                        print(f"   Deleted file: {item_path}")
                    except Exception as e:
                        print(f"   ❌ Failed to delete file {item_path}: {e}")
                        
        except Exception as e:
            print(f"   ❌ Failed to list contents of {dir_path}: {e}")
    
    try:
        # Start deletion from the root directory
        root_dir_client = share_client.get_directory_client("")
        delete_directory_contents(root_dir_client)
        print("✅ Azure File Share cleared successfully")
        
    except Exception as e:
        print(f"❌ Failed to clear Azure File Share: {e}")

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
    
    print(f"Using account: {account_name}")
    print(f"Account key length: {len(account_key) if account_key else 0}")
    
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
        print(f"✅ Connected to share: {share_properties.name}")

        clear_azure_file_share(share_client)
        
    except Exception as e:
        print(f"❌ Failed to connect to Azure: {e}")
        return
    
    local_folder = Path(local_folder).resolve()
    
    for file_path in local_folder.rglob("*"):
        if file_path.is_file():
            relative_path = file_path.relative_to(local_folder)
            remote_path = str(relative_path).replace("\\", "/")
            
            print(f"Uploading {file_path.name} ({file_path.stat().st_size:,} bytes)")
            
            try:
                # Create directories
                parent_dir = str(Path(remote_path).parent)
                if parent_dir != ".":
                    create_directories_recursive(share_client, parent_dir)
                
                # Get file client and upload
                file_client = share_client.get_file_client(remote_path)
                
                with open(file_path, "rb") as data:
                    file_client.upload_file(data)
                
                print(f"✅ Success: {remote_path}")
                
            except Exception as e:
                print(f"❌ Error: {e}")
                continue

def create_directories_recursive(share_client, dir_path):
    """Create directory structure recursively."""
    parts = dir_path.split("/")
    current = ""
    
    for part in parts:
        current = f"{current}/{part}" if current else part
        try:
            dir_client = share_client.get_directory_client(current)
            dir_client.create_directory()
        except:
            pass  # Directory exists