import json
import re
import time
from typing import Optional

import http_client
import monday_config
import monday_file_downloader


def download_attachments(item_id: int, download_dir: Optional[str] = None, 
                         email: str = "", group_name: str = "") -> bool:
    """
    Downloads all attachments for an item to specified directory.
    @param item_id The item ID
    @param download_dir The directory to save files to
    @return true if at least one file was downloaded
    """
    if download_dir is None:
        download_dir = monday_config.BASE_DOWNLOAD_DIR
    
    downloaded = False
    
    # Query to get all updates with assets
    payload = (
        "{ \"query\": \"query { "
        f"items(ids: [{item_id}]) {{ "
        "id "
        "updates(limit: 100) { "
        "id "
        "body "
        "assets { "
        "id "
        "name "
        "public_url "
        "file_extension "
        "} "
        "} "
        "} }\" }"
    )
    
    response = http_client.post(payload)
    root = json.loads(response)
    
    # Check for errors
    if "errors" in root:
        print("ERROR: GraphQL errors while fetching attachments:")
        print(root["errors"])
        raise RuntimeError(f"Failed to fetch attachments: {root['errors']}")
    
    items_node = root.get("data", {}).get("items", [])
    if not items_node:
        print(f"ERROR: No items returned for item ID: {item_id}")
        return False
    
    item = items_node[0]
    updates = item.get("updates", [])
    
    if not isinstance(updates, list):
        print(f"INFO: No updates found for item {item_id}")
        return False
    
    asset_count = 0
    for update in updates:
        assets = update.get("assets", [])
        if not isinstance(assets, list):
            continue
        
        for asset in assets:
            asset_id = asset.get("id", "")
            file_name = asset.get("name", "")
            file_url = asset.get("public_url", "")
            file_extension = asset.get("file_extension", "")
            
            if not asset_id:
                print("WARNING: Skipping asset with no ID")
                continue
            
            try:
                data = None
                
                # Try using the URL from the asset first (if available)
                if file_url and file_url != "null":
                    try:
                        print(f"   Trying to download from URL: {file_url}")
                        data = http_client.download_file(file_url)
                    except Exception as e:
                        print("   URL download failed, trying API endpoint...")
                
                # Fallback: Use Monday.com file API endpoint
                if data is None:
                    download_url = f"https://api.monday.com/v2/file?assetId={asset_id}"
                    print(f"   Downloading from API: {download_url}")
                    data = http_client.download_file_with_auth(download_url)
                
                # Ensure file has extension
                if "." not in file_name and file_extension:
                    file_name = f"{file_name}.{file_extension}"
                
                # Get base filename (without extension)
                base_name = _sanitize_file_name(file_name)
                if "." in base_name:
                    base_name, ext = base_name.rsplit(".", 1)
                    ext = f".{ext}"
                else:
                    ext = ""
                
                # Format filename: filename__itemid__email__groupname.ext
                # Sanitize email and group_name for filename
                safe_email = _sanitize_file_name(email) if email else "noemail"
                safe_group = _sanitize_file_name(group_name) if group_name else "nogroup"
                safe_group = safe_group.replace("/", "_").replace("\\", "_")
                
                file_name = f"{base_name}__{item_id}__{safe_email}__{safe_group}{ext}"
                
                monday_file_downloader.save(file_name, data, download_dir)
                downloaded = True
                asset_count += 1
                print(f"Downloaded: {file_name}")
            except Exception as e:
                print(f"ERROR: Failed to download asset {asset_id}: {str(e)}")
                import traceback
                traceback.print_exc()
    
    if asset_count == 0:
        print(f"INFO: No attachments found for item {item_id}")
    else:
        print(f"Downloaded {asset_count} attachment(s) for item {item_id}")
    
    return downloaded


def _sanitize_file_name(file_name: str) -> str:
    """
    Sanitize filename to remove invalid characters
    """
    if not file_name:
        return f"attachment_{int(time.time() * 1000)}"
    # Remove invalid characters for Windows/Unix
    return re.sub(r'[<>:"|?*\\/]', '_', file_name).strip()
