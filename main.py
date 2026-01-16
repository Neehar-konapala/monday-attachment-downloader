#!/usr/bin/env python3

import json
import sys
import os

# Add current directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict

# Import monday_config first and patch it
import monday_config

# Hard-coded configuration values
TARGET_STATUS = "Retry"  # Process items with empty status or "Retry"
NEW_STATUS = "In Queue"  # Status to set after downloading
DAYS_TO_PROCESS = 1  # Process items from today and yesterday (past 2 days)
STATUS_COLUMN_TITLE = "Status"
EMAIL_COLUMN_TITLE = "Email"
MONDAY_API_URL = "https://api.monday.com/v2"

# Now import other modules (they will use the patched monday_config)
import group_result
import http_client
import item_result
import monday_attachment_service
import monday_item_service
import monday_file_downloader


# Thread pool executors
GROUP_EXECUTOR = ThreadPoolExecutor(max_workers=7)
ITEM_EXECUTOR = ThreadPoolExecutor(max_workers=10)


def resolve_board_id(api_token: str, workspace_name: str, board_name: str) -> int:
    """
    Resolve board ID by board name (optionally within workspace)
    """
    # Save original values
    original_token = monday_config.MONDAY_API_TOKEN
    original_url = monday_config.MONDAY_API_URL
    
    # Set temporary values
    monday_config.MONDAY_API_TOKEN = api_token
    monday_config.MONDAY_API_URL = MONDAY_API_URL
    
    try:
        # Try to get boards from workspace
        payload = (
            "{ \"query\": \"query { "
            "workspaces(limit: 100) { "
            "id name "
            "} "
            "}\" }"
        )
        
        try:
            response = http_client.post(payload)
            root = json.loads(response)
            
            if "errors" not in root:
                workspaces = root.get("data", {}).get("workspaces", [])
                workspace_id = None
                
                for workspace in workspaces:
                    if workspace.get("name", "") == workspace_name:
                        workspace_id = workspace.get("id", "")
                        break
        except Exception:
            pass
        
        # Fallback: search all boards
        payload = "{ \"query\": \"query { boards(limit: 500) { id name } }\" }"
        
        response = http_client.post(payload)
        root = json.loads(response)
        
        if "errors" in root:
            raise RuntimeError(f"Failed to query boards: {root['errors']}")
        
        boards = root.get("data", {}).get("boards", [])
        
        for board in boards:
            if board.get("name", "") == board_name:
                return int(board.get("id", 0))
        
        raise RuntimeError(f"Board not found: {board_name}")
    finally:
        # Restore original values
        monday_config.MONDAY_API_TOKEN = original_token
        monday_config.MONDAY_API_URL = original_url


def process_group(board_id: int, group_title: str, status_col_id: str, email_col_id: Optional[str],
                 download_folder: str, api_token: str) -> group_result.GroupResult:
    """
    Process a single group
    """
    import threading
    lock = threading.Lock()
    
    with lock:
        print("==========================================", file=sys.stderr)
        print(f"Processing Group: {group_title}", file=sys.stderr)
        print(f"Download Folder: {download_folder}", file=sys.stderr)
        print(f"Searching for items from last 2 days with status 'Retry' or empty...", file=sys.stderr)
    
    items = []
    try:
        items = monday_item_service.get_items_from_group(
            board_id,
            group_title,
            status_col_id,
            monday_config.TARGET_STATUS,
            email_col_id
        )
    except Exception as e:
        with lock:
            print(f"ERROR: Failed to fetch items for group {group_title}: {str(e)}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
        return group_result.GroupResult.failed_group()
    
    if not items:
        with lock:
            print(f"INFO: No items found in group: {group_title}", file=sys.stderr)
        return group_result.GroupResult.processed(0, 0)
    
    with lock:
        print(f"Found {len(items)} item(s) in group: {group_title}", file=sys.stderr)
        print("", file=sys.stderr)
    
    # Process items in parallel
    item_futures = []
    for item_info in items:
        future = ITEM_EXECUTOR.submit(
            process_item,
            board_id,
            item_info.item_id,
            download_folder,
            item_info.email,
            item_info.group_name,
            api_token
        )
        item_futures.append(future)
    
    # Wait for all items to complete
    for future in item_futures:
        future.result()
    
    success = 0
    failed = 0
    
    for future in item_futures:
        r = future.result()
        success += r.success
        failed += r.failed
    
    import threading
    lock = threading.Lock()
    with lock:
        print(f"Group summary for '{group_title}': {success} succeeded, {failed} failed", file=sys.stderr)
        print("", file=sys.stderr)
    
    return group_result.GroupResult.processed(success, failed)


def process_item(board_id: int, item_id: int, download_folder: str, 
                 email: str, group_name: str, api_token: str) -> item_result.ItemResult:
    """
    Process a single item
    """
    import threading
    lock = threading.Lock()
    
    try:
        with lock:
            print(f"  Processing item ID: {item_id}", file=sys.stderr)
        
        success = monday_attachment_service.download_attachments(
            item_id, download_folder, email, group_name
        )
        
        monday_item_service.update_status(item_id, monday_config.NEW_STATUS, board_id)
        
        with lock:
            if success:
                print(f"  Successfully processed item {item_id}", file=sys.stderr)
            else:
                print(f"  WARNING: No attachments found for item {item_id}", file=sys.stderr)
        
        return item_result.ItemResult.SUCCESS
    
    except Exception as e:
        with lock:
            print(f"  ERROR: Failed to process item {item_id}: {str(e)}", file=sys.stderr)
        return item_result.ItemResult.FAILURE


def download_attachments(api_token: str, workspace_name: str, board_name: str,
                        groups: List[str], group_folder_map: Dict[str, str]) -> dict:
    """
    Main function to download attachments from Monday.com
    
    Args:
        api_token: Monday.com API token
        workspace_name: Name of the workspace
        board_name: Name of the board
        groups: List of group names to process
        group_folder_map: Mapping of group names to download folders
    
    Returns:
        Dictionary with result or error
    """
    try:
        # Set configuration values in monday_config module
        monday_config.MONDAY_API_TOKEN = api_token
        monday_config.MONDAY_API_URL = MONDAY_API_URL
        monday_config.STATUS_COLUMN_TITLE = STATUS_COLUMN_TITLE
        monday_config.EMAIL_COLUMN_TITLE = EMAIL_COLUMN_TITLE
        monday_config.DAYS_TO_PROCESS = DAYS_TO_PROCESS
        monday_config.TARGET_STATUS = TARGET_STATUS
        monday_config.NEW_STATUS = NEW_STATUS
        monday_config.BASE_DOWNLOAD_DIR = ""  # Not used, folders come from group_folder_map
        
        print("Starting Monday.com attachment download job...", file=sys.stderr)
        print(f"Workspace: {workspace_name}", file=sys.stderr)
        print(f"Board: {board_name}", file=sys.stderr)
        print(f"Processing items from last 2 days with status 'Retry' or empty", file=sys.stderr)
        print("", file=sys.stderr)
        
        # Resolve board ID
        board_id = resolve_board_id(api_token, workspace_name, board_name)
        print(f"Found board ID: {board_id}", file=sys.stderr)
        print("", file=sys.stderr)
        
        # Initialize group cache
        monday_item_service.initialize_group_cache(board_id)
        print("", file=sys.stderr)
        
        # Get column IDs
        status_col_id = monday_item_service.get_status_column_id(board_id)
        email_col_id = monday_item_service.get_email_column_id(board_id)
        
        print(f"Found Status column ID: {status_col_id}", file=sys.stderr)
        if email_col_id:
            print(f"Found Email column ID: {email_col_id}", file=sys.stderr)
        print("", file=sys.stderr)
        
        # Process groups in parallel
        group_futures = []
        for group in groups:
            download_folder = group_folder_map.get(group)
            if not download_folder:
                print(f"WARNING: No folder mapping found for group: {group}", file=sys.stderr)
                continue
            
            future = GROUP_EXECUTOR.submit(
                process_group,
                board_id,
                group,
                status_col_id,
                email_col_id,
                download_folder,
                api_token
            )
            group_futures.append(future)
        
        # Wait for all groups to complete
        for future in group_futures:
            future.result()
        
        total_success = 0
        total_failed = 0
        groups_processed = 0
        
        for future in group_futures:
            r = future.result()
            total_success += r.success
            total_failed += r.failed
            if r.processed:
                groups_processed += 1
        
        print("==========================================", file=sys.stderr)
        print("Attachment download job completed", file=sys.stderr)
        print(f"   Groups processed: {groups_processed} / {len(groups)}", file=sys.stderr)
        print(f"   Success: {total_success}", file=sys.stderr)
        print(f"   Failed: {total_failed}", file=sys.stderr)
        
        GROUP_EXECUTOR.shutdown(wait=True)
        ITEM_EXECUTOR.shutdown(wait=True)
        
        return {
            "result": {
                "groups_processed": groups_processed,
                "total_groups": len(groups),
                "success": total_success,
                "failed": total_failed,
                "board_id": board_id
            },
            "capability": "download_attachments"
        }
    
    except Exception as e:
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"ERROR: {error_msg}", file=sys.stderr)
        return {
            "error": str(e),
            "capability": "download_attachments"
        }


def main():
    """Main entry point - reads JSON from stdin, outputs JSON to stdout"""
    try:
        input_data = json.load(sys.stdin)
        
        capability = input_data.get("capability")
        args = input_data.get("args", {})
        
        if capability == "download_attachments":
            result = download_attachments(
                api_token=args.get("api_token"),
                workspace_name=args.get("workspace_name"),
                board_name=args.get("board_name"),
                groups=args.get("groups", []),
                group_folder_map=args.get("group_folder_map", {})
            )
            print(json.dumps(result, indent=2))
        else:
            print(json.dumps({
                "error": f"Unknown capability: {capability}",
                "capability": capability
            }, indent=2))
    
    except Exception as e:
        import traceback
        print(json.dumps({
            "error": f"Error: {str(e)}",
            "capability": "unknown"
        }, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()
