import json
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import group_result
import http_client
import item_result
import monday_attachment_service
import monday_config
import monday_item_service


# Thread pool executors
GROUP_EXECUTOR = ThreadPoolExecutor(max_workers=7)  # one per group
ITEM_EXECUTOR = ThreadPoolExecutor(max_workers=10)  # shared item workers


def main():
    print("Starting Monday.com attachment download job...")
    print(f"Workspace: {monday_config.WORKSPACE_NAME}")
    print(f"Board: {monday_config.BOARD_NAME}")
    print(f"Testing Mode: {'ON (processing first 10 items per group)' if monday_config.TESTING_MODE else 'OFF'}")
    print()
    
    try:
        board_id = resolve_board_id()
        print(f"Found board ID: {board_id}")
        print()
        
        monday_item_service.initialize_group_cache(board_id)
        print()
        
        list_all_groups(board_id)
        print()
        
    except Exception as e:
        print(f"ERROR: Fatal error during initialization: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    groups = [
        monday_config.GROUP_1,
        monday_config.GROUP_2,
        monday_config.GROUP_3,
        monday_config.GROUP_4,
        monday_config.GROUP_5,
        monday_config.GROUP_6,
        monday_config.GROUP_7
    ]
    
    status_col_id = monday_item_service.get_status_column_id(board_id)
    email_col_id = monday_item_service.get_email_column_id(board_id)
    
    # Process groups in parallel
    group_futures = []
    for group in groups:
        future = GROUP_EXECUTOR.submit(
            process_group,
            board_id,
            group,
            status_col_id,
            email_col_id
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
    
    print("==========================================")
    print("Attachment download job completed")
    print(f"   Groups processed: {groups_processed} / {len(groups)}")
    print(f"   Success: {total_success}")
    print(f"   Failed: {total_failed}")
    
    GROUP_EXECUTOR.shutdown(wait=True)
    ITEM_EXECUTOR.shutdown(wait=True)


def process_group(board_id: int, group_title: str, status_col_id: str, email_col_id: Optional[str]) -> group_result.GroupResult:
    import threading
    lock = threading.Lock()
    
    with lock:
        print("==========================================")
        print(f"Processing Group: {group_title}")
    
    download_folder = monday_config.GROUP_FOLDER_MAP.get(group_title)
    if download_folder is None:
        with lock:
            print(f"WARNING: No folder mapping found for group: {group_title}")
        return group_result.GroupResult.not_processed()
    
    with lock:
        print(f"Download Folder: {download_folder}")
        print(f"Searching for items from last 2 days with status 'Retry' or empty...")
    
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
            print(f"ERROR: Failed to fetch items for group {group_title}: {str(e)}")
            import traceback
            traceback.print_exc()
        return group_result.GroupResult.failed_group()
    
    if not items:
        with lock:
            print(f"INFO: No items found in group: {group_title}")
        return group_result.GroupResult.processed(0, 0)
    
    with lock:
        print(f"Found {len(items)} item(s) in group: {group_title}")
        print()
    
    # Process items in parallel
    item_futures = []
    for item_info in items:
        future = ITEM_EXECUTOR.submit(
            process_item,
            board_id,
            item_info.item_id,
            download_folder,
            item_info.email,
            item_info.group_name
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
        print(f"Group summary for '{group_title}': {success} succeeded, {failed} failed")
        print()
    
    return group_result.GroupResult.processed(success, failed)


def process_item(board_id: int, item_id: int, download_folder: str, 
                 email: str = "", group_name: str = "") -> item_result.ItemResult:
    import threading
    lock = threading.Lock()
    
    try:
        with lock:
            print(f"  Processing item ID: {item_id}")
        
        success = monday_attachment_service.download_attachments(
            item_id, download_folder, email, group_name
        )
        
        monday_item_service.update_status(item_id, monday_config.NEW_STATUS, board_id)
        
        with lock:
            if success:
                print(f"  Successfully processed item {item_id}")
            else:
                print(f"  WARNING: No attachments found for item {item_id}")
        
        return item_result.ItemResult.SUCCESS
    
    except Exception as e:
        with lock:
            print(f"  ERROR: Failed to process item {item_id}: {str(e)}")
        return item_result.ItemResult.FAILURE


def resolve_board_id() -> int:
    """
    Resolve board ID by board name (optionally within workspace)
    """
    # Try to get boards from workspace using correct query structure
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
        
        # Check for errors - if workspace query fails, just use simple method
        if "errors" in root:
            print("WARNING: Workspace query had errors (this is okay, using fallback):")
            print(f"   {root['errors']}")
            # Fallback to simple board query
            return resolve_board_id_simple()
        
        # If we can get workspaces, try to find the workspace and then query its boards
        workspaces = root.get("data", {}).get("workspaces", [])
        workspace_id = None
        
        for workspace in workspaces:
            workspace_name = workspace.get("name", "")
            if monday_config.WORKSPACE_NAME == workspace_name:
                workspace_id = workspace.get("id", "")
                print(f"Found workspace '{workspace_name}' (ID: {workspace_id})")
                break
        
        # If workspace found, try to query boards in that workspace
        # Note: Monday.com API might require different query structure for workspace boards
        # For now, fall through to simple method which works
        
    except Exception:
        # Fallback on any error
        pass
    
    # Fallback: search all boards (this works reliably)
    return resolve_board_id_simple()


def resolve_board_id_simple() -> int:
    """
    Simple board resolution (fallback)
    """
    payload = "{ \"query\": \"query { boards(limit: 500) { id name } }\" }"
    
    response = http_client.post(payload)
    root = json.loads(response)
    
    if "errors" in root:
        print("ERROR: GraphQL errors:")
        print(root["errors"])
        raise RuntimeError(f"Failed to query boards: {root['errors']}")
    
    boards = root.get("data", {}).get("boards", [])
    
    for board in boards:
        board_name = board.get("name", "")
        if monday_config.BOARD_NAME == board_name:
            return int(board.get("id", 0))
    
    raise RuntimeError(f"Board not found: {monday_config.BOARD_NAME}")


def list_all_groups(board_id: int) -> None:
    """
    List all groups in the board for debugging
    """
    payload = (
        "{ \"query\": \"query { "
        f"boards(ids:{board_id}) {{ "
        "groups { id title } "
        "} }\" }"
    )
    
    try:
        response = http_client.post(payload)
        root = json.loads(response)
        
        if "errors" in root:
            print(f"WARNING: Could not list groups: {root['errors']}")
            return
        
        boards_node = root.get("data", {}).get("boards", [])
        if not boards_node:
            return
        
        groups = boards_node[0].get("groups", [])
        if isinstance(groups, list):
            print("Groups found in board:")
            for group in groups:
                group_title = group.get("title", "")
                print(f"   - {group_title}")
    except Exception as e:
        print(f"WARNING: Could not list groups: {str(e)}")


if __name__ == "__main__":
    main()
