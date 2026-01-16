import json
from datetime import datetime, timedelta
from typing import List, Optional, Dict, NamedTuple

import http_client
import monday_config


class ItemInfo(NamedTuple):
    """Container for item information"""
    item_id: int
    email: str
    group_name: str


_status_column_id: Optional[str] = None
_email_column_id: Optional[str] = None
_group_id_cache: Dict[str, str] = {}


def get_status_column_id(board_id: int) -> str:
    """
    Get the status column ID from the board structure (public for use in job)
    """
    global _status_column_id
    if _status_column_id is not None:
        return _status_column_id
    
    payload = (
        "{ \"query\": \"query { "
        f"boards(ids:{board_id}) {{ "
        "columns { id title type } "
        "} }\" }"
    )
    
    response = http_client.post(payload)
    root = json.loads(response)
    
    # Check for errors
    if "errors" in root:
        print(f"ERROR: GraphQL errors while fetching columns:")
        print(root["errors"])
        raise RuntimeError(f"Failed to fetch columns: {root['errors']}")
    
    boards_node = root.get("data", {}).get("boards", [])
    if not boards_node:
        raise RuntimeError(f"Board not found: {board_id}")
    
    columns = boards_node[0].get("columns", [])
    
    for col in columns:
        title = col.get("title", "")
        if monday_config.STATUS_COLUMN_TITLE.lower() == title.lower():
            _status_column_id = col.get("id", "")
            print(f"Found Status column ID: {_status_column_id}")
            return _status_column_id
    
    # Fallback: try to find by ID if it's a standard status column
    for col in columns:
        col_id = col.get("id", "")
        if col_id.lower() == "status" or "status" in col_id.lower():
            _status_column_id = col_id
            print(f"Found Status column by ID: {_status_column_id}")
            return _status_column_id
    
    raise RuntimeError(f"Status column not found in board {board_id}")


def get_email_column_id(board_id: int) -> Optional[str]:
    """
    Get the email column ID from the board structure
    """
    global _email_column_id
    if _email_column_id is not None:
        return _email_column_id
    
    payload = (
        "{ \"query\": \"query { "
        f"boards(ids:{board_id}) {{ "
        "columns { id title type } "
        "} }\" }"
    )
    
    response = http_client.post(payload)
    root = json.loads(response)
    
    # Check for errors
    if "errors" in root:
        print(f"WARNING: GraphQL errors while fetching columns for email:")
        print(root["errors"])
        return None
    
    boards_node = root.get("data", {}).get("boards", [])
    if not boards_node:
        return None
    
    columns = boards_node[0].get("columns", [])
    
    for col in columns:
        title = col.get("title", "")
        if monday_config.EMAIL_COLUMN_TITLE.lower() == title.lower():
            _email_column_id = col.get("id", "")
            print(f"Found Email column ID: {_email_column_id}")
            return _email_column_id
    
    # Fallback: try to find by ID if it's a standard email column
    for col in columns:
        col_id = col.get("id", "")
        if col_id.lower() == "email" or "email" in col_id.lower():
            _email_column_id = col_id
            print(f"Found Email column by ID: {_email_column_id}")
            return _email_column_id
    
    print(f"WARNING: Email column not found in board {board_id}")
    return None


def get_items_from_group(board_id: int, group_title: str, 
                         status_col_id: Optional[str] = None, 
                         target_status: Optional[str] = None,
                         email_col_id: Optional[str] = None) -> List[ItemInfo]:
    """
    Get items from a specific group with status and date filtering
    Returns ItemInfo objects with item_id, email, and group_name
    Filters by date: only items from the last DAYS_TO_PROCESS days
    """
    # Calculate date threshold (today and yesterday = 2 days)
    days_to_process = monday_config.DAYS_TO_PROCESS
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    date_threshold = today - timedelta(days=days_to_process)
    
    # First, try to get the group ID directly
    group_id = _get_group_id_by_title(board_id, group_title)
    if group_id is not None:
        # OPTIMIZED: Query items from this specific group using API filter
        return _get_items_from_group_id_optimized_with_date(
            board_id, group_id, group_title, status_col_id, target_status, 
            email_col_id, date_threshold
        )
    
    # Fallback: search through all items
    return _get_items_from_group_by_search_with_date(
        board_id, group_title, status_col_id, target_status, 
        email_col_id, date_threshold
    )


def get_item_ids_from_group(board_id: int, group_title: str, limit: int, 
                            status_col_id: Optional[str] = None, 
                            target_status: Optional[str] = None) -> List[int]:
    """
    DEPRECATED: Use get_items_from_group instead
    Get the first N item IDs from a specific group with status filtering (for testing)
    Uses Monday.com API filtering for efficiency
    """
    # First, try to get the group ID directly
    group_id = _get_group_id_by_title(board_id, group_title)
    if group_id is not None:
        # OPTIMIZED: Query items from this specific group using API filter
        items = _get_items_from_group_id_optimized(board_id, group_id, limit, status_col_id, target_status)
        return items
    
    # Fallback: search through all items
    return _get_item_ids_from_group_by_search(board_id, group_title, limit, status_col_id, target_status)


def initialize_group_cache(board_id: int) -> None:
    """
    Initialize group ID cache - fetch all groups once
    """
    global _group_id_cache
    if _group_id_cache:
        return  # Already cached
    
    print("Initializing group ID cache...")
    payload = (
        "{ \"query\": \"query { "
        f"boards(ids:{board_id}) {{ "
        "groups { id title } "
        "} }\" }"
    )
    
    response = http_client.post(payload)
    root = json.loads(response)
    
    if "errors" in root:
        print(f"ERROR: Failed to fetch groups: {root['errors']}")
        return
    
    boards_node = root.get("data", {}).get("boards", [])
    if not boards_node:
        return
    
    groups = boards_node[0].get("groups", [])
    if not isinstance(groups, list):
        return
    
    for group in groups:
        current_title = group.get("title", "").strip()
        group_id = group.get("id", "")
        
        # Store both with and without "> " prefix
        _group_id_cache[current_title] = group_id
        if current_title.startswith("> "):
            _group_id_cache[current_title[2:].strip()] = group_id
    
    print(f"Cached {len(_group_id_cache)} group IDs")


def _get_group_id_by_title(board_id: int, group_title: str) -> Optional[str]:
    """
    Get group ID by group title (uses cache if available)
    """
    # Check cache first
    cached_id = _group_id_cache.get(group_title)
    if cached_id is not None:
        return cached_id
    
    # If not in cache, try to match flexibly
    for key, value in _group_id_cache.items():
        if _matches_group_title(group_title, key):
            return value
    
    # If still not found and cache is empty, initialize it
    if not _group_id_cache:
        initialize_group_cache(board_id)
        return _get_group_id_by_title(board_id, group_title)  # Retry after caching
    
    return None


def _get_items_from_group_id_optimized(board_id: int, group_id: str, limit: int,
                                      status_col_id: Optional[str],
                                      target_status: Optional[str]) -> List[int]:
    """
    OPTIMIZED: Get first N items from a group using Monday.com API filtering
    """
    result = []
    cursor = None
    items_found = 0
    consecutive_empty_pages = 0
    
    while items_found < limit:
        cursor_part = f", cursor: \\\"{cursor}\\\"" if cursor else ""
        page_limit = 100
        
        import time
        start_time = time.time() * 1000
        
        # Query items with status column if filtering by status
        column_values_part = "column_values { id text } " if status_col_id else ""
        payload = (
            "{ \"query\": \"query { "
            f"boards(ids:{board_id}) {{ "
            f"items_page(limit: {page_limit}{cursor_part}) {{ "
            "cursor "
            "items { "
            "id "
            "group { id title } "
            f"{column_values_part}"
            "} } } }\" }"
        )
        
        response = http_client.post(payload)
        root = json.loads(response)
        
        if "errors" in root:
            # If this query format doesn't work, try alternative
            return _get_item_ids_from_group_id_alternative(board_id, group_id, limit, status_col_id, target_status)
        
        boards_node = root.get("data", {}).get("boards", [])
        if not boards_node:
            break
        
        items_page = boards_node[0].get("items_page", {})
        items_node = items_page.get("items", [])
        
        if not items_node:
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= 2:
                break
        else:
            consecutive_empty_pages = 0
        
        # Filter items by group ID and status
        items_in_this_page = 0
        for item in items_node:
            if items_found >= limit:
                break
            
            group_node = item.get("group")
            if not group_node:
                continue
            
            item_group_id = group_node.get("id", "")
            if group_id != item_group_id:
                continue  # Skip items not in target group
            
            # If status filtering is enabled, check status
            if status_col_id:
                columns_node = item.get("column_values", [])
                if not columns_node:
                    # No status column = empty status, include if targetStatus is empty/null
                    if not target_status:
                        result.append(int(item.get("id", 0)))
                        items_found += 1
                        items_in_this_page += 1
                    continue
                
                current_status = ""
                for col in columns_node:
                    col_id = col.get("id", "")
                    if status_col_id == col_id:
                        current_status = col.get("text", "").strip()
                        break
                
                # Check if status matches (empty or targetStatus)
                if not current_status or (target_status and target_status.lower() == current_status.lower()):
                    result.append(int(item.get("id", 0)))
                    items_found += 1
                    items_in_this_page += 1
            else:
                # No status filtering, add all items from group
                result.append(int(item.get("id", 0)))
                items_found += 1
                items_in_this_page += 1
        
        query_time = int((time.time() * 1000) - start_time)
        if items_in_this_page > 0:
            print(f"    Found {items_in_this_page} matching item(s) in this page (total: {items_found}/{limit}) - Query took {query_time}ms")
        
        # Get cursor for next page
        cursor_node = items_page.get("cursor")
        cursor = cursor_node if cursor_node else None
        
        # Stop if we've found enough items or no more pages
        if items_found >= limit or not cursor:
            break
    
    return result


def _get_items_from_group_id_optimized_with_date(board_id: int, group_id: str, group_title: str,
                                                  status_col_id: Optional[str],
                                                  target_status: Optional[str],
                                                  email_col_id: Optional[str],
                                                  date_threshold: datetime) -> List[ItemInfo]:
    """
    OPTIMIZED: Get items from a group with date filtering (last 2 days)
    Returns ItemInfo objects with item_id, email, and group_name
    """
    result = []
    cursor = None
    consecutive_empty_pages = 0
    items_checked = 0
    max_items_to_check = 1000  # Limit total items checked to avoid infinite loops
    
    print(f"    Searching for items in group '{group_title}' from last 2 days...")
    
    while items_checked < max_items_to_check:
        cursor_part = f", cursor: \\\"{cursor}\\\"" if cursor else ""
        page_limit = 100
        
        # Query all column values (we'll filter in code)
        column_values_part = "column_values { id text } " if (status_col_id or email_col_id) else ""
        
        payload = (
            "{ \"query\": \"query { "
            f"boards(ids:{board_id}) {{ "
            f"items_page(limit: {page_limit}{cursor_part}) {{ "
            "cursor "
            "items { "
            "id "
            "created_at "
            "group { id title } "
            f"{column_values_part}"
            "} } } }\" }"
        )
        
        response = http_client.post(payload)
        root = json.loads(response)
        
        if "errors" in root:
            # If this query format doesn't work, try alternative
            return _get_items_from_group_by_search_with_date(
                board_id, group_title, status_col_id, target_status, 
                email_col_id, date_threshold
            )
        
        boards_node = root.get("data", {}).get("boards", [])
        if not boards_node:
            break
        
        items_page = boards_node[0].get("items_page", {})
        items_node = items_page.get("items", [])
        
        if not items_node:
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= 2:
                break
        else:
            consecutive_empty_pages = 0
        
        # Filter items by group ID, status, and date
        oldest_date_in_page = None
        
        for item in items_node:
            group_node = item.get("group")
            if not group_node:
                continue
            
            item_group_id = group_node.get("id", "")
            if group_id != item_group_id:
                continue  # Skip items not in target group
            
            # Check date filter
            created_at_str = item.get("created_at", "")
            if created_at_str:
                try:
                    # Monday.com returns ISO 8601 format: "2026-01-14T10:30:00Z"
                    created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                    # Convert to local timezone (remove timezone for comparison)
                    created_at = created_at.replace(tzinfo=None)
                    
                    # Track oldest date in this page
                    if oldest_date_in_page is None or created_at < oldest_date_in_page:
                        oldest_date_in_page = created_at
                    
                    if created_at < date_threshold:
                        continue  # Skip items older than threshold
                except Exception as e:
                    # If we can't parse the date, skip it to be safe
                    continue
            else:
                # No created_at - skip to be safe
                continue
            
            # Get email and status from column values
            columns_node = item.get("column_values", [])
            email = ""
            current_status = ""
            
            if columns_node:
                for col in columns_node:
                    col_id = col.get("id", "")
                    if status_col_id and status_col_id == col_id:
                        current_status = col.get("text", "").strip()
                    elif email_col_id and email_col_id == col_id:
                        email = col.get("text", "").strip()
            
            # Check status filter
            if status_col_id:
                # Check if status matches (empty or targetStatus)
                if not current_status or (target_status and target_status.lower() == current_status.lower()):
                    result.append(ItemInfo(
                        item_id=int(item.get("id", 0)),
                        email=email,
                        group_name=group_title
                    ))
            else:
                # No status filtering, add all items from group
                result.append(ItemInfo(
                    item_id=int(item.get("id", 0)),
                    email=email,
                    group_name=group_title
                ))
        
        # Early exit optimization: if oldest date in page is before threshold, 
        # and we've already found some items, we can stop (items are sorted by date)
        if oldest_date_in_page and oldest_date_in_page < date_threshold and result:
            break
        
        # Get cursor for next page
        cursor_node = items_page.get("cursor")
        cursor = cursor_node if cursor_node else None
        
        # Stop if no more pages
        if not cursor:
            break
    
    return result


def _get_items_from_group_by_search_with_date(board_id: int, group_title: str,
                                              status_col_id: Optional[str],
                                              target_status: Optional[str],
                                              email_col_id: Optional[str],
                                              date_threshold: datetime) -> List[ItemInfo]:
    """
    Get items from a group by searching through all items with date filtering
    """
    result = []
    cursor = None
    items_checked = 0
    max_items_to_check = 2000  # Limit to avoid infinite loops
    
    print(f"    Searching for items in group '{group_title}' from last 2 days...")
    
    while (cursor is not None or items_checked == 0) and items_checked < max_items_to_check:
        cursor_part = f", cursor: \\\"{cursor}\\\"" if cursor else ""
        
        # Build column values query
        column_parts = []
        if status_col_id:
            column_parts.append(f"column_values(ids: [\\\"{status_col_id}\\\"]) {{ id text }}")
        if email_col_id:
            column_parts.append(f"column_values(ids: [\\\"{email_col_id}\\\"]) {{ id text }}")
        column_values_part = " ".join(column_parts) if column_parts else ""
        
        payload = (
            "{ \"query\": \"query { "
            f"boards(ids:{board_id}) {{ "
            f"items_page(limit: 100{cursor_part}) {{ "
            "cursor "
            "items { "
            "id "
            "created_at "
            "name "
            "group { id title } "
            f"{column_values_part} "
            "} } } }\" }"
        )
        
        response = http_client.post(payload)
        root = json.loads(response)
        
        if "errors" in root:
            print("ERROR: GraphQL errors:")
            print(root["errors"])
            return []
        
        boards_node = root.get("data", {}).get("boards", [])
        if not boards_node:
            break
        
        items_page = boards_node[0].get("items_page", {})
        items_node = items_page.get("items", [])
        
        if not isinstance(items_node, list):
            break
        
        for item in items_node:
            items_checked += 1
            
            # Check date filter
            created_at_str = item.get("created_at", "")
            if created_at_str:
                try:
                    created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                    created_at = created_at.replace(tzinfo=None)
                    if created_at < date_threshold:
                        continue  # Skip items older than threshold
                except Exception as e:
                    # If we can't parse the date, skip it to be safe
                    continue
            
            group_node = item.get("group")
            if not group_node:
                continue
            
            current_group_title = group_node.get("title", "").strip()
            
            # Remove "> " prefix if present
            if current_group_title.startswith("> "):
                current_group_title = current_group_title[2:].strip()
            
            # Check if group matches
            group_matches = False
            if current_group_title == group_title:
                group_matches = True
            else:
                # Flexible matching
                import re
                normalized_current = re.sub(r'\([^)]*\)', '', current_group_title).strip()
                normalized_target = re.sub(r'\([^)]*\)', '', group_title).strip()
                
                if normalized_current.lower() == normalized_target.lower():
                    group_matches = True
                elif _matches_group_flexible(group_title, current_group_title):
                    group_matches = True
            
            if not group_matches:
                continue  # Skip items not in target group
            
            # Group matches, now check status if filtering is enabled
            columns_node = item.get("column_values", [])
            email = ""
            current_status = ""
            
            if columns_node:
                for col in columns_node:
                    col_id = col.get("id", "")
                    if status_col_id and status_col_id == col_id:
                        current_status = col.get("text", "").strip()
                    elif email_col_id and email_col_id == col_id:
                        email = col.get("text", "").strip()
            
            # Check status filter
            if status_col_id:
                if not current_status or (target_status and target_status.lower() == current_status.lower()):
                    result.append(ItemInfo(
                        item_id=int(item.get("id", 0)),
                        email=email,
                        group_name=group_title
                    ))
            else:
                # No status filtering, add all items from group
                result.append(ItemInfo(
                    item_id=int(item.get("id", 0)),
                    email=email,
                    group_name=group_title
                ))
        
        # Get cursor for next page
        cursor_node = items_page.get("cursor")
        cursor = cursor_node if cursor_node else None
        
        if not cursor:
            break
    
    if not result:
        print(f"   WARNING: Searched {items_checked} items but didn't find matching items in group: {group_title}")
    else:
        print(f"   Found {len(result)} matching item(s) in group: {group_title}")
    return result


def _get_item_ids_from_group_id_alternative(board_id: int, group_id: str, limit: int,
                                            status_col_id: Optional[str],
                                            target_status: Optional[str]) -> List[int]:
    """
    Alternative method to get first N items from group with status filtering
    """
    result = []
    cursor = None
    
    while len(result) < limit:
        cursor_part = f", cursor: \\\"{cursor}\\\"" if cursor else ""
        payload = (
            "{ \"query\": \"query { "
            f"boards(ids:{board_id}) {{ "
            f"items_page(limit: 100{cursor_part}) {{ "
            "cursor "
            "items { "
            "id "
            "group { id title } "
            "} } } }\" }"
        )
        
        response = http_client.post(payload)
        root = json.loads(response)
        
        if "errors" in root:
            break
        
        boards_node = root.get("data", {}).get("boards", [])
        if not boards_node:
            break
        
        items_page = boards_node[0].get("items_page", {})
        items_node = items_page.get("items", [])
        
        if isinstance(items_node, list):
            for item in items_node:
                if len(result) >= limit:
                    break
                
                group_node = item.get("group")
                if not group_node:
                    continue
                
                item_group_id = group_node.get("id", "")
                if group_id != item_group_id:
                    continue
                
                # If status filtering is enabled, check status
                if status_col_id:
                    columns_node = item.get("column_values", [])
                    if not columns_node:
                        # No status column = empty status, include if targetStatus is empty/null
                        if not target_status:
                            result.append(int(item.get("id", 0)))
                        continue
                    
                    current_status = ""
                    for col in columns_node:
                        col_id = col.get("id", "")
                        if status_col_id == col_id:
                            current_status = col.get("text", "").strip()
                            break
                    
                    # Check if status matches (empty or targetStatus)
                    if not current_status or (target_status and target_status.lower() == current_status.lower()):
                        result.append(int(item.get("id", 0)))
                else:
                    # No status filtering, add all items from group
                    result.append(int(item.get("id", 0)))
        
        cursor_node = items_page.get("cursor")
        cursor = cursor_node if cursor_node else None
        
        # Stop if we've found enough items
        if len(result) >= limit or not cursor:
            break
    
    return result


def _matches_group_title(target: str, current: str) -> bool:
    """
    Check if two group titles match (flexible matching)
    """
    # Exact match
    if target == current:
        return True
    
    # Remove "> " prefix if present
    if current.startswith("> "):
        current = current[2:].strip()
        if target == current:
            return True
    
    # Use the flexible matching logic
    return _matches_group_flexible(target, current)


def _get_item_ids_from_group_by_search(board_id: int, group_title: str, limit: int,
                                       status_col_id: Optional[str],
                                       target_status: Optional[str]) -> List[int]:
    """
    Get the first N item IDs from a specific group by searching through items (fallback method)
    """
    result = []
    cursor = None
    items_checked = 0
    
    while cursor is not None or items_checked == 0:
        cursor_part = f", cursor: \\\"{cursor}\\\"" if cursor else ""
        column_values_part = "column_values { id text } " if status_col_id else ""
        payload = (
            "{ \"query\": \"query { "
            f"boards(ids:{board_id}) {{ "
            f"items_page(limit: 100{cursor_part}) {{ "
            "cursor "
            "items { "
            "id "
            "name "
            "group { id title } "
            f"{column_values_part}"
            "} } } }\" }"
        )
        
        response = http_client.post(payload)
        root = json.loads(response)
        
        if "errors" in root:
            print("ERROR: GraphQL errors:")
            print(root["errors"])
            return []
        
        boards_node = root.get("data", {}).get("boards", [])
        if not boards_node:
            break
        
        items_page = boards_node[0].get("items_page", {})
        items_node = items_page.get("items", [])
        
        if not isinstance(items_node, list):
            break
        
        for item in items_node:
            items_checked += 1
            group_node = item.get("group")
            if not group_node:
                continue
            
            current_group_title = group_node.get("title", "").strip()
            
            # Remove "> " prefix if present (sometimes Monday.com adds this)
            if current_group_title.startswith("> "):
                current_group_title = current_group_title[2:].strip()
            
            # Check if we've found enough items
            if len(result) >= limit:
                break
            
            # Check if group matches
            group_matches = False
            
            # Exact match first
            if current_group_title == group_title:
                group_matches = True
            else:
                # Flexible matching for groups with wildcards
                import re
                normalized_current = re.sub(r'\([^)]*\)', '', current_group_title).strip()
                normalized_target = re.sub(r'\([^)]*\)', '', group_title).strip()
                
                if normalized_current.lower() == normalized_target.lower():
                    group_matches = True
                elif "NPOP" in group_title and "NPOP" in current_group_title:
                    # Also check if key words match
                    target_key = _extract_key_identifier(group_title)
                    current_key = _extract_key_identifier(current_group_title)
                    if target_key and current_key and target_key == current_key:
                        group_matches = True
                elif "New Tender" in group_title and "New Tender" in current_group_title:
                    # Check for "New Tender" groups - match by region name
                    target_region = _extract_region_from_tender(group_title)
                    current_region = _extract_region_from_tender(current_group_title)
                    if target_region and current_region and target_region == current_region:
                        group_matches = True
                elif _matches_group_flexible(group_title, current_group_title):
                    # Additional flexible matching
                    group_matches = True
            
            if not group_matches:
                continue  # Skip items not in target group
            
            # Group matches, now check status if filtering is enabled
            if status_col_id:
                columns_node = item.get("column_values", [])
                if not columns_node:
                    # No status column = empty status, include if targetStatus is empty/null
                    if not target_status:
                        if not result:
                            print(f"   Found match for group: {group_title} (status: empty)")
                        result.append(int(item.get("id", 0)))
                    continue
                
                current_status = ""
                for col in columns_node:
                    col_id = col.get("id", "")
                    if status_col_id == col_id:
                        current_status = col.get("text", "").strip()
                        break
                
                # Check if status matches (empty or targetStatus)
                if not current_status or (target_status and target_status.lower() == current_status.lower()):
                    if not result:
                        print(f"   Found match for group: {group_title} (status: '{current_status}')")
                    result.append(int(item.get("id", 0)))
            else:
                # No status filtering, add all items from group
                if not result:
                    print(f"   Found match for group: {group_title}")
                result.append(int(item.get("id", 0)))
        
        # Get cursor for next page
        cursor_node = items_page.get("cursor")
        cursor = cursor_node if cursor_node else None
        
        if not cursor:
            break
    
    if not result:
        print(f"   WARNING: Searched {items_checked} items but didn't find group: {group_title}")
    else:
        print(f"   Found {len(result)} item(s) in group: {group_title}")
    return result


def _matches_group_flexible(target_group: str, current_group: str) -> bool:
    """
    Flexible group matching using keywords
    """
    # For NPOP groups - check LA3 vs LA6
    if "NPOP" in target_group and "NPOP" in current_group:
        target_has_la3 = "LA3" in target_group
        target_has_la6 = "LA6" in target_group
        current_has_la3 = "LA3" in current_group
        current_has_la6 = "LA6" in current_group
        
        if (target_has_la3 and current_has_la3) or (target_has_la6 and current_has_la6):
            # Check if the second part matches (SOBEYSMIF vs MIFLAOPS)
            if "SOBEYSMIF" in target_group and "SOBEYSMIF" in current_group:
                return True
            if "MIFLAOPS" in target_group and "MIFLAOPS" in current_group:
                return True
    
    # For New Tender groups - check region
    if "New Tender" in target_group and "New Tender" in current_group:
        regions = ["Atlantic", "West", "Quebec", "Ontario"]
        for region in regions:
            if region in target_group and region in current_group:
                return True
    
    # For Pepsi groups
    if ("Pepsi" in target_group and "Pepsi" in current_group and
        "Load Tender" in target_group and "Load Tender" in current_group):
        return True
    
    return False


def _extract_key_identifier(group_title: str) -> Optional[str]:
    """
    Extract key identifier from group title (e.g., "LA3" from "NPOP (LA3)/{SOBEYSMIF}")
    """
    start = group_title.find('(')
    end = group_title.find(')')
    if start >= 0 and end > start:
        return group_title[start + 1:end].strip()
    return None


def _extract_region_from_tender(group_title: str) -> Optional[str]:
    """
    Extract region from "New Tender" group title (e.g., "Atlantic" from "New Tender - Sobeys MIF (Atlantic)")
    """
    last_paren = group_title.rfind('(')
    last_close_paren = group_title.rfind(')')
    if last_paren >= 0 and last_close_paren > last_paren:
        return group_title[last_paren + 1:last_close_paren].strip()
    return None


def update_status(item_id: int, new_status: str, board_id: int) -> None:
    """
    Update Status column
    """
    status_col_id = get_status_column_id(board_id)
    
    payload = (
        "{ \"query\": \"mutation { "
        "change_simple_column_value( "
        f"board_id: {board_id}, "
        f"item_id: {item_id}, "
        f"column_id: \\\"{status_col_id}\\\", "
        f"value: \\\"{new_status}\\\" "
        ") { id } "
        "}\" }"
    )
    
    response = http_client.post(payload)
    root = json.loads(response)
    
    # Check for errors
    if "errors" in root:
        print(f"ERROR: Failed to update status for item {item_id}")
        print(f"Errors: {root['errors']}")
        raise RuntimeError(f"Status update failed: {root['errors']}")
    
    print(f"Status updated to '{new_status}' for item {item_id}")
