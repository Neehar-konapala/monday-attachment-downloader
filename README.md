# Monday.com Attachment Downloader Toolkit

A toolkit for downloading attachments from Monday.com board items based on status and date filters.

## Description

This toolkit processes items from Monday.com boards and downloads their attachments. It:
- Processes items from the last 2 days (today and yesterday)
- Filters items by status: empty or "Retry"
- Updates item status to "In Queue" after downloading
- Downloads attachments with formatted filenames: `filename__itemid__email__groupname.ext`

## Capabilities

### download_attachments

Downloads attachments from Monday.com board items.

**Parameters:**
- `api_token` (string, required): Monday.com API authentication token
- `workspace_name` (string, required): Name of the workspace containing the board
- `board_name` (string, required): Name of the board to process
- `groups` (array of strings, required): List of group names to process
- `group_folder_map` (object, required): Mapping of group names to download folder paths

**Example:**
```json
{
  "capability": "download_attachments",
  "args": {
    "api_token": "your-api-token",
    "workspace_name": "Bot Activity",
    "board_name": "BOT: Shipment Creation",
    "groups": [
      "Pepsi (Load Tender issued (********))"
    ],
    "group_folder_map": {
      "Pepsi (Load Tender issued (********))": "C:/Users/neeha/Downloads/monday_downloads/pepsi"
    }
  }
}
```

## Configuration

The following values are hard-coded:
- **TARGET_STATUS**: "Retry" (processes items with empty status or "Retry")
- **NEW_STATUS**: "In Queue" (status set after downloading)
- **DAYS_TO_PROCESS**: 1 (processes items from today and yesterday = 2 days)
- **STATUS_COLUMN_TITLE**: "Status"
- **EMAIL_COLUMN_TITLE**: "Email"

## Output

Returns a JSON object with:
- `result`: Contains `groups_processed`, `total_groups`, `success`, `failed`, and `board_id`
- `error`: Error message if something went wrong
- `capability`: Name of the capability executed

## Dependencies

- requests>=2.31.0

## Installation

```bash
pip install -r requirements.txt
```

## Testing

Test the toolkit locally:
```bash
echo '{"capability": "download_attachments", "args": {"api_token": "token", "workspace_name": "Workspace", "board_name": "Board", "groups": ["Group1"], "group_folder_map": {"Group1": "/path/to/folder"}}}' | python main.py
```
