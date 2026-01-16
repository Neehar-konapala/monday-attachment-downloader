MONDAY_API_URL = "https://api.monday.com/v2"
MONDAY_API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjYwMDkwOTQ0NSwiYWFpIjoxMSwidWlkIjo3MDg3NjkyMiwiaWFkIjoiMjAyNS0xMi0yNFQxNTowMzo0MC4xMzNaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTcyMTMyOTEsInJnbiI6InVzZTEifQ.ak4kJC_6t0EDt07YcP8NRIBCE_2kudS0KjvnDYgIHXU"

WORKSPACE_NAME = "Bot Activity"
BOARD_NAME = "BOT: Shipment Creation"

# Status column title - will be resolved to actual column ID dynamically
STATUS_COLUMN_TITLE = "Status"

# Email column title - will be resolved to actual column ID dynamically
EMAIL_COLUMN_TITLE = "Email"

# Target status values to process
TARGET_STATUS = "Retry"

# Base download directory
BASE_DOWNLOAD_DIR = "C:/Users/neeha/Downloads/monday_downloads"

# Group names
GROUP_1 = "NPOP (LA3)/{SOBEYSMIF}"
GROUP_2 = "NPOP (LA6)/{MIFLAOPS}"
GROUP_3 = "New Tender - Sobeys MIF (Atlantic)"
GROUP_4 = "New Tender - Sobeys MIF (West)"
GROUP_5 = "New Tender - Sobeys MIF (Quebec)"
GROUP_6 = "New Tender - Sobeys MIF (Ontario)"
GROUP_7 = "Pepsi (Load Tender issued (********))"

# Folder mappings: Group name -> Download folder
GROUP_FOLDER_MAP = {
    GROUP_1: BASE_DOWNLOAD_DIR + "/sobeys_old_template",
    GROUP_2: BASE_DOWNLOAD_DIR + "/sobeys_old_template",
    GROUP_3: BASE_DOWNLOAD_DIR + "/sobeys_template-1",
    GROUP_4: BASE_DOWNLOAD_DIR + "/sobeys_template-1",
    GROUP_5: BASE_DOWNLOAD_DIR + "/sobeys_template-1",
    GROUP_6: BASE_DOWNLOAD_DIR + "/sobeys_template-1",
    GROUP_7: BASE_DOWNLOAD_DIR + "/pepsi"
}

# New status to set after downloading
NEW_STATUS = "In Queue"

# Testing mode: only process first item from each group
TESTING_MODE = False

# Number of days to look back for items (0 = today only, 1 = today and yesterday, etc.)
DAYS_TO_PROCESS = 1  # Process items from today and yesterday (past 2 days)
