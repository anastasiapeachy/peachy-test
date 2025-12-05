"""
Notion Old Pages Monitor
Scans Notion workspace for pages not edited in the past year.
Sends the top 10 oldest pages to Slack.
"""
from notion_client import Client
from notion_client.errors import APIResponseError
import os
import time
import requests
from datetime import datetime, timezone, timedelta
from typing import List, Dict

# ======================================================
# CONFIGURATION
# ======================================================
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Validate required environment variables
if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN environment variable is required")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID environment variable is required")

notion = Client(auth=NOTION_TOKEN)
ONE_YEAR_AGO = datetime.now(timezone.utc) - timedelta(days=365)

# ======================================================
# API REQUEST HANDLER
# ======================================================
def safe_request(func, *args, **kwargs):
    """
    Wrapper for Notion API requests with retry logic and rate limiting.
    Handles 429 rate limits and 5xx server errors automatically.
    """
    max_retries = 8
    base_delay = 0.3
    backoff_multiplier = 2
    max_backoff = 30

    for attempt in range(max_retries):
        try:
            # Small delay to avoid hitting rate limits
            time.sleep(base_delay)
            return func(*args, **kwargs)
        except APIResponseError as e:
            status = e.status

            # Handle rate limiting
            if status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"Rate limit hit (429). Waiting {retry_after}s before retry...")
                time.sleep(retry_after)
                continue

            # Handle server errors with exponential backoff
            if 500 <= status <= 599:
                backoff_time = min(backoff_multiplier ** attempt, max_backoff)
                print(f"Server error ({status}). Retrying in {backoff_time}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(backoff_time)
                continue

            # For other errors, raise immediately
            raise

    raise RuntimeError(f"Notion API failed after {max_retries} retries")

# ======================================================
# NOTION HELPERS
# ======================================================
def build_notion_url(page_id: str) -> str:
    """Convert a page ID to a Notion URL."""
    clean_id = page_id.replace("-", "")
    return f"https://www.notion.so/{clean_id}"

def get_page_info(page_id: str) -> Dict[str, any]:
    """
    Retrieve page metadata including title and last edited time.
    """
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    # Extract title from properties
    title = "Untitled"
    if "properties" in page:
        for prop in page["properties"].values():
            if prop["type"] == "title" and prop.get("title"):
                title = prop["title"][0]["plain_text"]
                break

    # Parse last edited timestamp
    last_edited_raw = page.get("last_edited_time", "")
    last_edited = datetime.fromisoformat(last_edited_raw.replace("Z", "+00:00")).astimezone(timezone.utc)

    return {
        "id": page_id,
        "title": title,
        "url": build_notion_url(page_id),
        "last_edited": last_edited,
    }

def get_block_children(block_id: str) -> List[Dict]:
    """
    Retrieve all child blocks with pagination support.
    """
    blocks = []
    cursor = None

    while True:
        response = safe_request(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor
        )
        blocks.extend(response.get("results", []))
        cursor = response.get("next_cursor")
        
        if not cursor:
            break
        
        time.sleep(0.1)  # Small delay between pagination requests

    return blocks

def get_database_pages(database_id: str) -> List[Dict]:
    """
    Retrieve all pages from a database with pagination support.
    """
    pages = []
    cursor = None

    while True:
        response = safe_request(
            notion.databases.query,
            database_id=database_id,
            start_cursor=cursor
        )
        pages.extend(response.get("results", []))
        cursor = response.get("next_cursor")
        
        if not cursor:
            break
        
        time.sleep(0.1)  # Small delay between pagination requests

    return pages

def is_empty_page(page_id: str) -> bool:
    """
    Check if a page has any content blocks.
    """
    try:
        children = get_block_children(page_id)
        return len(children) == 0
    except Exception as e:
        print(f"Error checking if page {page_id} is empty: {e}")
        return False

# ======================================================
# RECURSIVE PAGE SCANNER
# ======================================================
def scan_all_pages(block_id: str) -> List[Dict]:
    """
    Recursively scan all pages and databases starting from a root block.
    Returns a list of page information dictionaries.
    """
    pages = []
    children = get_block_children(block_id)

    for block in children:
        block_type = block["type"]
        block_id = block["id"]

        # Handle child pages
        if block_type == "child_page":
            try:
                page_info = get_page_info(block_id)
                pages.append(page_info)
                # Recursively scan child pages
                pages.extend(scan_all_pages(block_id))
            except Exception as e:
                print(f"Skipping child_page {block_id}: {e}")

        # Handle child databases
        elif block_type == "child_database":
            try:
                db_pages = get_database_pages(block_id)
                for db_page in db_pages:
                    page_id = db_page["id"]

                    # Skip empty database pages
                    if is_empty_page(page_id):
                        print(f"Skipping empty database page: {page_id}")
                        continue

                    try:
                        page_info = get_page_info(page_id)
                        pages.append(page_info)
                        # Recursively scan database pages
                        pages.extend(scan_all_pages(page_id))
                    except Exception as e:
                        print(f"Skipping database page {page_id}: {e}")
            except Exception as e:
                print(f"Skipping child_database {block_id}: {e}")

        # Handle deeply nested blocks (e.g., toggle lists, columns)
        if block.get("has_children") and block_type not in ("child_page", "child_database"):
            try:
                pages.extend(scan_all_pages(block_id))
            except Exception as e:
                print(f"Skipping nested block {block_id}: {e}")

    return pages

# ======================================================
# SLACK NOTIFICATION
# ======================================================
def send_slack_notification(old_pages: List[Dict], total_old_count: int) -> None:
    """
    Send Slack notification with top 10 oldest pages.
    """
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL not configured. Skipping Slack notification.")
        return

    if not old_pages:
        print("No old pages found. Skipping Slack notification.")
        return

    # Format the top 10 pages
    pages_text = []
    for i, page in enumerate(old_pages[:10], 1):
        title = page["title"]
        url = page["url"]
        days_ago = (datetime.now(timezone.utc) - page["last_edited"]).days
        pages_text.append(f"{i}. <{url}|{title}> - _{days_ago} days ago_")

    pages_list = "\n".join(pages_text)
    
    # Add note if there are more pages
    more_text = ""
    if total_old_count > 10:
        more_text = f"\n\n_...and {total_old_count - 10} more pages not edited in over a year_"

    message = {
        "text": f"📄 Notion Old Pages Report - {total_old_count} pages found",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "📄 Notion Old Pages Report"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Found *{total_old_count}* pages that haven't been edited for over a year.\n\n*Top 10 oldest pages:*"
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": pages_list + more_text
                }
            }
        ]
    }

    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json=message,
            timeout=10
        )
        
        if response.status_code == 200:
            print("Slack notification sent successfully.")
        else:
            print(f"Slack notification failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error sending Slack notification: {e}")

# ======================================================
# MAIN EXECUTION
# ======================================================
def main():
    """
    Scan Notion workspace and send Slack notification with top 10 oldest pages.
    """
    print("Starting Notion workspace scan...")
    print(f"Looking for pages not edited since {ONE_YEAR_AGO.strftime('%Y-%m-%d')}")
    
    all_pages = scan_all_pages(ROOT_PAGE_ID)
    print(f"Total pages discovered: {len(all_pages)}")

    # Filter pages older than one year
    old_pages = [
        page for page in all_pages
        if page["last_edited"] < ONE_YEAR_AGO
    ]

    # Sort by last edited date (oldest first)
    old_pages.sort(key=lambda x: x["last_edited"])
    
    total_old_count = len(old_pages)
    print(f"Pages not edited in over a year: {total_old_count}")

    # Send Slack notification
    send_slack_notification(old_pages, total_old_count)

if __name__ == "__main__":
    main()
