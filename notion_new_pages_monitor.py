from notion_client import Client
from notion_client.errors import APIResponseError
import os
import time
import requests
from datetime import datetime, timezone, timedelta

# ------------------------
# ENVIRONMENT
# ------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN is not set")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID is not set")

notion = Client(auth=NOTION_TOKEN)

# Time range: 7 to 21 days ago
SEVEN_DAYS_AGO = datetime.now(timezone.utc) - timedelta(days=7)
TWENTY_ONE_DAYS_AGO = datetime.now(timezone.utc) - timedelta(days=21)

# Timeout protection (5 hours max)
MAX_EXECUTION_TIME = 5 * 60 * 60  # 5 hours in seconds
START_TIME = time.time()

# ------------------------
# SAFE REQUEST (retry)
# ------------------------
def safe_request(func, *args, **kwargs):
    max_retries = 8
    delay = 0.25
    backoff = 1

    for attempt in range(max_retries):
        try:
            time.sleep(delay)
            return func(*args, **kwargs)
        except APIResponseError as e:
            status = e.status

            if status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limit → wait {retry_after}s")
                time.sleep(retry_after)
                continue

            if 500 <= status <= 599:
                print(f"[{status}] Server error → retry in {backoff}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue

            raise

    raise RuntimeError("Notion is not responding after retries")

# ------------------------
# TIMEOUT CHECK
# ------------------------
def check_timeout():
    if time.time() - START_TIME > MAX_EXECUTION_TIME:
        raise RuntimeError("⏱️ Execution timeout exceeded (5 hours)")

# ------------------------
# HELPERS
# ------------------------
def notion_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"

def get_page_info(page_id):
    """Extracts title, url, author, created."""
    check_timeout()
    
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    # Title
    title = "Untitled"
    for prop in page.get("properties", {}).values():
        if prop["type"] == "title" and prop.get("title"):
            title = prop["title"][0]["plain_text"]
            break

    # Author
    created_by = page.get("created_by", {})
    author = created_by.get("name") or created_by.get("id", "Unknown")

    # Fix missing name
    if author == created_by.get("id"):
        try:
            user = safe_request(notion.users.retrieve, user_id=created_by["id"])
            author = user.get("name") or author
        except:
            pass

    # Created time
    created_raw = page.get("created_time", "")
    created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00")).astimezone(timezone.utc)

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "author": author,
        "created": created_dt
    }

# ------------------------
# BLOCK CHILDREN
# ------------------------
def get_block_children(block_id):
    check_timeout()
    
    blocks = []
    cursor = None

    while True:
        resp = safe_request(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor
        )

        blocks.extend(resp.get("results", []))

        cursor = resp.get("next_cursor")
        if not cursor:
            break

        time.sleep(0.1)

    return blocks

# ------------------------
# FULL SCAN WITH VISITED TRACKING
# ------------------------
def get_all_pages(block_id, visited=None, depth=0):
    """
    Recursively scan pages with cycle detection.
    visited: set of already-processed page/block IDs
    depth: recursion depth (for debugging)
    """
    if visited is None:
        visited = set()
    
    # Prevent cycles
    if block_id in visited:
        return []
    
    visited.add(block_id)
    check_timeout()
    
    # Safety: limit recursion depth
    if depth > 50:
        print(f"⚠️ Max depth (50) reached at block {block_id}")
        return []
    
    pages = []
    
    try:
        children = get_block_children(block_id)
    except Exception as e:
        print(f"❌ Failed to get children of {block_id}: {e}")
        return []

    for block in children:
        check_timeout()
        
        btype = block["type"]
        bid = block["id"]

        # Skip if already visited
        if bid in visited:
            continue

        # 1️⃣ Child page
        if btype == "child_page":
            try:
                info = get_page_info(bid)
                pages.append(info)
                # Recursively scan this page's children
                pages.extend(get_all_pages(bid, visited, depth + 1))
            except Exception as e:
                print(f"Skip child_page {bid}: {e}")

        # 2️⃣ Child database
        elif btype == "child_database":
            try:
                db_pages = safe_request(notion.databases.query, database_id=bid)
                for db_page in db_pages["results"]:
                    pid = db_page["id"]
                    
                    if pid in visited:
                        continue

                    try:
                        info = get_page_info(pid)
                        pages.append(info)
                        pages.extend(get_all_pages(pid, visited, depth + 1))
                    except Exception as e:
                        print(f"Skip db row {pid}: {e}")

            except Exception as e:
                print(f"Skip database {bid}: {e}")

        # 3️⃣ Nested blocks (columns, toggles, etc.) - ONLY if not a page/database
        elif btype not in ["child_page", "child_database"] and block.get("has_children", False):
            try:
                # Don't add to pages list, just recurse to find nested pages
                pages.extend(get_all_pages(bid, visited, depth + 1))
            except Exception as e:
                print(f"Skip nested block {bid}: {e}")

    return pages

# ------------------------
# SLACK
# ------------------------
def send_slack(text):
    if not SLACK_WEBHOOK_URL:
        print("Slack webhook missing")
        return

    resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text})
    print("Slack:", resp.status_code, resp.text)

# ------------------------
# MAIN
# ------------------------
def main():
    try:
        print("Scanning Notion deeply…")
        pages = get_all_pages(ROOT_PAGE_ID)
        print(f"Total discovered pages: {len(pages)}")

        # Filter pages created between 7 and 21 days ago
        filtered_pages = [
            p for p in pages 
            if TWENTY_ONE_DAYS_AGO <= p["created"] <= SEVEN_DAYS_AGO
        ]
        print(f"Pages created 7-21 days ago: {len(filtered_pages)}")

        if not filtered_pages:
            send_slack("❗ No pages found created between 7 and 21 days ago.")
            return

        msg = ["🆕 *Pages created 7-21 days ago (biweekly report):*", ""]
        for p in filtered_pages:
            msg.append(
                f"📘 *{p['title']}*\n"
                f"🔗 {p['url']}\n"
                f"✍️ {p['author']}\n"
            )

        send_slack("\n".join(msg))
        
    except RuntimeError as e:
        error_msg = f"❌ Script error: {str(e)}"
        print(error_msg)
        send_slack(error_msg)
        raise

if __name__ == "__main__":
    main()
