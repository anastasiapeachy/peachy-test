from notion_client import Client
from notion_client.errors import APIResponseError
import json
import os
import time
import requests
from datetime import datetime, timezone, timedelta


# ==============================
# ENVIRONMENT
# ==============================
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
STORAGE_FILE = "notion_tracker_data/known_pages.json"

notion = Client(auth=NOTION_TOKEN)

WEEK_DELAY = timedelta(days=7)
NOW = datetime.now(timezone.utc)


# ==============================
# SAFE REQUEST (retry)
# ==============================
def safe_request(func, *args, **kwargs):
    retries = 7
    delay = 0.25
    backoff = 1

    for attempt in range(retries):
        try:
            time.sleep(delay)
            return func(*args, **kwargs)

        except APIResponseError as e:
            status = e.status

            if status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limit → waiting {retry_after}s…")
                time.sleep(retry_after)
                continue

            if 500 <= status <= 599:
                print(f"[{status}] Server error → retry in {backoff}s…")
                time.sleep(backoff)
                backoff = min(backoff * 2, 20)
                continue

            raise

    raise RuntimeError("Notion failed after multiple retries")


# ==============================
# HELPERS
# ==============================
def notion_url(page_id):
    return f"https://www.notion.so/{page_id.replace('-', '')}"


def get_page_info(page_id):
    """Extract title, author, created_time, public_url."""
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    # Title
    title = "Untitled"
    for prop in page.get("properties", {}).values():
        if prop["type"] == "title" and prop.get("title"):
            title = prop["title"][0]["plain_text"]
            break

    # Author
    author = "Unknown"
    created_by = page.get("created_by", {})
    if created_by:
        try:
            user_data = safe_request(notion.users.retrieve, user_id=created_by["id"])
            author = user_data.get("name") or created_by.get("id")
        except Exception:
            pass

    # Timestamps
    created_raw = page.get("created_time")
    created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))

    last_edited_raw = page.get("last_edited_time")
    last_edited_dt = datetime.fromisoformat(last_edited_raw.replace("Z", "+00:00"))

    # Is page public?
    is_public = bool(page.get("public_url"))

    return {
        "id": page_id,
        "title": title,
        "author": author,
        "url": notion_url(page_id),
        "created": created_dt,
        "last_edited": last_edited_dt,
        "is_public": is_public,
    }


def get_block_children(block_id):
    """Load all children with pagination."""
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


def get_database_pages(database_id):
    """Load all rows of a database."""
    pages = []
    cursor = None

    while True:
        resp = safe_request(
            notion.databases.query,
            database_id=database_id,
            start_cursor=cursor
        )
        pages.extend(resp.get("results", []))

        cursor = resp.get("next_cursor")
        if not cursor:
            break

        time.sleep(0.1)

    return pages


def is_empty_page(page_id):
    """Page without blocks = empty (skip)."""
    try:
        blocks = get_block_children(page_id)
        return len(blocks) == 0
    except Exception:
        return False


# ==============================
# FULL RECURSIVE SCANNER
# (child_page, child_database, nested blocks)
# ==============================
def get_all_pages(block_id):
    pages = []
    children = get_block_children(block_id)

    for block in children:
        btype = block["type"]

        # ---- child_page ----
        if btype == "child_page":
            pid = block["id"]
            try:
                info = get_page_info(pid)
                pages.append(info)
                pages.extend(get_all_pages(pid))
            except Exception as e:
                print(f"Skip page {pid}: {e}")

        # ---- child_database ----
        elif btype == "child_database":
            db_id = block["id"]
            try:
                db_pages = get_database_pages(db_id)
                for row in db_pages:
                    pid = row["id"]

                    if is_empty_page(pid):
                        print(f"Skip empty DB row: {pid}")
                        continue

                    try:
                        info = get_page_info(pid)
                        pages.append(info)
                        pages.extend(get_all_pages(pid))
                    except Exception as e:
                        print(f"Skip DB page {pid}: {e}")
            except Exception as e:
                print(f"Skip database {db_id}: {e}")

        # ---- nested blocks with children ----
        if block.get("has_children") and btype not in ("child_page", "child_database"):
            try:
                pages.extend(get_all_pages(block["id"]))
            except Exception as e:
                print(f"Skip nested block {block['id']}: {e}")

    return pages


# ==============================
# CACHE MANAGEMENT
# ==============================
def load_known_pages():
    os.makedirs(os.path.dirname(STORAGE_FILE), exist_ok=True)
    if os.path.exists(STORAGE_FILE):
        with open(STORAGE_FILE, "r") as f:
            return json.load(f)
    return []


def save_known_pages(pages):
    os.makedirs(os.path.dirname(STORAGE_FILE), exist_ok=True)
    with open(STORAGE_FILE, "w") as f:
        json.dump(pages, f, indent=2, default=str)


# ==============================
# SLACK
# ==============================
def send_to_slack(message):
    if not SLACK_WEBHOOK_URL:
        print("No Slack webhook found.")
        return
    requests.post(SLACK_WEBHOOK_URL, json={"text": message})


# ==============================
# MAIN LOGIC
# ==============================
def main():
    print("Scanning Notion deeply…")
    known = load_known_pages()
    known_ids = {p["id"] for p in known}

    all_pages = get_all_pages(ROOT_PAGE_ID)

    # New pages (not in cache)
    new_pages = [p for p in all_pages if p["id"] not in known_ids]

    # Filter: only public AND older than 7 days
    eligible = []
    for p in new_pages:
        if p["is_public"] and p["created"] < (NOW - WEEK_DELAY):
            eligible.append(p)

    # Slack message
    if eligible:
        lines = ["🆕 *New public Notion pages (older than 7 days):*\n"]
        for p in eligible:
            lines.append(
                f":blue_book: *{p['title']}*\n"
                f":link: {p['url']}\n"
                f":writing_hand: {p['author']}_
