from notion_client import Client
from notion_client.errors import APIResponseError
import os
import time
import json
import requests
from datetime import datetime, timezone, timedelta


# =========================================
# CONFIG
# =========================================
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")  # ID раздела
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

STORAGE_FILE = "notion_tracker_data/known_pages.json"

WEEK_DELAY = timedelta(days=7)
NOW = datetime.now(timezone.utc)

notion = Client(auth=NOTION_TOKEN)


# =========================================
# SAFE REQUEST (RETRY LOGIC)
# =========================================
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

            # Rate limited
            if status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limit → wait {retry_after}s…")
                time.sleep(retry_after)
                continue

            # Server errors
            if 500 <= status <= 599:
                print(f"[{status}] Server error → retry in {backoff}s…")
                time.sleep(backoff)
                backoff = min(backoff * 2, 20)
                continue

            raise

    raise RuntimeError("Notion did not respond after retries")


# =========================================
# HELPERS
# =========================================
def notion_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_page_info(page_id):
    """Extract title, author, timestamps, public_url."""
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    # ------------ Title ------------
    title = "Untitled"
    for prop in page.get("properties", {}).values():
        if prop["type"] == "title" and prop.get("title"):
            title = prop["title"][0]["plain_text"]
            break

    # ------------ Author ------------
    author = "Unknown"
    created_by = page.get("created_by", {})
    if created_by:
        try:
            data = safe_request(notion.users.retrieve, user_id=created_by["id"])
            author = data.get("name") or created_by.get("id")
        except Exception:
            pass

    # ------------ Time ------------
    created_raw = page.get("created_time")
    created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))

    last_edited_raw = page.get("last_edited_time")
    last_edited_dt = datetime.fromisoformat(last_edited_raw.replace("Z", "+00:00"))

    # ------------ Public? ------------
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
    pages = []
    cursor = None

    while True:
        resp = safe_request(
            notion.datases.query,
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
    """Skip empty database rows."""
    try:
        children = get_block_children(page_id)
        return len(children) == 0
    except Exception:
        return False


# =========================================
# FULL RECURSIVE SCAN
# =========================================
def get_all_pages(block_id):
    pages = []
    children = get_block_children(block_id)

    for block in children:
        btype = block["type"]

        # ------------ Normal Notion Page ------------
        if btype == "child_page":
            pid = block["id"]
            try:
                info = get_page_info(pid)
                pages.append(info)
                pages.extend(get_all_pages(pid))
            except Exception as e:
                print(f"Skip page {pid}: {e}")

        # ------------ Database ------------
        elif btype == "child_database":
            db_id = block["id"]
            try:
                rows = get_database_pages(db_id)
                for row in rows:
                    pid = row["id"]

                    if is_empty_page(pid):
                        print(f"Skip empty DB page: {pid}")
                        continue

                    try:
                        info = get_page_info(pid)
                        pages.append(info)
                        pages.extend(get_all_pages(pid))
                    except Exception as e:
                        print(f"Skip DB row {pid}: {e}")

            except Exception as e:
                print(f"Skip database {db_id}: {e}")

        # ------------ Nested blocks ------------
        if block.get("has_children") and btype not in ("child_page", "child_database"):
            try:
                pages.extend(get_all_pages(block["id"]))
            except Exception as e:
                print(f"Skip nested block {block['id']}: {e}")

    return pages


# =========================================
# CACHE
# =========================================
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


# =========================================
# SLACK
# =========================================
def send_to_slack(message):
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL not set.")
        return
    requests.post(SLACK_WEBHOOK_URL, json={"text": message})


# =========================================
# MAIN
# =========================================
def main():
    print("Deep scan of Notion…")

    known = load_known_pages()
    known_ids = {p["id"] for p in known}

    all_pages = get_all_pages(ROOT_PAGE_ID)

    # pages → these are new (not in cache)
    new_pages = [p for p in all_pages if p["id"] not in known_ids]

    # Apply filters:
    # - public pages only
    # - created at least 7 days ago
    eligible = []
    for p in new_pages:
        if p["is_public"] and p["created"] < (NOW - WEEK_DELAY):
            eligible.append(p)

    # Send to Slack
    if eligible:
        lines = ["🆕 *New public Notion pages (older than 7 days):*\n"]
        for p in eligible:
            lines.append(
                f":blue_book: *{p['title']}*\n"
                f":link: {p['url']}\n"
                f":writing_hand: {p['author']}\n"
            )
        send_to_slack("\n".join(lines))
    else:
        print("No eligible pages to send.")

    # Save full list to cache
    save_known_pages(all_pages)
    print("Done.")


if __name__ == "__main__":
    main()
