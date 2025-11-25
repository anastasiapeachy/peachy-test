"""
REQUIREMENTS:
pip install notion-client==2.2.1
pip install requests
"""

from notion_client import Client
from notion_client.errors import APIResponseError
import os
import time
import json
import requests
from datetime import datetime, timezone, timedelta

# ----------------------------------------------------
# ENVIRONMENT
# ----------------------------------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN is not set")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID is not set")

STORAGE_FILE = "notion_tracker_data/known_pages.json"
notion = Client(auth=NOTION_TOKEN)

WEEK_AGO = datetime.now(timezone.utc) - timedelta(days=7)


# ----------------------------------------------------
# SAFE REQUEST WRAPPER
# ----------------------------------------------------
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
                print(f"[429] Rate limited → wait {retry_after}s…")
                time.sleep(retry_after)
                continue

            if 500 <= status <= 599:
                print(f"[{status}] Server error → retry in {backoff}s…")
                time.sleep(backoff)
                backoff = min(backoff * 2, 20)
                continue

            raise

    raise RuntimeError("Notion failed after retries")


# ----------------------------------------------------
# HELPERS
# ----------------------------------------------------
def notion_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_page_info(page_id):
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    title = "Untitled"
    for prop in page.get("properties", {}).values():
        if prop["type"] == "title" and prop.get("title"):
            title = prop["title"][0]["plain_text"]
            break

    created_raw = page.get("created_time", "")
    created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00")).astimezone(timezone.utc)

    author = page.get("created_by", {}).get("name") or "Unknown"

    # Check if page is publicly shared
    is_public = bool(page.get("public_url"))

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "author": author,
        "created_time": created_dt,
        "is_public": is_public
    }


# ----------------------------------------------------
# FULL DEEP SCANNER (IDENTICAL TO YOUR OLD-PAGES VERSION)
# ----------------------------------------------------
def get_all_pages(block_id):
    pages = []
    cursor = None

    while True:
        resp = safe_request(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor
        )

        for block in resp["results"]:
            btype = block["type"]

            # child_page
            if btype == "child_page":
                pid = block["id"]
                try:
                    info = get_page_info(pid)
                    pages.append(info)
                    pages.extend(get_all_pages(pid))
                except Exception as e:
                    print(f"Skip child_page {pid}: {e}")

            # deep-scan any block with children
            if block.get("has_children", False):
                try:
                    pages.extend(get_all_pages(block["id"]))
                except Exception:
                    pass

            # forced scanning inside major content blocks
            if btype in [
                "column", "column_list",
                "bulleted_list_item", "numbered_list_item",
                "toggle", "to_do", "synced_block",
                "paragraph", "quote", "callout"
            ]:
                try:
                    pages.extend(get_all_pages(block["id"]))
                except Exception:
                    pass

        cursor = resp.get("next_cursor")
        if not cursor:
            break

        time.sleep(0.15)

    return pages


# ----------------------------------------------------
# STORAGE
# ----------------------------------------------------
def load_known():
    os.makedirs(os.path.dirname(STORAGE_FILE), exist_ok=True)
    if os.path.exists(STORAGE_FILE):
        with open(STORAGE_FILE, "r") as f:
            data = json.load(f)
        print(f"Loaded known pages: {len(data)}")
        return data
    return []


def save_known(pages):
    os.makedirs(os.path.dirname(STORAGE_FILE), exist_ok=True)
    with open(STORAGE_FILE, "w") as f:
        json.dump(pages, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(pages)} pages to cache")


# ----------------------------------------------------
# SLACK
# ----------------------------------------------------
def send_to_slack(message):
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL is missing")
        return
    requests.post(SLACK_WEBHOOK_URL, json={"text": message})


# ----------------------------------------------------
# MAIN
# ----------------------------------------------------
def main():
    known = load_known()
    known_ids = {p["id"] for p in known}

    print("Deep scan of Notion…")
    pages = get_all_pages(ROOT_PAGE_ID)
    print(f"Found total pages BEFORE filtering: {len(pages)}")

    # NEW pages (not seen before)
    new_pages = [p for p in pages if p["id"] not in known_ids]

    # Filter: only public + older than 7 days
    eligible = [
        p for p in new_pages
        if p["is_public"] and p["created_time"] < WEEK_AGO
    ]

    if eligible:
        message = "*🆕 New public articles older than 7 days:*\n\n"
        for p in eligible:
            message += f":blue_book: *{p['title']}*\n{p['url']}\n✍️ {p['author']}\n\n"

        send_to_slack(message)
    else:
        print("No eligible pages to send.")

    # Update the cache
    save_known(pages)


if __name__ == "__main__":
    main()
