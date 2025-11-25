from notion_client import Client
from notion_client.errors import APIResponseError
import os
import time
import requests
from datetime import datetime, timezone, timedelta
import json

# ======================================================
# ENV
# ======================================================
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN is not set")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID is not set")

notion = Client(auth=NOTION_TOKEN)

STORAGE_FILE = "notion_tracker_data/known_pages.json"
ONE_WEEK_AGO = datetime.now(timezone.utc) - timedelta(days=7)


# ======================================================
# SAFE REQUEST (must appear BEFORE functions using it)
# ======================================================
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
            # Rate limit
            if status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limit → waiting {retry_after}s…")
                time.sleep(retry_after)
                continue
            # Server errors
            if 500 <= status <= 599:
                print(f"[{status}] Server error → retrying in {backoff}s…")
                time.sleep(backoff)
                backoff = min(backoff * 2, 25)
                continue
            raise
    raise RuntimeError("Notion API not responding after retries")


# ======================================================
# HELPERS
# ======================================================
def notion_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_page_info(page_id):
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    title = "Untitled"
    if "properties" in page:
        for prop in page["properties"].values():
            if prop["type"] == "title" and prop.get("title"):
                title = prop["title"][0]["plain_text"]
                break

    created_raw = page.get("created_time", "")
    created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00")).astimezone(timezone.utc)

    # PUBLIC PAGE CHECK
    is_public = bool(page.get("public_url"))

    # Author
    author_info = page.get("created_by", {})
    author_name = author_info.get("name") or "Unknown"

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "created": created_dt,
        "is_public": is_public,
        "author": author_name,
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


# ======================================================
# FULL DEEP RECURSIVE SCAN
# (your original working implementation)
# ======================================================
def get_all_pages(block_id):
    pages = []
    children = get_block_children(block_id)

    for block in children:
        btype = block["type"]

        # -------- child_page --------
        if btype == "child_page":
            pid = block["id"]
            try:
                info = get_page_info(pid)
                pages.append(info)
                pages.extend(get_all_pages(pid))
            except Exception as e:
                print(f"Skip child_page {pid}: {e}")

        # -------- child_database --------
        elif btype == "child_database":
            db_id = block["id"]
            try:
                db_pages = safe_request(notion.databases.query, database_id=db_id)["results"]
                for db_page in db_pages:
                    pid = db_page["id"]
                    try:
                        info = get_page_info(pid)
                        pages.append(info)
                        pages.extend(get_all_pages(pid))
                    except Exception as e:
                        print(f"Skip DB row {pid}: {e}")
            except Exception as e:
                print(f"Skip database {db_id}: {e}")

        # -------- nested blocks --------
        if block.get("has_children") and btype not in ("child_page", "child_database"):
            try:
                pages.extend(get_all_pages(block["id"]))
            except Exception as e:
                print(f"Skip nested block {block['id']}: {e}")

    return pages


# ======================================================
# STORAGE
# ======================================================
def load_known():
    os.makedirs("notion_tracker_data", exist_ok=True)
    if os.path.exists(STORAGE_FILE):
        with open(STORAGE_FILE, "r") as f:
            data = json.load(f)
        print(f"Loaded known pages: {len(data)}")
        return data
    return []


def save_known(pages):
    os.makedirs("notion_tracker_data", exist_ok=True)
    with open(STORAGE_FILE, "w") as f:
        json.dump(pages, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(pages)} pages to cache")


# ======================================================
# SEND SLACK
# ======================================================
def send_slack(message):
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL missing → skip")
        return
    requests.post(SLACK_WEBHOOK_URL, json={"text": message})


# ======================================================
# MAIN LOGIC — NEW ARTICLES OVER 7 DAYS OLD
# ======================================================
def main():
    print("Deep scan of Notion…")
    known = load_known()
    known_ids = {p["id"] for p in known}

    all_pages = get_all_pages(ROOT_PAGE_ID)
    print(f"Found total pages BEFORE filtering: {len(all_pages)}")

    # only pages not seen before
    new_pages = [p for p in all_pages if p["id"] not in known_ids]

    # only public + older than 7 days
    eligible = [
        p for p in new_pages
        if p["is_public"] and p["created"] < ONE_WEEK_AGO
    ]

    if eligible:
        lines = ["🆕 *New public Notion articles (older than 7 days):*\n"]
        for p in eligible:
            lines.append(
                f":blue_book: *{p['title']}*\n"
                f":link: {p['url']}\n"
                f":writing_hand: {p['author']}\n"
            )
        send_slack("\n".join(lines))
    else:
        print("No eligible pages to send.")

    save_known(all_pages)


if __name__ == "__main__":
    main()
