from notion_client import Client
from notion_client.errors import APIResponseError
import os
import time
import json
import requests
from datetime import datetime, timezone, timedelta

# ------------------------------
# ENV
# ------------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN is missing")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID is missing")

notion = Client(auth=NOTION_TOKEN)
STORAGE_FILE = "notion_tracker_data/known_pages.json"

ONE_WEEK_AGO = datetime.now(timezone.utc) - timedelta(days=7)


# ------------------------------
# SAFE REQUEST
# ------------------------------
def safe_request(func, *args, **kwargs):
    retries = 7
    delay = 0.25
    backoff = 1

    for _ in range(retries):
        try:
            time.sleep(delay)
            return func(*args, **kwargs)

        except APIResponseError as e:
            if e.status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limited → waiting {retry_after}s…")
                time.sleep(retry_after)
                continue

            if 500 <= e.status <= 599:
                print(f"[{e.status}] Server error → retry in {backoff}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 20)
                continue

            raise

    raise RuntimeError("Notion is not responding after retries")


# ------------------------------
# HELPERS 
# ------------------------------
def notion_url(page_id):
    return f"https://www.notion.so/{page_id.replace('-', '')}"


def get_page_info(page_id):
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    # title
    title = "Untitled"
    for prop in page.get("properties", {}).values():
        if prop["type"] == "title" and prop.get("title"):
            title = prop["title"][0]["plain_text"]
            break

    created_raw = page["created_time"]
    created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00")).astimezone(timezone.utc)

    # public?
    is_public = bool(page.get("public_url"))

    author = page.get("created_by", {}).get("name") or "Unknown"

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "created": created_dt,
        "created_iso": created_dt.isoformat(),
        "is_public": is_public,
        "author": author,
    }


# ------------------------------
# FAST & CORRECT RECURSIVE SCAN
# (based on your working version)
# ------------------------------
visited = set()

def get_all_pages(block_id):
    if block_id in visited:
        return []
    visited.add(block_id)

    pages = []
    cursor = None

    while True:
        resp = safe_request(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor
        )

        for block in resp.get("results", []):
            btype = block["type"]

            # child_page → recursive
            if btype == "child_page":
                pid = block["id"]
                try:
                    info = get_page_info(pid)
                    pages.append(info)
                    pages.extend(get_all_pages(pid))
                except Exception as e:
                    print(f"Skip page {pid}: {e}")

            # child_database → query DB then recurse
            elif btype == "child_database":
                db_id = block["id"]
                try:
                    db_entries = safe_request(notion.databases.query, database_id=db_id)
                    for row in db_entries.get("results", []):
                        pid = row["id"]
                        try:
                            info = get_page_info(pid)
                            pages.append(info)
                            pages.extend(get_all_pages(pid))
                        except Exception as e:
                            print(f"Skip db row {pid}: {e}")
                except Exception as e:
                    print(f"Skip database {db_id}: {e}")

            # nested blocks only if has children
            if block.get("has_children") and btype not in ("child_page", "child_database"):
                try:
                    pages.extend(get_all_pages(block["id"]))
                except Exception:
                    pass

        cursor = resp.get("next_cursor")
        if not cursor:
            break

    return pages


# ------------------------------
# STORAGE
# ------------------------------
def load_known():
    if os.path.exists(STORAGE_FILE):
        with open(STORAGE_FILE, "r") as f:
            data = json.load(f)
        print(f"Loaded known pages: {len(data)}")
        return data
    return []


def save_known(data):
    os.makedirs("notion_tracker_data", exist_ok=True)
    simple_data = [{"id": p["id"], "created_iso": p["created_iso"]} for p in data]
    with open(STORAGE_FILE, "w") as f:
        json.dump(simple_data, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(simple_data)} pages to cache")


# ------------------------------
# SLACK
# ------------------------------
def send_slack(message):
    r = requests.post(SLACK_WEBHOOK_URL, json={"text": message})
    print("Slack status:", r.status_code)


# ------------------------------
# MAIN
# ------------------------------
def main():
    known = load_known()
    known_ids = {p["id"] for p in known}

    pages = get_all_pages(ROOT_PAGE_ID)
    print(f"Total discovered pages: {len(pages)}")

    # NEW pages
    new_pages = [p for p in pages if p["id"] not in known_ids]

    # FILTER: public + older than 7 days
    eligible = [
        p for p in new_pages
        if p["is_public"] and p["created"] < ONE_WEEK_AGO
    ]

    if eligible:
        msg = ["🆕 *New public Notion pages (older than 7 days):*\n"]
        for p in eligible:
            msg.append(
                f"📘 *{p['title']}*\n"
                f"🔗 {p['url']}\n"
                f"✍️ {p['author']}\n"
            )
        send_slack("\n".join(msg))
    else:
        print("No eligible pages to send.")

    save_known(pages)


if __name__ == "__main__":
    main()
