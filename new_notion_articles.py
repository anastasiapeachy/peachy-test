from notion_client import Client
from notion_client.errors import APIResponseError
import os
import time
import requests
import json
from datetime import datetime, timezone, timedelta

# ======================================================
# Environment
# ======================================================
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
CACHE_FILE = "notion_tracker_data/known_pages.json"

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN is not set")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID is not set")

notion = Client(auth=NOTION_TOKEN)

DELAY_DAYS = 7
delay_threshold = datetime.now(timezone.utc) - timedelta(days=DELAY_DAYS)


# ======================================================
# SAFE REQUEST
# ======================================================
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
                print(f"[429] Rate limit → wait {retry_after}s…")
                time.sleep(retry_after)
                continue

            if 500 <= status <= 599:
                print(f"[{status}] Server error → retry {backoff}s…")
                time.sleep(backoff)
                backoff = min(backoff * 2, 20)
                continue

            raise

    raise RuntimeError("Notion not responding after retries")


# ======================================================
# Helpers
# ======================================================
def notion_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_page_info(page_id):
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    # title
    title = "Untitled"
    props = page.get("properties", {})
    for prop in props.values():
        if prop["type"] == "title" and prop.get("title"):
            title = prop["title"][0]["plain_text"]
            break

    created = page.get("created_time")
    created_dt = datetime.fromisoformat(created.replace("Z", "+00:00")).astimezone(timezone.utc)

    # public
    is_public = bool(page.get("public_url"))

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "created_time": created_dt.isoformat(),
        "is_public": is_public
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
# FULL RECURSIVE SCAN (your working version)
# ======================================================
def get_all_pages(block_id):
    pages = []
    children = get_block_children(block_id)

    for block in children:
        btype = block["type"]

        # child_page
        if btype == "child_page":
            pid = block["id"]
            try:
                info = get_page_info(pid)
                pages.append(info)
                pages.extend(get_all_pages(pid))
            except Exception as e:
                print(f"Skip page {pid}: {e}")

        # deep nested blocks
        if block.get("has_children"):
            try:
                pages.extend(get_all_pages(block["id"]))
            except Exception:
                pass

    return pages


# ======================================================
# Caching
# ======================================================
def load_cache():
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                print(f"Loaded {len(data)} pages from cache")
                return data
            except Exception:
                return []
    return []


def save_cache(data):
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(data)} pages to cache")


# ======================================================
# Slack
# ======================================================
def send_slack(msg):
    if not SLACK_WEBHOOK_URL:
        print("Slack webhook missing")
        return
    requests.post(SLACK_WEBHOOK_URL, json={"text": msg})


# ======================================================
# MAIN LOGIC: new public pages older than 7 days
# ======================================================
def main():
    print("📡 Deep scan of Notion…")

    cache = load_cache()
    cached_ids = {p["id"] for p in cache}

    pages = get_all_pages(ROOT_PAGE_ID)
    print(f"➡️ Total pages found BEFORE filtering: {len(pages)}")

    # new pages only
    new_pages = [p for p in pages if p["id"] not in cached_ids]

    # filter public + older than 7 days
    eligible = []
    now = datetime.now(timezone.utc)

    for p in new_pages:
        if not p["is_public"]:
            continue
        created_dt = datetime.fromisoformat(p["created_time"])
        if created_dt <= delay_threshold:
            eligible.append(p)

    if not eligible:
        print("No eligible pages to send")
    else:
        lines = ["🆕 *New public Notion pages older than 7 days:*", ""]
        for p in eligible:
            lines.append(f"📘 *{p['title']}*\n🔗 {p['url']}\n")
        send_slack("\n".join(lines))

    save_cache(pages)


if __name__ == "__main__":
    main()
