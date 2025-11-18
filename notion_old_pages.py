import os
import csv
import time
from datetime import datetime, timedelta, timezone
from notion_client import Client
from notion_client.errors import APIResponseError, HTTPResponseError

# ------------------------------------
# Environment
# ------------------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID environment variables")

notion = Client(auth=NOTION_TOKEN)

NOW = datetime.now(timezone.utc)
ONE_YEAR_AGO = NOW - timedelta(days=365)


# ------------------------------------
# Utilities
# ------------------------------------
def normalize_id(raw_id: str) -> str:
    raw_id = raw_id.strip()
    if "/" in raw_id:
        raw_id = raw_id.split("/")[-1]
    return raw_id.replace("-", "")

ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)


def safe_request(func, *args, **kwargs):
    """
    Ultra-safe wrapper:
    - retries on 429
    - retries on ALL 5xx including 502, 503, 504
    - retries on network timeouts
    - exponential backoff (1 → 2 → 4 → ... → 60 sec)
    """
    max_retries = 15
    backoff = 1  # seconds

    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)

        except (APIResponseError, HTTPResponseError) as e:
            status = getattr(e, "status", None)

            # 429 Too Many Requests
            if status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limit — waiting {retry_after}s...")
                time.sleep(retry_after)
                continue

            # Any 5xx (500–599), including 502/503/504
            if status and 500 <= status <= 599:
                print(f"[{status}] Server error — retry {attempt+1}/{max_retries} in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)  # exponential backoff max 60s
                continue

            # No status — connection issue, weird API timeout
            print(f"[NETWORK] Error '{e}' — retrying in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue

        except Exception as e:
            print(f"[UNKNOWN ERROR] {e} — retrying in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue

    raise RuntimeError("Too many retries — Notion API still failing.")


def get_block_children(container_id: str):
    """Retrieve all child blocks (columns, toggles, synced blocks, etc.)."""
    blocks = []
    next_cursor = None

    while True:
        response = safe_request(
            notion.blocks.children.list,
            block_id=container_id,
            start_cursor=next_cursor
        )
        blocks.extend(response.get("results", []))
        next_cursor = response.get("next_cursor")
        if not next_cursor:
            break

    return blocks


def get_page_info(page):
    """Extract page title, last edited timestamp, URL."""
    props = page.get("properties", {})

    title_prop = props.get("title") or props.get("Name")
    if title_prop and title_prop.get("title"):
        title = "".join(t["plain_text"] for t in title_prop["title"])
    else:
        title = "(untitled)"

    last_edited_raw = page["last_edited_time"].replace("Z", "+00:00")
    last_edited = datetime.fromisoformat(last_edited_raw).astimezone(timezone.utc)

    url = f"https://notion.so/{page['id'].replace('-', '')}"

    return title, last_edited, url


def fetch_all_pages_recursively(container_id: str, collected_pages: list):
    """Recursively scan ALL child pages, including those inside columns."""
    children = get_block_children(container_id)

    for block in children:
        block_type = block["type"]

        # Direct child page
        if block_type == "child_page":
            page_id = block["id"]
            page = safe_request(notion.pages.retrieve, page_id)
            collected_pages.append(page)
            fetch_all_pages_recursively(page_id, collected_pages)

        # Any container block with nested content
        if block.get("has_children") and block_type != "child_page":
            fetch_all_pages_recursively(block["id"], collected_pages)


# ------------------------------------
# Main
# ------------------------------------
def main():
    print("🔍 Scanning Notion pages under ROOT recursively...")

    all_pages = []

    # Try getting root page
    try:
        root_page = safe_request(notion.pages.retrieve, ROOT_PAGE_ID)
        all_pages.append(root_page)
    except Exception as e:
        print(f"Warning: cannot retrieve ROOT page: {e}")

    fetch_all_pages_recursively(ROOT_PAGE_ID, all_pages)

    print(f"📄 Total pages found (including root): {len(all_pages)}")

    records = []

    for page in all_pages:
        title, last_edited, url = get_page_info(page)
        is_old = last_edited < ONE_YEAR_AGO

        records.append({
            "title": title,
            "last_edited": last_edited,
            "last_edited_str": last_edited.isoformat(),
            "url": url,
            "is_old": "yes" if is_old else "no",
        })

        time.sleep(0.05)

    # Sort by last edited (oldest first)
    records.sort(key=lambda r: r["last_edited"])

    old_pages = [r for r in records if r["is_old"] == "yes"]

    # --------------------------------------------
    # CSV: all pages
    # --------------------------------------------
    with open("notion_all_pages.csv", "w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f)
        wr.writerow(["title", "last_edited", "url", "is_old"])
        for r in records:
            wr.writerow([r["title"], r["last_edited_str"], r["url"], r["is_old"]])

    # --------------------------------------------
    # CSV: old pages only
    # --------------------------------------------
    with open("notion_old_pages.csv", "w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f)
        wr.writerow(["title", "last_edited", "url"])
        for r in old_pages:
            wr.writerow([r["title"], r["last_edited_str"], r["url"]])

    print("\n📁 CSV saved: notion_all_pages.csv, notion_old_pages.csv")


if __name__ == "__main__":
    main()
