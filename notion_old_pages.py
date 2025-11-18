import os
import csv
import time
from datetime import datetime, timedelta, timezone
from notion_client import Client
from notion_client.errors import APIResponseError

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
    """Retry on 429 and 5xx with backoff."""
    max_retries = 10
    base_delay = 0.35
    backoff = 1

    for attempt in range(max_retries):
        try:
            time.sleep(base_delay)
            return func(*args, **kwargs)
        except APIResponseError as e:
            status = e.status
            if status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limit exceeded. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            if 500 <= status <= 599:
                print(f"[{status}] Notion internal error. Retrying in {backoff}s…")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            raise
    raise RuntimeError("Too many retries.")


def get_block_children(container_id: str):
    """Get all child blocks of any container (page / block)."""
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
    """Extract title, last_edited, url."""
    props = page.get("properties", {})

    title_prop = props.get("title") or props.get("Name")
    if title_prop and title_prop.get("title"):
        title = "".join([t["plain_text"] for t in title_prop["title"]])
    else:
        title = "(untitled)"

    last_edited_raw = page["last_edited_time"].replace("Z", "+00:00")
    last_edited = datetime.fromisoformat(last_edited_raw).astimezone(timezone.utc)

    url = f"https://notion.so/{page['id'].replace('-', '')}"

    return title, last_edited, url


def fetch_all_pages_recursively(container_id: str, collected_pages: list):
    """Find ALL descendants (any depth), including within columns."""
    children = get_block_children(container_id)

    for block in children:
        block_type = block["type"]

        # Child page
        if block_type == "child_page":
            page_id = block["id"]
            page = safe_request(notion.pages.retrieve, page_id)
            collected_pages.append(page)
            fetch_all_pages_recursively(page_id, collected_pages)

        # Any block that can contain children (columns, toggles, synced blocks, etc.)
        if block.get("has_children") and block_type != "child_page":
            fetch_all_pages_recursively(block["id"], collected_pages)


# ------------------------------------
# Main
# ------------------------------------

def main():
    print("🔍 Scanning Notion pages under ROOT_PAGE_ID recursively...")

    all_pages = []

    # Include root page if needed
    try:
        root_page = safe_request(notion.pages.retrieve, ROOT_PAGE_ID)
        all_pages.append(root_page)
    except Exception as e:
        print(f"Warning: could not retrieve ROOT page: {e}")

    # Recursive scanning
    fetch_all_pages_recursively(ROOT_PAGE_ID, all_pages)

    print(f"📄 Total pages found: {len(all_pages)}")

    records = []

    for page in all_pages:
        title, last_edited, url = get_page_info(page)
        is_old = last_edited < ONE_YEAR_AGO

        records.append(
            {
                "title": title,
                "last_edited": last_edited,
                "last_edited_str": last_edited.isoformat(),
                "url": url,
                "is_older_than_1_year": is_old,
            }
        )

        time.sleep(0.05)

    # Sort by oldest first
    records.sort(key=lambda r: r["last_edited"])

    old_pages = [r for r in records if r["is_older_than_1_year"]]

    print("\n================ OLD PAGES (>1 year) ================\n")

    if not old_pages:
        print("🎉 No outdated pages found!")
    else:
        for p in old_pages:
            print(f"• {p['title']}")
            print(f"  Last edited: {p['last_edited_str']}")
            print(f"  URL: {p['url']}\n")

    # ----------------------------
    # CSV with ALL pages
    # ----------------------------
    with open("notion_all_pages.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "last_edited", "url", "is_old"])
        for r in records:
            writer.writerow([
                r["title"],
                r["last_edited_str"],
                r["url"],
                "yes" if r["is_older_than_1_year"] else "no",
            ])

    # ----------------------------
    # CSV with old pages only
    # ----------------------------
    with open("notion_old_pages.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "last_edited", "url"])
        for r in old_pages:
            writer.writerow([
                r["title"],
                r["last_edited_str"],
                r["url"],
            ])

    print("\n📁 CSV saved: notion_all_pages.csv, notion_old_pages.csv")


if __name__ == "__main__":
    main()
