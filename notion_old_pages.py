import os
import time
from datetime import datetime, timedelta
from notion_client import Client
from notion_client.errors import APIResponseError

# ----------------------------
# Environment
# ----------------------------

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID environment variables")

notion = Client(auth=NOTION_TOKEN)

ONE_YEAR_AGO = datetime.utcnow() - timedelta(days=365)


# ----------------------------
# Utilities
# ----------------------------

def normalize_id(raw_id: str) -> str:
    """Clean Notion page ID (handles URLs & removes dashes)."""
    raw_id = raw_id.strip()
    if "/" in raw_id:
        raw_id = raw_id.split("/")[-1]
    return raw_id.replace("-", "")


ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)


def safe_request(func, *args, **kwargs):
    """
    Safely call the Notion API with:
    - automatic retry on 429 (rate limit)
    - small delay before every request
    - max retry protection
    """
    max_retries = 10
    base_delay = 0.35  # Notion allows ~3 requests/sec

    for attempt in range(max_retries):
        try:
            time.sleep(base_delay)
            return func(*args, **kwargs)

        except APIResponseError as e:
            if e.status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[RATE LIMIT] Waiting {retry_after}s...")
                time.sleep(retry_after)
            else:
                raise

    raise RuntimeError("Too many retry attempts after hitting rate limits.")


# ----------------------------
# Page scanning
# ----------------------------

def get_block_children(page_id):
    """Returns ALL child blocks (with pagination)."""
    blocks = []
    next_cursor = None

    while True:
        response = safe_request(
            notion.blocks.children.list,
            block_id=page_id,
            start_cursor=next_cursor
        )

        blocks.extend(response.get("results", []))
        next_cursor = response.get("next_cursor")

        if not next_cursor:
            break

    return blocks


def get_page_info(page):
    """Extract title, author and last edited time from a page."""
    props = page.get("properties", {})

    # Title
    title_prop = props.get("title") or props.get("Name")
    if title_prop and title_prop.get("title"):
        title = "".join([t["plain_text"] for t in title_prop["title"]])
    else:
        title = "(untitled)"

    # Last edited timestamp
    last_edited = datetime.fromisoformat(
        page["last_edited_time"].replace("Z", "+00:00")
    )

    # Author
    editor_info = page.get("last_edited_by", {})
    author = editor_info.get("person", {}).get("email") \
             or editor_info.get("name") \
             or "Unknown"

    return title, author, last_edited


def fetch_all_pages_recursively(page_id, collected):
    """Recursively explore all child pages."""
    children = get_block_children(page_id)

    for block in children:
        block_type = block["type"]

        # Child pages
        if block_type == "child_page":
            page = safe_request(notion.pages.retrieve, block["id"])
            collected.append(page)

            # Recurse into this page
            fetch_all_pages_recursively(block["id"], collected)

        # Some block types (e.g. toggles, synced blocks) can have nested children
        if block.get("has_children"):
            fetch_all_pages_recursively(block["id"], collected)


# ----------------------------
# Main
# ----------------------------

def main():
    print("🔍 Scanning Notion pages recursively...")

    all_pages = []
    fetch_all_pages_recursively(ROOT_PAGE_ID, all_pages)

    print(f"📄 Total pages found: {len(all_pages)}")

    old_pages = []

    for page in all_pages:
        title, author, last_edited = get_page_info(page)

        if last_edited < ONE_YEAR_AGO:
            old_pages.append({
                "title": title,
                "author": author,
                "last_edited": last_edited.isoformat(),
                "url": f"https://notion.so/{page['id'].replace('-', '')}"
            })

    print("\n================ OLD PAGES (>1 year) ================\n")

    if not old_pages:
        print("🎉 No outdated pages found. Everything is fresh!")
        return

    for p in old_pages:
        print(f"• {p['title']}")
        print(f"  Author: {p['author']}")
        print(f"  Last edited: {p['last_edited']}")
        print(f"  URL: {p['url']}\n")

    # OPTIONAL: fail job if any pages are old
    # raise SystemExit("Old pages found — review required.")


if __name__ == "__main__":
    main()
