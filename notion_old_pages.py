import os
import time
from datetime import datetime, timedelta
from notion_client import Client

# ---- Settings ----
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")  # parent page to scan

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID env vars")

notion = Client(auth=NOTION_TOKEN)

ONE_YEAR_AGO = datetime.utcnow() - timedelta(days=365)

# Normalize input ID (remove dashes, handle URLs)
def normalize_id(raw_id: str) -> str:
    raw_id = raw_id.strip()
    if "/" in raw_id:
        raw_id = raw_id.split("/")[-1]
    return raw_id.replace("-", "")

ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)


# ---- Helpers ----

def get_block_children(page_id):
    """Returns all children blocks of a page."""
    blocks = []
    next_cursor = None

    while True:
        response = notion.blocks.children.list(
            block_id=page_id,
            start_cursor=next_cursor
        )
        blocks.extend(response.get("results", []))

        next_cursor = response.get("next_cursor")
        if not next_cursor:
            break

    return blocks


def get_page_info(page):
    """Extract useful data from a page object."""
    props = page.get("properties", {})

    # Page title
    title_prop = props.get("title") or props.get("Name")
    if title_prop and title_prop.get("title"):
        title = "".join([t["plain_text"] for t in title_prop["title"]])
    else:
        title = "(untitled)"

    # Last edited
    last_edited = datetime.fromisoformat(
        page["last_edited_time"].replace("Z", "+00:00")
    )

    # Author
    editor_info = page.get("last_edited_by", {})
    author = editor_info.get("person", {}).get("email") or \
             editor_info.get("name") or "Unknown"

    return title, author, last_edited


def fetch_all_pages_recursively(page_id, collected):
    """Recursively explore all child pages"""
    children = get_block_children(page_id)

    for block in children:
        if block["type"] == "child_page":
            page = notion.pages.retrieve(block["id"])
            collected.append(page)

            # Recurse
            fetch_all_pages_recursively(block["id"], collected)

        # If it's a synced block or toggle with nested pages
        if block.get("has_children"):
            fetch_all_pages_recursively(block["id"], collected)


# ---- Main Logic ----

def main():
    print("Scanning Notion pages ...")

    all_pages = []
    fetch_all_pages_recursively(ROOT_PAGE_ID, all_pages)

    print(f"Total pages found: {len(all_pages)}")

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

    print("\n=== Pages not edited for >1 year ===")
    if not old_pages:
        print("No old pages found.")
        return

    for p in old_pages:
        print(f"- {p['title']}")
        print(f"  Author: {p['author']}")
        print(f"  Last edited: {p['last_edited']}")
        print(f"  URL: {p['url']}")
        print()

    # OPTIONAL: fail build if needed
    # if old_pages:
    #     raise SystemExit("Some pages have not been updated for >1 year")


if __name__ == "__main__":
    main()
