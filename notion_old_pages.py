import os
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


# ------------------------------------
# Utilities
# ------------------------------------
def normalize_id(raw_id: str) -> str:
    """Clean Notion page ID (handles URLs & removes dashes)."""
    raw_id = raw_id.strip()
    if "/" in raw_id:
        raw_id = raw_id.split("/")[-1]
    return raw_id.replace("-", "")

ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)

# Time comparison must be timezone-aware
ONE_YEAR_AGO = datetime.now(timezone.utc) - timedelta(days=365)


def safe_request(func, *args, **kwargs):
    """
    Safe API call:
    - retry only for rate limiting (429)
    - avoids 5xx repeat loops (search() rarely fails)
    """
    max_retries = 5

    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)

        except APIResponseError as e:
            if e.status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limit. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            raise

    raise RuntimeError("Too many retries")


# ------------------------------------
# Fetch ALL pages using notion.search()
# ------------------------------------
def get_all_pages():
    results = []
    next_cursor = None

    while True:
        response = safe_request(
            notion.search,
            query="",
            filter={"property": "object", "value": "page"},
            start_cursor=next_cursor
        )

        results.extend(response.get("results", []))
        next_cursor = response.get("next_cursor")

        if not next_cursor:
            break

    return results


# ------------------------------------
# Main Logic
# ------------------------------------
def main():
    print("🔍 Fetching all pages from Notion via search()...")
    all_pages = get_all_pages()
    print(f"Total pages in workspace: {len(all_pages)}")

    # Filter only pages with direct parent = ROOT_PAGE_ID
    pages_under_root = [
        p for p in all_pages
        if p.get("parent", {}).get("type") == "page_id"
        and normalize_id(p["parent"]["page_id"]) == ROOT_PAGE_ID
    ]

    print(f"Pages under ROOT_PAGE_ID: {len(pages_under_root)}")

    old_pages = []

    for page in pages_under_root:
        # Last edited
        last_edited = datetime.fromisoformat(
            page["last_edited_time"].replace("Z", "+00:00")
        )

        # Title
        props = page.get("properties", {})
        title_prop = props.get("title") or props.get("Name")
        if title_prop and title_prop.get("title"):
            title = "".join([t["plain_text"] for t in title_prop["title"]])
        else:
            title = "(untitled)"

        # Author
        editor = page.get("last_edited_by", {})
        author = editor.get("person", {}).get("email") or editor.get("name") or "Unknown"

        # Compare correctly (aware vs aware)
        if last_edited < ONE_YEAR_AGO:
            old_pages.append({
                "title": title,
                "author": author,
                "last_edited": last_edited.isoformat(),
                "url": f"https://notion.so/{page['id'].replace('-', '')}"
            })

    # Output result
    print("\n=== Pages not edited for >1 year ===\n")

    if not old_pages:
        print("🎉 No outdated pages found!")
        return

    for p in old_pages:
        print(f"• {p['title']}")
        print(f"  Author: {p['author']}")
        print(f"  Last edited: {p['last_edited']}")
        print(f"  URL: {p['url']}\n")

    # OPTIONAL: fail job
    # raise SystemExit("Old pages found — review required.")


if __name__ == "__main__":
    main()
