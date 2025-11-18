import os
import csv
from datetime import datetime, timedelta, timezone
from notion_client import Client
from notion_client.errors import APIResponseError

# ------------------------------------
# Environment
# ------------------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
CSV_PATH = os.getenv("CSV_PATH", "notion_old_pages_report.csv")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID")

notion = Client(auth=NOTION_TOKEN)


# ------------------------------------
# Utilities
# ------------------------------------
def normalize_id(raw_id: str) -> str:
    raw_id = raw_id.strip()
    if "/" in raw_id:
        raw_id = raw_id.split("/")[-1]
    return raw_id.replace("-", "")

ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)

ONE_YEAR_AGO = datetime.now(timezone.utc) - timedelta(days=365)


def safe_request(func, *args, **kwargs):
    """Retry only for rate limiting (429)."""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except APIResponseError as e:
            if e.status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limit. Waiting {retry_after}s…")
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
# Build tree of descendants
# ------------------------------------
def find_all_descendants(all_pages, root_id):
    """Returns set of all page IDs that descend from root page (any depth)."""

    # Build a mapping page_id → parent_page_id
    parent_map = {}
    for p in all_pages:
        parent = p.get("parent", {})
        if parent.get("type") == "page_id":
            parent_map[p["id"].replace("-", "")] = parent["page_id"].replace("-", "")

    # BFS/DFS to find all descendants
    descendants = set()
    stack = [root_id]

    while stack:
        current = stack.pop()
        for page_id, parent_id in parent_map.items():
            if parent_id == current and page_id not in descendants:
                descendants.add(page_id)
                stack.append(page_id)

    return descendants


# ------------------------------------
# Main Logic
# ------------------------------------
def main():
    print("🔍 Fetching all pages via search()…")
    all_pages = get_all_pages()
    print(f"📄 Total pages found: {len(all_pages)}")

    # Build descendant tree
    print("🔧 Building descendant tree…")
    descendants = find_all_descendants(all_pages, ROOT_PAGE_ID)
    print(f"📚 Pages under ROOT_PAGE_ID (all levels): {len(descendants)}")

    # Convert list of page objects to dict by id for fast lookup
    page_index = {p["id"].replace("-", ""): p for p in all_pages}

    old_pages = []

    for page_id in descendants:
        page = page_index.get(page_id)
        if not page:
            continue

        # Parse last edited (timezone-aware)
        last_edited = datetime.fromisoformat(
            page["last_edited_time"].replace("Z", "+00:00")
        )

        # Title
        props = page.get("properties", {})
        title_prop = props.get("title") or props.get("Name")
        if title_prop and title_prop.get("title"):
            title = "".join(t["plain_text"] for t in title_prop["title"])
        else:
            title = "(untitled)"

        # Author
        editor = page.get("last_edited_by", {})
        author = editor.get("person", {}).get("email") or editor.get("name") or "Unknown"

        # Filter old pages
        if last_edited < ONE_YEAR_AGO:
            old_pages.append({
                "title": title,
                "author": author,
                "last_edited": last_edited.isoformat(),
                "url": f"https://notion.so/{page['id'].replace('-', '')}"
            })

    # -----------------------------------
    # Save CSV report
    # -----------------------------------
    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "author", "last_edited", "url"])
        for p in old_pages:
            writer.writerow([p["title"], p["author"], p["last_edited"], p["url"]])

    print(f"\n📁 CSV report saved to: {CSV_PATH}")

    # Console output
    if not old_pages:
        print("🎉 No outdated pages found!")
    else:
        print("\n=== Outdated pages (all depths) ===\n")
        for p in old_pages:
            print(f"• {p['title']}")
            print(f"  Author: {p['author']}")
            print(f"  Last edited: {p['last_edited']}")
            print(f"  URL: {p['url']}\n")


if __name__ == "__main__":
    main()
