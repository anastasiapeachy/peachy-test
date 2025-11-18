import os
import csv
import time
from datetime import datetime, timedelta, timezone
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

NOW = datetime.now(timezone.utc)
ONE_YEAR_AGO = NOW - timedelta(days=365)


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
    Safely call Notion API with:
    - retry on 429 (rate limit)
    - retry on 5xx errors (Notion internal API failures)
    - exponential backoff
    """
    max_retries = 10
    base_delay = 0.35
    backoff = 1

    for attempt in range(max_retries):
        try:
            time.sleep(base_delay)
            return func(*args, **kwargs)

        except APIResponseError as e:
            status = e.status

            # 429 - rate limit
            if status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limit exceeded. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue

            # 5xx - Notion internal failure
            if 500 <= status <= 599:
                print(f"[{status}] Notion API internal error. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)  # exponential backoff, max 30s
                continue

            # Other errors — not recoverable
            raise

    raise RuntimeError("Too many retries — Notion API not responding.")


def get_block_children(container_id: str):
    """
    Returns ALL child blocks (with pagination) for a given container:
    - page
    - block (column_list, column, toggle и т.п.)
    """
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
    """Extract title, author and last edited time from a page."""
    props = page.get("properties", {})

    # Title
    title_prop = props.get("title") or props.get("Name")
    if title_prop and title_prop.get("title"):
        title = "".join([t["plain_text"] for t in title_prop["title"]])
    else:
        title = "(untitled)"

    # Last edited timestamp -> always aware UTC
    last_edited_raw = page["last_edited_time"].replace("Z", "+00:00")
    last_edited = datetime.fromisoformat(last_edited_raw).astimezone(timezone.utc)

    # Author
    editor_info = page.get("last_edited_by", {})
    author = (
        editor_info.get("person", {}).get("email")
        or editor_info.get("name")
        or "Unknown"
    )

    # URL
    page_id_no_dashes = page["id"].replace("-", "")
    url = f"https://notion.so/{page_id_no_dashes}"

    return title, author, last_edited, url


def fetch_all_pages_recursively(container_id: str, collected_pages: list):
    """
    Recursively explore all child pages inside given container:
    - сначала берём все блоки
    - если блок типа child_page -> сохраняем страницу и рекурсивно идём внутрь
    - если у блока есть дети (column_list, column, toggle и т.п.) -> тоже рекурсивно идём внутрь
    Это как раз закрывает кейс, когда child_page лежит в двух столбцах layout'а.
    """
    children = get_block_children(container_id)

    for block in children:
        block_type = block["type"]

        # Если это дочерняя страница
        if block_type == "child_page":
            page_id = block["id"]
            page = safe_request(notion.pages.retrieve, page_id)
            collected_pages.append(page)

            # Рекурсивно обходим подстраницы этой страницы
            fetch_all_pages_recursively(page_id, collected_pages)

        # Любой блок, у которого есть дети (column_list, column, toggle, synced_block, etc.)
        # ВАЖНО: это позволяет добраться до child_page, лежащих внутри колонок
        if block.get("has_children") and block_type != "child_page":
            fetch_all_pages_recursively(block["id"], collected_pages)


# ----------------------------
# Main
# ----------------------------

def main():
    print("🔍 Scanning Notion pages under ROOT_PAGE_ID recursively...")

    all_pages = []

    # Добавим саму ROOT-страницу в список (по желанию можно убрать)
    try:
        root_page = safe_request(notion.pages.retrieve, ROOT_PAGE_ID)
        all_pages.append(root_page)
    except Exception as e:
        print(f"Warning: could not retrieve ROOT page: {e}")

    # Рекурсивно обходим всех потомков
    fetch_all_pages_recursively(ROOT_PAGE_ID, all_pages)

    print(f"📄 Total pages found (including root): {len(all_pages)}")

    records = []
    for page in all_pages:
        title, author, last_edited, url = get_page_info(page)
        is_old = last_edited < ONE_YEAR_AGO

        records.append(
            {
                "title": title,
                "author": author,
                "last_edited": last_edited.isoformat(),
                "url": url,
                "is_older_than_1_year": "yes" if is_old else "no",
            }
        )

        # Лёгкая пауза, чтобы не душить API
        time.sleep(0.05)

    # Фильтруем старые страницы
    old_pages = [r for r in records if r["is_older_than_1_year"] == "yes"]

    print("\n================ OLD PAGES (>1 year) ================\n")
    if not old_pages:
        print("🎉 No outdated pages found. Everything is fresh!")
    else:
        for p in old_pages:
            print(f"• {p['title']}")
            print(f"  Author: {p['author']}")
            print(f"  Last edited: {p['last_edited']}")
            print(f"  URL: {p['url']}\n")

    # ----------------------------
    # CSV: все потомки
    # ----------------------------
    all_csv_path = "notion_all_pages.csv"
    with open(all_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "author", "last_edited", "url", "is_older_than_1_year"])
        for r in records:
            writer.writerow(
                [
                    r["title"],
                    r["author"],
                    r["last_edited"],
                    r["url"],
                    r["is_older_than_1_year"],
                ]
            )

    # ----------------------------
    # CSV: только старые страницы
    # ----------------------------
    old_csv_path = "notion_old_pages.csv"
    with open(old_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "author", "last_edited", "url"])
        for r in old_pages:
            writer.writerow(
                [
                    r["title"],
                    r["author"],
                    r["last_edited"],
                    r["url"],
                ]
            )

    print(f"\n✅ CSV with ALL descendants: {all_csv_path}")
    print(f"✅ CSV with OLD pages:       {old_csv_path}")


if __name__ == "__main__":
    main()
