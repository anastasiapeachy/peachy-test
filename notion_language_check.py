import os
import csv
import re
import time
from datetime import datetime, timezone, timedelta
from notion_client import Client
from notion_client.errors import APIResponseError, HTTPResponseError
from langdetect import detect

# =====================================
# ENV
# =====================================

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN is not set")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID is not set")

notion = Client(auth=NOTION_TOKEN)

# =====================================
# SAFE REQUEST
# =====================================

def safe_request(func, *args, **kwargs):
    max_retries = 7
    delay = 1

    for attempt in range(max_retries):
        try:
            res = func(*args, **kwargs)
            time.sleep(0.2)
            return res

        except APIResponseError as e:
            status = getattr(e, "status", None)
            code = getattr(e, "code", None)

            # Rate limit
            if status == 429 or code == "rate_limited":
                retry_after = getattr(e, "headers", {}).get("Retry-After")
                if retry_after:
                    try:
                        wait = int(retry_after)
                    except Exception:
                        wait = 2
                else:
                    wait = (attempt + 1) * 2
                print(f"[rate limit] waiting {wait}s…")
                time.sleep(wait)
                continue

            # 5xx server error
            if status and 500 <= status <= 599:
                print(f"[{status}] server error, retrying…")
                time.sleep(delay)
                delay = min(delay * 2, 10)
                continue

            raise

        except HTTPResponseError:
            print(f"[HTTP error] retrying…")
            time.sleep(delay)
            delay = min(delay * 2, 10)

    raise RuntimeError("Notion API not responding after retries")


# =====================================
# HELPERS
# =====================================

def normalize_id(raw_id: str) -> str:
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip().replace("-", "")
    m = re.search(r"([0-9a-fA-F]{32})", s)
    return m.group(1) if m else s

ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)

def make_url(page_id: str) -> str:
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"

def get_page(page_id):
    return safe_request(notion.pages.retrieve, page_id=page_id)

def get_page_title(page) -> str:
    props = page.get("properties", {}) or {}
    for prop in props.values():
        if prop.get("type") == "title":
            parts = [t.get("plain_text", "") for t in prop.get("title", [])]
            if parts:
                return "".join(parts)
    return "(untitled)"

def get_children(block_id):
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
    return blocks

# =====================================
# DATABASE QUERY
# =====================================

def query_db(db_id, cursor=None):
    body = {"page_size": 100}
    if cursor:
        body["start_cursor"] = cursor

    return safe_request(
        notion.databases.query,
        **{"database_id": db_id, **body}
    )

# =====================================
# BLOCK CACHE
# =====================================

BLOCK_CACHE = {}

def get_blocks_recursive(block_id):
    if block_id in BLOCK_CACHE:
        return BLOCK_CACHE[block_id]

    blocks = []
    cursor = None

    while True:
        resp = safe_request(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor
        )

        for block in resp.get("results", []):
            blocks.append(block)
            if block.get("has_children"):
                blocks.extend(get_blocks_recursive(block["id"]))

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    BLOCK_CACHE[block_id] = blocks
    return blocks

# =====================================
# TEXT EXTRACTION FIXED
# =====================================

def extract_rich_text(rt_list):
    pieces = []
    for rt in rt_list:
        if isinstance(rt, dict):
            txt = rt.get("plain_text")
            if txt:
                pieces.append(txt)
    return " ".join(pieces)

def block_own_text(block) -> str:
    """Extract text from any text-containing block type."""
    btype = block.get("type")
    data = block.get(btype, {}) if btype else {}

    # main text source
    if "rich_text" in data:
        return extract_rich_text(data.get("rich_text", []))

    # captions
    if "caption" in data:
        return extract_rich_text(data.get("caption", []))

    # table cells
    if btype == "table_row":
        parts = []
        for cell in data.get("cells", []):
            parts.append(extract_rich_text(cell))
        return " ".join(parts)

    return ""

# =====================================
# PAGE TEXT PRESENCE CHECK
# =====================================

def page_has_real_text(page_id: str) -> bool:
    """Return True only if page contains real readable text."""
    blocks = get_blocks_recursive(page_id)
    for block in blocks:
        text = block_own_text(block).strip()
        if text:
            return True
    return False

# =====================================
# PAGE VISIT CACHE
# =====================================

VISITED_PAGES = set()

def collect_all_pages(root_id: str):
    if root_id in VISITED_PAGES:
        return []
    VISITED_PAGES.add(root_id)

    pages = []
    children = get_children(root_id)

    for block in children:
        btype = block.get("type")

        if btype == "child_page":
            pid = normalize_id(block["id"])
            pages.append(pid)
            pages.extend(collect_all_pages(pid))

        elif btype == "child_database":
            db_id = block["id"]
            cursor = None

            while True:
                resp = query_db(db_id, cursor)
                for row in resp["results"]:
                    pid = row["id"]
                    pages.append(pid)
                    pages.extend(collect_all_pages(pid))

                cursor = resp.get("next_cursor")
                if not cursor:
                    break

        if block.get("has_children") and btype not in ("child_page", "child_database"):
            pages.extend(collect_all_pages(block["id"]))

    return pages

# =====================================
# LANGUAGE ANALYSIS
# =====================================

def detect_lang(text: str) -> str:
    try:
        return detect(text)
    except Exception:
        return "unknown"

def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))

def analyze_page_language(page_id: str):
    ru = 0
    en = 0

    blocks = get_blocks_recursive(page_id)
    if not blocks:
        return ru, en, True

    for block in blocks:
        text = block_own_text(block).strip()
        if not text:
            continue

        lang = detect_lang(text)
        words = count_words(text)

        if lang == "ru":
            ru += words
        elif lang == "en":
            en += words

    unreadable = (ru + en == 0)
    return ru, en, unreadable

# =====================================
# MAIN
# =====================================

def main():
    start = time.time()
    print("Collecting all pages under ROOT_PAGE_ID…")

    page_ids = collect_all_pages(ROOT_PAGE_ID)
    page_ids = list(dict.fromkeys(page_ids))

    print(f"Total pages discovered: {len(page_ids)}")

    results = []
    unreadable_pages = []

    for pid in page_ids:

        # 1) Skip pages fully empty
        if not page_has_real_text(pid):
            continue

        try:
            page = get_page(pid)
        except Exception as e:
            print(f"Skip page {pid}: {e}")
            continue

        title = get_page_title(page)
        url = make_url(pid)

        # author
        author_info = page.get("created_by", {}) or {}
        author = author_info.get("name", "(unknown)")

        ru, en, unreadable = analyze_page_language(pid)

        if unreadable:
            unreadable_pages.append((title, url))

        total = ru + en
        ru_pct = ru * 100 / total if total else 0
        en_pct = en * 100 / total if total else 0

        results.append({
            "Page Title": title,
            "Page URL": url,
            "Author": author,
            "% Russian": round(ru_pct, 2),
            "% English": round(en_pct, 2),
        })

    # sort
    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    # csv
    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Page Title", "Page URL", "Author", "% Russian", "% English"]
        )
        writer.writeheader()
        for row in results:
            writer.writerow(row)

    print(f"\nSaved {len(results)} rows → {fname}")

    if unreadable_pages:
        print("\nUnreadable pages:")
        for t, u in unreadable_pages:
            print(f"- {t}: {u}")

    print(f"\nDone in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
