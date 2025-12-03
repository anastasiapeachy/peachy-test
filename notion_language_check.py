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
            time.sleep(0.15)
            return res

        except APIResponseError as e:
            status = getattr(e, "status", None)
            code = getattr(e, "code", None)

            if status == 429 or code == "rate_limited":
                retry_after = getattr(e, "headers", {}).get("Retry-After")
                if retry_after:
                    try:
                        wait = int(retry_after)
                    except:
                        wait = 2
                else:
                    wait = (attempt + 1) * 2

                print(f"[rate limit] waiting {wait}s…")
                time.sleep(wait)
                continue

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
    return f"https://www.notion.so/{page_id.replace('-', '')}"

def get_page(page_id):
    return safe_request(notion.pages.retrieve, page_id=page_id)

def get_page_title(page):
    props = page.get("properties", {})
    for prop in props.values():
        if prop.get("type") == "title":
            return "".join([t.get("plain_text", "") for t in prop.get("title", [])])
    return "(untitled)"

def get_page_title_as_text(page):
    """Fallback: treat title as body text if page has no blocks."""
    return get_page_title(page).strip()

def get_children(block_id):
    blocks = []
    cursor = None
    while True:
        resp = safe_request(notion.blocks.children.list, block_id=block_id, start_cursor=cursor)
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

    return safe_request(notion.databases.query, **{"database_id": db_id, **body})

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
# TEXT EXTRACTION — FIXED & FULL
# =====================================

def extract_rich_text(rt_list):
    parts = []
    for rt in rt_list:
        if isinstance(rt, dict):
            txt = rt.get("plain_text")
            if txt:
                parts.append(txt)
    return " ".join(parts)

def extract_text_fallback(block):
    """Full fallback extractor for Notion weird cases."""

    btype = block.get("type")
    data = block.get(btype, {}) if btype else {}

    # most common — paragraphs
    if "rich_text" in data:
        txt = extract_rich_text(data["rich_text"])
        if txt.strip():
            return txt

    # caption fallback
    if "caption" in data:
        txt = extract_rich_text(data["caption"])
        if txt.strip():
            return txt

    # table rows
    if btype == "table_row":
        texts = []
        for cell in data.get("cells", []):
            texts.append(extract_rich_text(cell))
        combined = " ".join(texts).strip()
        if combined:
            return combined

    # final fallback: blocks sometimes contain "text" or "plain_text"
    if isinstance(data, dict):
        for key in data.keys():
            if isinstance(data[key], str) and data[key].strip():
                return data[key]

    return ""

def block_own_text(block):
    """Unified text extractor with fallback."""
    text = extract_text_fallback(block)
    return text.strip()

# =====================================
# CHECK IF PAGE HAS REAL TEXT
# =====================================

def page_has_real_text(page_id):
    """Return True if page contains real human text."""
    blocks = get_blocks_recursive(page_id)

    for block in blocks:
        txt = block_own_text(block)
        if txt.strip():
            return True

    # fallback to title-as-body
    page = get_page(page_id)
    title_txt = get_page_title_as_text(page)
    return bool(title_txt.strip())

# =====================================
# PAGE VISIT CACHE
# =====================================

VISITED_PAGES = set()

def collect_all_pages(root_id):
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
    except:
        return "unknown"

def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))

def analyze_page_language(page_id):
    ru = 0
    en = 0

    blocks = get_blocks_recursive(page_id)

    for block in blocks:
        text = block_own_text(block)
        if not text.strip():
            continue

        words = count_words(text)
        if words == 0:
            continue

        lang = detect_lang(text)

        if lang == "ru":
            ru += words
        elif lang == "en":
            en += words

    if ru + en == 0:
        return 0, 0, True

    return ru, en, False

# =====================================
# MAIN
# =====================================

def main():
    start = time.time()
    print("Collecting pages...")

    page_ids = list(dict.fromkeys(collect_all_pages(ROOT_PAGE_ID)))
    print(f"Total pages: {len(page_ids)}")

    results = []
    unreadable = []

    for pid in page_ids:
        if not page_has_real_text(pid):
            continue

        page = get_page(pid)
        title = get_page_title(page)
        url = make_url(pid)
        author = page.get("created_by", {}).get("name", "(unknown)")

        ru, en, unread = analyze_page_language(pid)

        if unread:
            unreadable.append((title, url))

        total = ru + en
        ru_pct = ru * 100 / total if total else 0
        en_pct = en * 100 / total if total else 0

        results.append({
            "Page Title": title,
            "Page URL": url,
            "Author": author,
            "% Russian": round(ru_pct, 2),
            "% English": round(en_pct, 2)
        })

    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    with open("notion_language_percentages.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["Page Title", "Page URL", "Author", "% Russian", "% English"])
        writer.writeheader()
        writer.writerows(results)

    print(f"Saved {len(results)} results → notion_language_percentages.csv")

    print(f"Done in {time.time() - start:.1f}s")

if __name__ == "__main__":
    main()
