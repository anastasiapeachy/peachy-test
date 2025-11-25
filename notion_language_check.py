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
# SAFE REQUEST (Rate limits + 5xx)
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

            if status == 429 or code == "rate_limited":
                retry_after = getattr(e, "headers", {}).get("Retry-After")
                if retry_after:
                    try:
                        wait = int(retry_after)
                    except Exception:
                        wait = 2
                else:
                    wait = (attempt + 1) * 2

                print(f"[rate limit] waiting {wait}s… (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                continue

            if status and 500 <= status <= 599:
                print(f"[{status}] server error, retry in {delay}s…")
                time.sleep(delay)
                delay = min(delay * 2, 10)
                continue

            raise

        except HTTPResponseError:
            print(f"[HTTP error] retry in {delay}s…")
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
# DATABASE QUERY (взято из твоего рабочего кода)
# =====================================

def query_db(db_id, cursor=None):
    """Запрос строк БД с пагинацией."""
    body = {"page_size": 100}
    if cursor:
        body["start_cursor"] = cursor

    return safe_request(
        notion.databases.query,
        **{"database_id": db_id, **body}
    )


# =====================================
# EMPTY CONTENT CHECK (взято из твоего рабочего кода)
# =====================================

def is_empty_content(page_id):
    """Проверка, есть ли у страницы хоть один блок текста."""
    try:
        blocks = get_children(page_id)
        return len(blocks) == 0
    except Exception:
        return False


# =====================================
# RECURSIVE BLOCK COLLECTION
# =====================================

def get_blocks_recursive(block_id):
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

    return blocks


# =====================================
# TEXT & LANGUAGE
# =====================================

def extract_rich_text(rt_list):
    pieces = []
    for rt in rt_list:
        if not isinstance(rt, dict):
            continue
        txt = rt.get("plain_text")
        if txt:
            pieces.append(txt)
    return " ".join(pieces)


def block_own_text(block) -> str:
    texts = []
    btype = block.get("type")
    data = block.get(btype, {}) if btype else {}

    rich_containers = [
        "paragraph", "heading_1", "heading_2", "heading_3",
        "heading_4", "heading_5", "heading_6",
        "quote", "callout", "bulleted_list_item",
        "numbered_list_item", "toggle", "to_do"
    ]

    if btype in rich_containers:
        texts.append(extract_rich_text(data.get("rich_text", [])))

    if btype == "code":
        texts.append(extract_rich_text(data.get("rich_text", [])))

    if isinstance(data, dict) and "caption" in data:
        texts.append(extract_rich_text(data.get("caption", [])))

    if btype == "equation":
        expr = data.get("expression")
        if expr:
            texts.append(expr)

    if btype == "table_row":
        for cell in data.get("cells", []):
            texts.append(extract_rich_text(cell))

    return " ".join(t for t in texts if t).strip()


def detect_lang(text: str) -> str:
    try:
        return detect(text)
    except Exception:
        return "unknown"


def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))


# =====================================
# EMPTY PAGE CHECK
# =====================================

def is_empty_page(page_id: str) -> bool:
    try:
        children = get_children(page_id)
        return len(children) == 0
    except Exception:
        return False


# =====================================
# COLLECT ALL PAGES
# =====================================

def collect_all_pages(root_id: str):
    pages = []
    children = get_children(root_id)

    for block in children:
        btype = block.get("type")

        # --- подстраница ---
        if btype == "child_page":
            pid = normalize_id(block["id"])
            pages.append(pid)
            pages.extend(collect_all_pages(pid))

        # --- база данных ---
        elif btype == "child_database":
            db_id = block["id"]
            cursor = None

            while True:
                resp = query_db(db_id, cursor)

                for row in resp["results"]:
                    pid = row["id"]
                    if not is_empty_content(pid):
                        pages.append(pid)
                        pages.extend(collect_all_pages(pid))

                cursor = resp.get("next_cursor")
                if not cursor:
                    break

        # --- другие блоки ---
        if block.get("has_children") and btype not in ("child_page", "child_database"):
            pages.extend(collect_all_pages(block["id"]))

    return pages


# =====================================
# ANALYZE LANGUAGE
# =====================================

def analyze_page_language(page_id: str):
    ru = 0
    en = 0

    blocks = get_blocks_recursive(page_id)
    if not blocks:
        return ru, en, True

    for block in blocks:
        if block.get("type") == "child_page":
            continue

        text = block_own_text(block)
        if not text.strip():
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

    print(f"Total pages discovered under root: {len(page_ids)}")

    results = []
    unreadable_pages = []

    for pid in page_ids:
        if is_empty_page(pid):
            continue

        try:
            page = get_page(pid)
        except Exception as e:
            print(f"Skip page {pid} (cannot retrieve): {e}")
            continue

        title = get_page_title(page)
        url = make_url(pid)

        author_info = page.get("created_by", {}) or {}
        author = author_info.get("name")

        if not author:
            uid = author_info.get("id")
            if uid:
                try:
                    user = safe_request(notion.users.retrieve, user_id=uid)
                    author = user.get("name")
                except Exception:
                    author = None

        if not author:
            author = "(unknown)"

        ru, en, unreadable = analyze_page_language(pid)

        if unreadable:
            print(f"⚠ Cannot reliably read page: {title} — {url}")
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

    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["Page Title", "Page URL", "Author", "% Russian", "% English"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(row)

    print(f"\nSaved {len(results)} rows → {fname}")

    if unreadable_pages:
        print("\n⚠ Pages that API could NOT read:")
        for t, u in unreadable_pages:
            print(f" - {t}: {u}")

    print(f"\nDone in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
