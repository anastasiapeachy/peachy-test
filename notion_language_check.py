import os
import csv
import time
import re
from datetime import datetime, timezone, timedelta
from notion_client import Client
from notion_client.errors import APIResponseError, HTTPResponseError
from langdetect import detect

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN is not set")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID is not set")

notion = Client(auth=NOTION_TOKEN)

# ============================
#  RATE-LIMIT SAFE REQUEST
# ============================
def safe_request(func, *args, **kwargs):
    max_retries = 7
    delay = 1

    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)

        except APIResponseError as e:
            if e.status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 2))
                print(f"[429] waiting {retry_after}s… (attempt {attempt+1}/{max_retries})")
                time.sleep(retry_after)
                continue

            if 500 <= e.status <= 599:
                print(f"[{e.status}] server error, retry in {delay}s")
                time.sleep(delay)
                delay = min(delay * 2, 10)
                continue

            raise

        except HTTPResponseError:
            print(f"[502] Bad Gateway, retry in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 10)

    raise RuntimeError("API not responding after retries")

# ============================
#   BASIC HELPERS
# ============================
def get_page_title(page):
    props = page.get("properties", {})
    for prop in props.values():
        if prop.get("type") == "title":
            arr = prop.get("title", [])
            return "".join(t.get("plain_text", "") for t in arr) or "(Untitled)"
    return "(Untitled)"

def get_page(page_id):
    return safe_request(notion.pages.retrieve, page_id=page_id)

def get_children(block_id):
    blocks = []
    cursor = None
    while True:
        resp = safe_request(notion.blocks.children.list, block_id=block_id, start_cursor=cursor)
        blocks.extend(resp["results"])
        cursor = resp.get("next_cursor")
        if not cursor:
            break
    return blocks

def extract_text_from_block(block):
    t = block.get("type")
    if not t:
        return ""
    data = block.get(t, {})
    rich = data.get("rich_text", [])
    return "".join(r.get("plain_text", "") for r in rich)

def detect_lang(text):
    try:
        return detect(text)
    except:
        return "unknown"

def count_words(text):
    return len(re.findall(r"\b\w+\b", text))

# =====================================================
#   ANALYZE PAGE TEXT — EXACT VERSION THAT WORKED
# =====================================================
def analyze_page_language(page_id):
    blocks = get_children(page_id)

    ru = 0
    en = 0

    for block in blocks:
        text = extract_text_from_block(block)
        if text.strip():
            lang = detect_lang(text)
            words = count_words(text)
            if lang == "ru":
                ru += words
            elif lang == "en":
                en += words

        # recursive subpages
        if block["type"] == "child_page":
            sub = block["id"]
            sub_ru, sub_en = analyze_page_language(sub)
            ru += sub_ru
            en += sub_en

    return ru, en

# =====================================================
#   "EMPTY PAGE" CHECK
# =====================================================
def is_empty_content(page_id):
    blocks = get_children(page_id)
    if not blocks:
        return True

    for b in blocks:
        txt = extract_text_from_block(b)
        if txt.strip():
            return False

    return True

# =====================================================
#   FULL RECURSION THROUGH NOTION
# =====================================================
def collect_all_pages(root):
    pages = []
    children = get_children(root)

    for block in children:
        t = block["type"]

        # child_page
        if t == "child_page":
            pid = block["id"]
            pages.append(pid)
            pages.extend(collect_all_pages(pid))

        # child_database
        if t == "child_database":
            dbid = block["id"]
            cursor = None
            while True:
                resp = safe_request(notion.databases.query, database_id=dbid, start_cursor=cursor)
                for row in resp["results"]:
                    pid = row["id"]
                    if not is_empty_content(pid):
                        pages.append(pid)
                        pages.extend(collect_all_pages(pid))
                cursor = resp.get("next_cursor")
                if not cursor:
                    break

        # nested blocks
        if block.get("has_children") and t not in ("child_page", "child_database"):
            pages.extend(collect_all_pages(block["id"]))

    return pages

# =====================================================
#   MAIN
# =====================================================
def main():
    print("Collecting all pages…")
    page_ids = list(dict.fromkeys(collect_all_pages(ROOT_PAGE_ID)))
    print(f"Total pages discovered: {len(page_ids)}")

    results = []

    for pid in page_ids:
        if is_empty_content(pid):
            continue

        page = get_page(pid)
        title = get_page_title(page)

        ru, en = analyze_page_language(pid)
        total = ru + en
        ru_p = round(ru / total * 100, 2) if total else 0
        en_p = round(en / total * 100, 2) if total else 0

        results.append([title, pid, ru_p, en_p])

    # save CSV
    with open("notion_language.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["title", "page_id", "ru_percent", "en_percent"])
        w.writerows(results)

    print("CSV saved: notion_language.csv")

if __name__ == "__main__":
    main()
