import os
import re
import time
from notion_client import Client
from notion_client.errors import APIResponseError
from langdetect import detect
import csv

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

notion = Client(auth=NOTION_TOKEN)


# ================================
# BASIC HELPERS
# ================================
def safe_request(func, *args, **kwargs):
    """Retry wrapper for 429 and 5xx"""
    delay = 0.25
    for _ in range(7):
        try:
            time.sleep(delay)
            return func(*args, **kwargs)
        except APIResponseError as e:
            if e.status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] waiting {retry_after}s")
                time.sleep(retry_after)
                continue
            if 500 <= e.status <= 599:
                print(f"[{e.status}] retrying...")
                time.sleep(delay)
                continue
            raise
    raise RuntimeError("Notion failed after retries")


def get_blocks(block_id):
    blocks = []
    cursor = None
    while True:
        resp = safe_request(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor
        )
        blocks.extend(resp["results"])
        cursor = resp.get("next_cursor")
        if not cursor:
            break
    return blocks


# ================================
# TEXT EXTRACTION
# ================================
def extract_text_from_block(block):
    """Extract text exactly like the version that worked correctly."""
    t = ""
    ttype = block.get("type")

    if ttype in block and isinstance(block[ttype], dict):
        rich = block[ttype].get("rich_text", [])
        t += "".join(part.get("plain_text", "") for part in rich)

    return t


def extract_text_recursive(block):
    """Full recursive extraction including columns — WORKING VERSION."""
    text_parts = []

    # own text
    own = extract_text_from_block(block)
    if own.strip():
        text_parts.append(own)

    # children
    if block.get("has_children"):
        children = get_blocks(block["id"])
        for ch in children:
            text_parts.append(extract_text_recursive(ch))

    return " ".join(t for t in text_parts if t).strip()


# ================================
# LANGUAGE DETECTION
# ================================
def detect_lang_safe(text):
    try:
        return detect(text)
    except:
        return "unknown"


def count_words(text):
    return len(re.findall(r'\b\w+\b', text))


# ================================
# PAGE PROCESSING
# ================================
def get_page_title(page_id):
    try:
        page = safe_request(notion.pages.retrieve, page_id=page_id)
        props = page.get("properties", {})
        for prop in props.values():
            if prop.get("type") == "title":
                return "".join(t["plain_text"] for t in prop["title"])
    except:
        pass
    return "(Без названия)"


def process_page(page_id, results):
    """Process one page exactly like the working version."""
    title = get_page_title(page_id)
    blocks = get_blocks(page_id)

    full_text = ""

    for b in blocks:
        full_text += " " + extract_text_recursive(b)

    full_text = full_text.strip()

    # -----------------------------
    # skip pages with no content
    # -----------------------------
    if not full_text:
        print(f"Skip empty page: {title}")
        return

    # -----------------------------
    # calculate lang %
    # -----------------------------
    ru_words = 0
    en_words = 0

    for segment in full_text.split("\n"):
        seg = segment.strip()
        if not seg:
            continue

        lang = detect_lang_safe(seg)
        words = count_words(seg)

        if lang == "ru":
            ru_words += words
        elif lang == "en":
            en_words += words

    total = ru_words + en_words
    ru_pct = round((ru_words / total * 100), 2) if total else 0
    en_pct = round((en_words / total * 100), 2) if total else 0

    results.append({
        "Page Title": title,
        "Page ID": page_id,
        "RU %": ru_pct,
        "EN %": en_pct
    })

    # -----------------------------
    # recurse into subpages
    # -----------------------------
    for b in blocks:
        if b["type"] == "child_page":
            process_page(b["id"], results)

        if b["type"] == "child_database":
            # extract rows
            rows = safe_request(notion.databases.query, database_id=b["id"])
            for row in rows["results"]:
                process_page(row["id"], results)


# ================================
# CSV EXPORT
# ================================
def save_csv(results):
    if not results:
        print("⚠ No pages found.")
        return

    with open("notion_language.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=results[0].keys())
        w.writeheader()
        w.writerows(results)

    print("CSV saved: notion_language.csv")


# ================================
# MAIN
# ================================
if __name__ == "__main__":
    results = []
    process_page(ROOT_PAGE_ID, results)
    save_csv(results)
