import os
import csv
import re
import time
import math
from notion_client import Client
from langdetect import detect

# ======================================================================
# ENV
# ======================================================================

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
BATCH_INDEX = int(os.getenv("BATCH_INDEX", "0"))  # номер батча
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))   # размер батча

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID")

notion = Client(auth=NOTION_TOKEN)


# ======================================================================
# NORMALIZE ID
# ======================================================================

def normalize_id(raw_id):
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip().replace("-", "")
    return s.lower()


ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)


# ======================================================================
# FIXED BLOCK FETCH WITH RETRY
# ======================================================================

def safe_blocks(block_id):
    """Fetch blocks with retry (429-proof)"""
    blocks = []
    cursor = None

    while True:
        for attempt in range(5):
            try:
                resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)
                break
            except Exception:
                time.sleep(0.4)
        else:
            return blocks

        blocks.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return blocks


# ======================================================================
# RECURSIVE TEXT EXTRACTOR (полностью рабочий)
# ======================================================================

def extract_all_text_from_block(block):
    out = []
    t = block["type"]
    data = block.get(t, {})

    # column + column_list
    if t in ("column", "column_list"):
        for ch in safe_blocks(block["id"]):
            out.append(extract_all_text_from_block(ch))
        return " ".join(out)

    # rich_text
    if "rich_text" in data:
        out.append(" ".join(x.get("plain_text", "") for x in data["rich_text"]))

    # common types
    if t in [
        "paragraph", "heading_1", "heading_2", "heading_3",
        "quote", "callout", "bulleted_list_item",
        "numbered_list_item", "toggle"
    ]:
        rt = data.get("rich_text", [])
        out.append(" ".join(x.get("plain_text", "") for x in rt))

    # caption
    if "caption" in data:
        out.append(" ".join(x.get("plain_text", "") for x in data["caption"]))

    # table
    if t == "table":
        for row in safe_blocks(block["id"]):
            if row["type"] == "table_row":
                for cell in row["table_row"]["cells"]:
                    out.append(" ".join(x.get("plain_text", "") for x in cell))

    # synced block
    if t == "synced_block":
        sf = data.get("synced_from")
        if sf:
            origin = sf.get("block_id")
            if origin:
                for ch in safe_blocks(origin):
                    out.append(extract_all_text_from_block(ch))

    # children
    if block.get("has_children"):
        for ch in safe_blocks(block["id"]):
            out.append(extract_all_text_from_block(ch))

    return " ".join(x for x in out if x).strip()


# ======================================================================
# LANG DETECTION
# ======================================================================

def detect_lang(text):
    try:
        return detect(text)
    except:
        return "unknown"


def count_words(text):
    return len(re.findall(r"\b\w+\b", text))


# ======================================================================
# PARENT RESOLUTION (strict)
# ======================================================================

def resolve_parent_page(block_or_page):
    parent = block_or_page["parent"]
    tp = parent["type"]

    if tp == "page_id":
        return normalize_id(parent["page_id"])

    if tp == "block_id":
        bid = parent["block_id"]
        for _ in range(5):
            try:
                blk = notion.blocks.retrieve(block_id=bid)
                return resolve_parent_page(blk)
            except:
                time.sleep(0.3)
        return None

    return None


# strict chain: page->page->...->root
def is_child_of_root(page_id):
    visited = set()
    cur = page_id

    while True:
        if cur in visited:
            return False
        visited.add(cur)

        try:
            p = notion.pages.retrieve(page_id=cur)
        except:
            return False

        parent_page = resolve_parent_page(p)
        if not parent_page:
            return False

        if parent_page == ROOT_PAGE_ID:
            return True

        cur = parent_page


# ======================================================================
# ANALYZE PAGE
# ======================================================================

def analyze_page(pid):
    ru = 0
    en = 0

    for blk in safe_blocks(pid):
        if blk["type"] == "child_page":
            continue

        txt = extract_all_text_from_block(blk)
        if not txt:
            continue

        lang = detect_lang(txt)
        words = count_words(txt)

        if lang == "ru":
            ru += words
        elif lang == "en":
            en += words

    return ru, en


# ======================================================================
# MAIN (batch processing)
# ======================================================================

def main():
    print("Fetching ALL pages in workspace (once)...")
    all_pages = notion.search(
        query="",
        filter={"value": "page", "property": "object"}
    )["results"]

    # filter only strict children of root
    selected = []
    for p in all_pages:
        pid = normalize_id(p["id"])
        if pid == ROOT_PAGE_ID:
            selected.append(pid)
            continue

        if is_child_of_root(pid):
            selected.append(pid)

    print(f"Total pages under root: {len(selected)}")

    # batching
    total_batches = math.ceil(len(selected) / BATCH_SIZE)
    start_idx = BATCH_INDEX * BATCH_SIZE
    end_idx = start_idx + BATCH_SIZE
    batch = selected[start_idx:end_idx]

    print(f"Processing batch {BATCH_INDEX+1}/{total_batches}, size={len(batch)}")

    rows = []

    for pid in batch:
        for _ in range(5):
            try:
                page = notion.pages.retrieve(page_id=pid)
                break
            except:
                time.sleep(0.3)

        title = "(untitled)"
        for prop in page.get("properties", {}).values():
            if prop["type"] == "title" and prop.get("title"):
                title = prop["title"][0]["plain_text"]
                break

        url = f"https://www.notion.so/{pid}"

        ru, en = analyze_page(pid)
        total = ru + en
        ru_pct = round(ru / total * 100, 2) if total else 0
        en_pct = round(en / total * 100, 2) if total else 0

        rows.append([title, url, ru_pct, en_pct])

    # save batch file
    fname = f"batch_{BATCH_INDEX}.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Page Title", "Page URL", "% Russian", "% English"])
        w.writerows(rows)

    print(f"Saved {len(rows)} rows → {fname}")


if __name__ == "__main__":
    main()
