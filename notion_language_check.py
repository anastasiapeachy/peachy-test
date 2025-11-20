import os
import re
import csv
import time
import math
import argparse
from notion_client import Client
from langdetect import detect
from datetime import datetime

# ======================
# ARGUMENTS
# ======================
parser = argparse.ArgumentParser()
parser.add_argument("--batch", type=int, default=None)
parser.add_argument("--batch-size", type=int, default=20)
args = parser.parse_args()

BATCH_INDEX = args.batch
BATCH_SIZE = args.batch_size

# ======================
# ENV VARS
# ======================
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID")

notion = Client(auth=NOTION_TOKEN)


# ======================
# HELPERS
# ======================
def normalize_id(x):
    if not isinstance(x, str):
        return x
    s = x.replace("-", "")
    if re.fullmatch(r"[0-9a-fA-F]{32}", s):
        return s
    m = re.search(r"([0-9a-fA-F]{32})", s)
    return m.group(1) if m else s


ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)


def notion_url(pid):
    return f"https://www.notion.so/{pid.replace('-', '')}"


def count_words(text):
    return len(re.findall(r"\b\w+\b", text))


def detect_lang_safe(text):
    try:
        return detect(text)
    except:
        return "unknown"


# ======================
# FETCH CHILD PAGES
# strict page → page → root chain
# ======================
def get_child_pages_recursive(page_id):
    pages = []
    cursor = None

    while True:
        resp = notion.blocks.children.list(page_id, start_cursor=cursor)
        for block in resp.get("results", []):
            if block["type"] == "child_page":
                pid = normalize_id(block["id"])
                pages.append(pid)
                pages.extend(get_child_pages_recursive(pid))
            if block.get("has_children"):
                pages.extend(get_child_pages_recursive(block["id"]))

        cursor = resp.get("next_cursor")
        if not cursor:
            break
    return pages


# ======================
# RECURSIVE TEXT EXTRACTION
# this is the FIXED version that correctly handles columns
# ======================
def get_text_from_block(block):

    t = []
    typ = block["type"]
    data = block.get(typ, {})

    # ---- FIX: columns ----
    if typ in ("column_list", "column"):
        cursor = None
        while True:
            resp = notion.blocks.children.list(block["id"], start_cursor=cursor)
            for ch in resp["results"]:
                t.append(get_text_from_block(ch))
            cursor = resp.get("next_cursor")
            if not cursor:
                break
        return " ".join(x for x in t if x)

    # ---- rich_text ----
    if isinstance(data, dict) and "rich_text" in data:
        r = " ".join(rt.get("plain_text", "") for rt in data["rich_text"])
        if r:
            t.append(r)

    # ---- caption ----
    if isinstance(data, dict) and "caption" in data:
        cap = " ".join(c.get("plain_text", "") for c in data["caption"])
        if cap:
            t.append(cap)

    # ---- synced_block ----
    if typ == "synced_block":
        sf = data.get("synced_from")
        if sf:
            original = sf.get("block_id")
            if original:
                resp = notion.blocks.children.list(original)
                for ch in resp["results"]:
                    t.append(get_text_from_block(ch))

    # ---- table ----
    if typ == "table":
        resp = notion.blocks.children.list(block["id"])
        for row in resp["results"]:
            if row["type"] == "table_row":
                for cell in row["table_row"]["cells"]:
                    part = " ".join(x.get("plain_text", "") for x in cell)
                    if part:
                        t.append(part)

    # ---- recurse children ----
    if block.get("has_children"):
        cursor = None
        while True:
            resp = notion.blocks.children.list(block["id"], start_cursor=cursor)
            for ch in resp["results"]:
                t.append(get_text_from_block(ch))
            cursor = resp.get("next_cursor")
            if not cursor:
                break

    return " ".join(x for x in t if x)


# ======================
# PAGE LANGUAGE ANALYSIS
# ======================
def analyze_page(page_id):

    ru_words = 0
    en_words = 0

    blocks = []
    cursor = None
    while True:
        resp = notion.blocks.children.list(page_id, start_cursor=cursor)
        blocks.extend(resp["results"])
        cursor = resp.get("next_cursor")
        if not cursor:
            break

    for block in blocks:
        if block["type"] == "child_page":
            continue
        text = get_text_from_block(block)
        if not text.strip():
            continue

        lang = detect_lang_safe(text)
        words = count_words(text)

        if lang == "ru":
            ru_words += words
        elif lang == "en":
            en_words += words

    return ru_words, en_words


# ======================
# MAIN (BATCH EXECUTION)
# ======================
def main():
    print("Collecting child pages…")
    all_pages = list(dict.fromkeys(get_child_pages_recursive(ROOT_PAGE_ID)))
    total_pages = len(all_pages)

    print(f"Total pages under root: {total_pages}")

    # batching
    if BATCH_INDEX is None:
        print("ERROR: batch index is required")
        return

    start = BATCH_INDEX * BATCH_SIZE
    end = min(start + BATCH_SIZE, total_pages)
    batch_pages = all_pages[start:end]

    print(f"Batch {BATCH_INDEX}: {len(batch_pages)} pages")

    results = []

    for pid in batch_pages:
        try:
            page = notion.pages.retrieve(pid)
            title = page["properties"]["title"]["title"][0]["plain_text"] \
                if "title" in page["properties"] else "(untitled)"
        except:
            title = "(unknown)"

        ru, en = analyze_page(pid)
        total = ru + en

        ru_pct = round(ru / total * 100, 2) if total else 0
        en_pct = round(en / total * 100, 2) if total else 0

        results.append({
            "Page": title,
            "URL": notion_url(pid),
            "%RU": ru_pct,
            "%EN": en_pct
        })

    # save batch CSV
    fname = f"batch_{BATCH_INDEX}.csv"
    with open(fname, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["Page", "URL", "%RU", "%EN"])
        w.writeheader()
        w.writerows(results)

    print(f"Saved {fname}")


if __name__ == "__main__":
    main()
