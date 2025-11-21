import os
import csv
import re
import time
import json
import argparse
from notion_client import Client
from langdetect import detect

# ===========================================================
# ARGPARSE
# ===========================================================

parser = argparse.ArgumentParser()
parser.add_argument("--list-pages", action="store_true")
parser.add_argument("--batch", default=None)
args = parser.parse_args()

# ===========================================================
# ENV
# ===========================================================

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID env vars")

# ===========================================================
# Utils
# ===========================================================

def normalize_id(raw_id):
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip()
    match = re.search(r"([0-9a-fA-F]{32})", s.replace("-", ""))
    if match:
        return match.group(1)
    return s.replace("-", "")

ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)
notion = Client(auth=NOTION_TOKEN)

# ===========================================================
# Fetch ALL pages from workspace
# ===========================================================

def get_all_pages():
    pages = []
    cursor = None
    while True:
        try:
            resp = notion.search(
                query="",
                filter={"value": "page", "property": "object"},
                start_cursor=cursor
            )
        except Exception as e:
            print(f"Search failed: {e}")
            break

        pages.extend(resp.get("results", []))

        if not resp.get("has_more"):
            break

        cursor = resp.get("next_cursor")

    return pages

# ===========================================================
# Title
# ===========================================================

def get_title(page):
    props = page.get("properties", {}) or {}
    for prop in props.values():
        if prop.get("type") == "title":
            parts = [t.get("plain_text", "") for t in prop.get("title", [])]
            if parts:
                return "".join(parts)
    return "(untitled)"

def make_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"

# ===========================================================
# Block fetch
# ===========================================================

def get_blocks(block_id):
    blocks = []
    cursor = None

    while True:
        resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)
        blocks.extend(resp.get("results", []))

        if not resp.get("has_more"):
            break

        cursor = resp.get("next_cursor")

    return blocks

# ===========================================================
# FULL TEXT EXTRACTOR
# ===========================================================

def extract_all_text_from_block(block):
    texts = []
    btype = block.get("type")
    content = block.get(btype, {}) if btype else {}

    # Column fix
    if btype in ("column_list", "column"):
        children = notion.blocks.children.list(block["id"]).get("results", [])
        for child in children:
            texts.append(extract_all_text_from_block(child))
        return " ".join(t for t in texts if t).strip()

    # rich_text
    if isinstance(content, dict) and "rich_text" in content:
        rich = content["rich_text"]
        texts.append(" ".join(t.get("plain_text", "") for t in rich if t.get("plain_text")))

    # types with rich_text
    for key in [
        "paragraph", "heading_1", "heading_2", "heading_3", "quote", "callout",
        "bulleted_list_item", "numbered_list_item", "toggle", "to_do"
    ]:
        if btype == key:
            rt = block[key].get("rich_text", [])
            texts.append(" ".join(t.get("plain_text", "") for t in rt if t.get("plain_text")))

    # caption
    if isinstance(content, dict) and "caption" in content:
        cap = content["caption"]
        texts.append(" ".join(t.get("plain_text", "") for t in cap if t.get("plain_text")))

    # synced_block
    if btype == "synced_block":
        sf = block.get("synced_block", {}).get("synced_from")
        if sf:
            original = sf.get("block_id")
            if original:
                children = notion.blocks.children.list(original).get("results", [])
                for ch in children:
                    texts.append(extract_all_text_from_block(ch))

    # tables
    if btype == "table":
        rows = notion.blocks.children.list(block["id"]).get("results", [])
        for row in rows:
            if row.get("type") == "table_row":
                for cell in row["table_row"]["cells"]:
                    texts.append(" ".join(t.get("plain_text", "") for t in cell if t.get("plain_text")))

    # children recursion
    if block.get("has_children"):
        children = notion.blocks.children.list(block["id"]).get("results", [])
        for ch in children:
            texts.append(extract_all_text_from_block(ch))

    return " ".join(t for t in texts if t).strip()

# ===========================================================
# Properties
# ===========================================================

def extract_text_from_properties(props):
    if not isinstance(props, dict):
        return ""
    texts = []

    for p in props.values():
        t = p.get("type")

        if t == "title":
            texts.append(" ".join(v.get("plain_text", "") for v in p.get("title", [])))

        elif t == "rich_text":
            texts.append(" ".join(v.get("plain_text", "") for v in p.get("rich_text", [])))

        elif t == "select":
            sel = p.get("select")
            if sel:
                texts.append(sel.get("name", ""))

        elif t == "multi_select":
            for item in p.get("multi_select", []):
                texts.append(item.get("name", ""))

    return " ".join(texts).strip()

# ===========================================================
# Language
# ===========================================================

def detect_lang(text):
    try:
        return detect(text)
    except:
        return "unknown"

def count_words(text):
    return len(re.findall(r"\b\w+\b", text))

# ===========================================================
# Parent resolution
# ===========================================================

def resolve_parent_to_page(block_id):
    visited = set()
    while True:
        if block_id in visited:
            return None
        visited.add(block_id)

        blk = notion.blocks.retrieve(block_id=block_id)
        parent = blk.get("parent", {})
        ptype = parent.get("type")

        if ptype == "page_id":
            return normalize_id(parent["page_id"])

        elif ptype == "block_id":
            block_id = parent["block_id"]
            continue

        else:
            return None

def is_child_of_root(page, root_id, page_index):
    visited = set()
    current = page

    while True:
        parent = current.get("parent", {})
        ptype = parent.get("type")

        if ptype == "page_id":
            pid = normalize_id(parent["page_id"])
            if pid == root_id:
                return True
            if pid in visited:
                return False
            visited.add(pid)
            current = page_index.get(pid) or notion.pages.retrieve(page_id=pid)
            continue

        elif ptype == "block_id":
            resolved = resolve_parent_to_page(parent["block_id"])
            if resolved == root_id:
                return True
            if resolved in visited:
                return False
            visited.add(resolved)
            current = page_index.get(resolved) or notion.pages.retrieve(page_id=resolved)
            continue

        return False

# ===========================================================
# Page Analysis
# ===========================================================

def analyze_page(pid):
    ru = 0
    en = 0

    page = notion.pages.retrieve(page_id=pid)
    props_text = extract_text_from_properties(page.get("properties", {}))

    if props_text:
        lang = detect_lang(props_text)
        w = count_words(props_text)
        if lang == "ru": ru += w
        elif lang == "en": en += w

    blocks = get_blocks(pid)
    for b in blocks:
        if b.get("type") == "child_page":
            continue
        text = extract_all_text_from_block(b)
        if not text:
            continue
        lang = detect_lang(text)
        w = count_words(text)
        if lang == "ru": ru += w
        elif lang == "en": en += w

    total = ru + en
    ru_pct = (ru / total * 100) if total else 0
    en_pct = (en / total * 100) if total else 0

    title = get_title(page)
    url = make_url(pid)

    author = page.get("created_by", {}).get("name", "(unknown)")

    return {
        "Page Title": title,
        "Page URL": url,
        "Author": author,
        "% Russian": round(ru_pct, 2),
        "% English": round(en_pct, 2),
    }

# ===========================================================
# MODE 1: List pages under root
# ===========================================================

if args.list_pages:
    pages = get_all_pages()
    idx = {normalize_id(p["id"]): p for p in pages}

    selected = []
    for p in pages:
        pid = normalize_id(p["id"])
        if pid == ROOT_PAGE_ID:
            selected.append(pid)
            continue
        if is_child_of_root(p, ROOT_PAGE_ID, idx):
            selected.append(pid)

    print(json.dumps(selected, ensure_ascii=False))
    exit(0)

# ===========================================================
# MODE 2: Batch mode
# ===========================================================

if args.batch:
    ids = [x.strip() for x in args.batch.split(",") if x.strip()]
    out_file = "notion_language_percentages.csv"

    write_header = not os.path.exists(out_file)

    with open(out_file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "Page Title", "Page URL", "Author",
                "% Russian", "% English"
            ]
        )
        if write_header:
            w.writeheader()

        for pid in ids:
            data = analyze_page(pid)
            w.writerow(data)

    print(f"Batch done: {len(ids)} pages")
    exit(0)

print("No mode specified. Use --list-pages or --batch.")
