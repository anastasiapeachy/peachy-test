import os
import csv
import re
import time
from notion_client import Client
from langdetect import detect

# ===========================================================
# ENV
# ===========================================================

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID env vars")

notion = Client(auth=NOTION_TOKEN)

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

# ===========================================================
# Fetch all pages in workspace
# ===========================================================

def get_all_pages_in_workspace():
    pages = []
    cursor = None

    while True:
        resp = notion.search(
            query="",
            filter={"value": "page", "property": "object"},
            start_cursor=cursor
        )

        pages.extend(resp.get("results", []))

        if not resp.get("has_more"):
            break

        cursor = resp.get("next_cursor")

    return pages

# ===========================================================
# Title extractor
# ===========================================================

def get_title(page):
    props = page.get("properties", {})
    for prop in props.values():
        if prop.get("type") == "title":
            parts = [t.get("plain_text", "") for t in prop.get("title", [])]
            if parts:
                return "".join(parts)
    try:
        blk = notion.blocks.retrieve(block_id=page["id"])
        if blk["type"] == "child_page":
            return blk["child_page"]["title"]
    except:
        pass
    return "(untitled)"

def make_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"

# ===========================================================
# Blocks fetch (with pagination)
# ===========================================================

def get_blocks(block_id):
    blocks = []
    cursor = None

    while True:
        try:
            resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)
        except:
            break

        blocks.extend(resp.get("results", []))

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return blocks

# ===========================================================
# FULL TEXT EXTRACTOR — THE GOOD ONE
# ===========================================================

def extract_all_text_from_block(block):
    texts = []
    btype = block.get("type")
    content = block.get(btype, {}) if btype else {}

    # Correct handling for columns
    if btype in ("column_list", "column"):
        try:
            children = notion.blocks.children.list(block["id"]).get("results", [])
            for child in children:
                texts.append(extract_all_text_from_block(child))
        except:
            pass
        return " ".join(t for t in texts if t).strip()

    # Rich text
    if isinstance(content, dict) and "rich_text" in content:
        rich = content.get("rich_text", [])
        texts.append(" ".join(t.get("plain_text", "") for t in rich))

    # Headings, paragraphs, list items etc
    for key in [
        "paragraph", "heading_1", "heading_2", "heading_3",
        "quote", "callout", "bulleted_list_item",
        "numbered_list_item", "toggle", "to_do"
    ]:
        if btype == key:
            rt = block.get(key, {}).get("rich_text", [])
            texts.append(" ".join(t.get("plain_text", "") for t in rt))

    # Caption
    if isinstance(content, dict) and "caption" in content:
        cap = content.get("caption", [])
        texts.append(" ".join(t.get("plain_text", "") for t in cap))

    # Synced block
    if btype == "synced_block":
        sf = block.get("synced_block", {}).get("synced_from")
        if sf:
            original_id = sf.get("block_id")
            if original_id:
                try:
                    children = notion.blocks.children.list(original_id).get("results", [])
                    for child in children:
                        texts.append(extract_all_text_from_block(child))
                except:
                    pass

    # Tables
    if btype == "table":
        try:
            rows = notion.blocks.children.list(block["id"]).get("results", [])
            for row in rows:
                if row["type"] == "table_row":
                    cells = row["table_row"]["cells"]
                    for cell in cells:
                        texts.append(" ".join(t.get("plain_text", "") for t in cell))
        except:
            pass

    # Children recursion
    if block.get("has_children"):
        try:
            children = notion.blocks.children.list(block["id"]).get("results", [])
            for child in children:
                texts.append(extract_all_text_from_block(child))
        except:
            pass

    return " ".join(t for t in texts if t).strip()

# ===========================================================
# Parent resolution for blocks in columns
# ===========================================================

def resolve_block_parent_to_page(block_id):
    visited = set()
    while True:
        if block_id in visited:
            return None
        visited.add(block_id)
        try:
            blk = notion.blocks.retrieve(block_id=block_id)
        except:
            return None
        parent = blk.get("parent", {})
        ptype = parent.get("type")
        if ptype == "page_id":
            return normalize_id(parent["page_id"])
        if ptype == "block_id":
            block_id = parent["block_id"]
            continue
        return None

# ===========================================================
# Is page child of root
# ===========================================================

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
            current = page_index.get(pid) or notion.pages.retrieve(pid)
            continue

        elif ptype == "block_id":
            bid = parent["block_id"]
            resolved = resolve_block_parent_to_page(bid)
            if resolved == root_id:
                return True
            if not resolved or resolved in visited:
                return False
            visited.add(resolved)
            current = page_index.get(resolved) or notion.pages.retrieve(resolved)
            continue

        else:
            return False

# ===========================================================
# Language helpers
# ===========================================================

def detect_lang(text):
    try:
        return detect(text)
    except:
        return "unknown"

def count_words(text):
    return len(re.findall(r'\b\w+\b', text))

# ===========================================================
# FIXED analyze_page() — MAIN BUG FIX
# ===========================================================

def analyze_page(page_id):
    ru = 0
    en = 0

    blocks = get_blocks(page_id)

    for block in blocks:
        text = extract_all_text_from_block(block)

        if not text:
            continue

        lang = detect_lang(text)
        words = count_words(text)

        if lang == "ru":
            ru += words
        elif lang == "en":
            en += words

    return ru, en

# ===========================================================
# MAIN with batching
# ===========================================================

def main():
    start = time.time()

    print("Fetching all pages in workspace...")
    all_pages = get_all_pages_in_workspace()
    page_index = {normalize_id(p["id"]): p for p in all_pages}

    # Select pages under root
    selected = []
    for p in all_pages:
        pid = normalize_id(p["id"])
        if pid == ROOT_PAGE_ID:
            selected.append(p)
            continue
        if is_child_of_root(p, ROOT_PAGE_ID, page_index):
            selected.append(p)

    print(f"Total pages under root: {len(selected)}")

    batch_size = 20
    batches = [selected[i:i+batch_size] for i in range(0, len(selected), batch_size)]

    results = []

    for bi, batch in enumerate(batches, start=1):
        print(f"Processing batch {bi}/{len(batches)} (size={len(batch)})")

        for p in batch:
            pid = normalize_id(p["id"])
            page = page_index.get(pid) or notion.pages.retrieve(pid)

            title = get_title(page)
            url = make_url(pid)

            author_info = page.get("created_by", {})
            author = author_info.get("name")
            if not author:
                try:
                    u = notion.users.retrieve(author_info.get("id"))
                    author = u.get("name")
                except:
                    author = "(unknown)"

            ru, en = analyze_page(pid)
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

        time.sleep(1.0)

    # Sort
    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    # Final CSV
    out = "notion_language_percentages.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        w.writeheader()
        w.writerows(results)

    print(f"\nSaved {len(results)} rows → {out}")
    print(f"Done in {time.time() - start:.1f}s")

if __name__ == "__main__":
    main()
