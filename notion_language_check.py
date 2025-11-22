import os
import csv
import re
import time
from notion_client import Client, APIResponseError
from langdetect import detect

# -------------------------------------------------------
# ENV
# -------------------------------------------------------

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID")

notion = Client(auth=NOTION_TOKEN)

# -------------------------------------------------------
# Helpers
# -------------------------------------------------------

def normalize_id(raw_id):
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip().replace("-", "")
    m = re.search(r"([0-9a-fA-F]{32})", s)
    return m.group(1) if m else s

ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)

# --- Safe Notion call with retry ---------------------------------------------

def notion_call(fn, *args, **kwargs):
    """Wraps all Notion API calls with retry & rate-limit handling."""
    for attempt in range(5):
        try:
            return fn(*args, **kwargs)
        except APIResponseError as e:
            if e.status == 429:
                wait = 1 + attempt * 2
                print(f"⚠ Rate limit, wait {wait}s...")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("Max retries exceeded")

# -------------------------------------------------------
# Fetch all pages
# -------------------------------------------------------

def get_all_pages_in_workspace():
    pages = []
    cursor = None

    while True:
        resp = notion_call(
            notion.search,
            query="",
            filter={"value": "page", "property": "object"},
            start_cursor=cursor
        )

        pages.extend(resp.get("results", []))

        if not resp.get("has_more"):
            break

        cursor = resp.get("next_cursor")

    return pages

# -------------------------------------------------------
# Title / URL
# -------------------------------------------------------

def get_title(page):
    props = page.get("properties", {})
    for p in props.values():
        if p.get("type") == "title":
            txt = [t.get("plain_text", "") for t in p.get("title", [])]
            if txt:
                return "".join(txt)
    return "(untitled)"

def make_url(page_id):
    return f"https://www.notion.so/{page_id.replace('-', '')}"

# -------------------------------------------------------
# Blocks
# -------------------------------------------------------

def get_blocks(block_id):
    blocks = []
    cursor = None

    while True:
        resp = notion_call(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor
        )
        blocks.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return blocks

# -------------------------------------------------------
# Deep text extractor (columns supported!)
# -------------------------------------------------------

def extract_all_text_from_block(block):
    texts = []
    btype = block.get("type")
    content = block.get(btype, {}) if btype else {}

    # Columns
    if btype in ("column_list", "column"):
        try:
            children = notion_call(notion.blocks.children.list, block["id"]).get("results", [])
            for c in children:
                texts.append(extract_all_text_from_block(c))
        except:
            pass
        return " ".join(t for t in texts if t).strip()

    # Generic rich_text
    if isinstance(content, dict) and "rich_text" in content:
        txt = " ".join(t.get("plain_text", "") for t in content["rich_text"])
        texts.append(txt)

    # Common rich text blocks
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
        origin = content.get("synced_from", {})
        oid = origin.get("block_id")
        if oid:
            try:
                ch = notion_call(notion.blocks.children.list, oid).get("results", [])
                for c in ch:
                    texts.append(extract_all_text_from_block(c))
            except:
                pass

    # Table
    if btype == "table":
        try:
            rows = notion_call(notion.blocks.children.list, block["id"]).get("results", [])
            for row in rows:
                if row["type"] == "table_row":
                    cells = row["table_row"]["cells"]
                    for cell in cells:
                        texts.append(" ".join(t.get("plain_text", "") for t in cell))
        except:
            pass

    # Recursion
    if block.get("has_children"):
        try:
            ch = notion_call(notion.blocks.children.list, block["id"]).get("results", [])
            for c in ch:
                texts.append(extract_all_text_from_block(c))
        except:
            pass

    return " ".join(t for t in texts if t).strip()

# -------------------------------------------------------
# Parent resolution
# -------------------------------------------------------

def resolve_block_parent_to_page(block_id):
    visited = set()
    while True:
        if block_id in visited:
            return None
        visited.add(block_id)

        try:
            blk = notion_call(notion.blocks.retrieve, block_id=block_id)
        except:
            return None

        parent = blk.get("parent", {})
        t = parent.get("type")

        if t == "page_id":
            return normalize_id(parent["page_id"])

        if t == "block_id":
            block_id = parent["block_id"]
            continue

        return None

def is_child_of_root(page, root_id, page_index):
    visited = set()
    current = page

    while True:
        parent = current.get("parent", {})
        t = parent.get("type")

        if t == "page_id":
            pid = normalize_id(parent["page_id"])
            if pid == root_id:
                return True
            if pid in visited:
                return False
            visited.add(pid)
            current = page_index.get(pid) or notion_call(notion.pages.retrieve, page_id=pid)
            continue

        if t == "block_id":
            resolved = resolve_block_parent_to_page(parent["block_id"])
            if resolved == root_id:
                return True
            if not resolved or resolved in visited:
                return False
            visited.add(resolved)
            current = page_index.get(resolved) or notion_call(notion.pages.retrieve, page_id=resolved)
            continue

        return False

# -------------------------------------------------------
# Lang helpers
# -------------------------------------------------------

def detect_lang_safe(text):
    try:
        return detect(text)
    except:
        return "unknown"

def count_words(text):
    return len(re.findall(r"\b\w+\b", text))

# -------------------------------------------------------
# Analyze page
# -------------------------------------------------------

def analyze_page(pid):
    ru = 0
    en = 0

    blocks = get_blocks(pid)

    for b in blocks:
        if b["type"] == "child_page":
            continue

        txt = extract_all_text_from_block(b)
        if not txt:
            continue

        lang = detect_lang_safe(txt)
        w = count_words(txt)

        if lang == "ru":
            ru += w
        elif lang == "en":
            en += w

    return ru, en

# -------------------------------------------------------
# MAIN
# -------------------------------------------------------

def main():
    print("Searching all workspace pages...")
    all_pages = get_all_pages_in_workspace()
    page_index = {normalize_id(p["id"]): p for p in all_pages}

    selected = []
    for p in all_pages:
        pid = normalize_id(p["id"])

        # root always included
        if pid == ROOT_PAGE_ID:
            selected.append(p)
            continue

        # recursive ancestry check
        try:
            if is_child_of_root(p, ROOT_PAGE_ID, page_index):
                selected.append(p)
        except Exception as e:
            print(f"⚠ ancestry check failed for {pid}: {e}")

    print(f"Total pages under root: {len(selected)}")

    # batching
    batch_size = 20
    batches = [selected[i:i+batch_size] for i in range(0, len(selected), batch_size)]

    results = []

    for bi, batch in enumerate(batches, 1):
        print(f"\nProcessing batch {bi}/{len(batches)} (size={len(batch)})")

        for p in batch:
            pid = normalize_id(p["id"])

            # skip pages Notion won't give us
            try:
                page = page_index[pid]
            except KeyError:
                try:
                    page = notion_call(notion.pages.retrieve, page_id=pid)
                except APIResponseError as e:
                    print(f"⚠ Skip page {pid}: {e}")
                    continue

            title = get_title(page)
            url = make_url(pid)

            # author
            author_info = page.get("created_by", {})
            author = author_info.get("name", "(unknown)")

            ru, en = analyze_page(pid)
            total = ru + en
            ru_pct = (ru * 100 / total) if total else 0
            en_pct = (en * 100 / total) if total else 0

            results.append({
                "Page Title": title,
                "Page URL": url,
                "Author": author or "(unknown)",
                "% Russian": round(ru_pct, 2),
                "% English": round(en_pct, 2)
            })

        time.sleep(0.4)

    # sorting
    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    # output
    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        w.writeheader()
        w.writerows(results)

    print(f"\nSaved {len(results)} pages → {fname}")

if __name__ == "__main__":
    main()
