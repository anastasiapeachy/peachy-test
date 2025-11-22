import os
import csv
import re
import time
from notion_client import Client
from notion_client.errors import APIResponseError
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
# Safe Notion call with basic retry for rate limit
# ===========================================================

def notion_call(method, *args, **kwargs):
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            return method(*args, **kwargs)
        except APIResponseError as e:
            msg = str(e).lower()
            status = getattr(e, "status", None)
            if status == 429 or "rate limited" in msg:
                wait = min(60, 2 ** attempt)
                print(f"[Rate limit] {method.__name__} attempt {attempt}/{max_attempts}, sleep {wait}s...")
                time.sleep(wait)
                continue
            raise
        except Exception as e:
            # Для нестандартных ошибок просто пробрасываем
            raise
    raise RuntimeError(f"Max retries exceeded for {method.__name__}")

# ===========================================================
# Fetch all pages in workspace
# ===========================================================

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
        time.sleep(0.1)

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
        blk = notion_call(notion.blocks.retrieve, block_id=page["id"])
        if blk.get("type") == "child_page":
            return blk["child_page"].get("title", "(untitled)")
    except Exception:
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
            resp = notion_call(
                notion.blocks.children.list,
                block_id=block_id,
                start_cursor=cursor
            )
        except Exception as e:
            print(f"Can't get blocks for {block_id}: {e}")
            break

        blocks.extend(resp.get("results", []))

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
        time.sleep(0.1)

    return blocks

# ===========================================================
# FULL TEXT EXTRACTOR (correct columns support)
# ===========================================================

def extract_all_text_from_block(block):
    texts = []
    btype = block.get("type")
    content = block.get(btype, {}) if btype else {}

    # FIX: Columns (column_list / column)
    if btype in ("column_list", "column"):
        try:
            children = notion_call(
                notion.blocks.children.list,
                block_id=block["id"]
            ).get("results", [])
            for child in children:
                texts.append(extract_all_text_from_block(child))
        except Exception:
            pass
        return " ".join(t for t in texts if t).strip()

    # generic rich_text
    if isinstance(content, dict) and "rich_text" in content:
        rich = content.get("rich_text", [])
        texts.append(" ".join(t.get("plain_text", "") for t in rich if t.get("plain_text")))

    # paragraph, headings, etc
    for key in [
        "paragraph", "heading_1", "heading_2", "heading_3",
        "quote", "callout", "bulleted_list_item",
        "numbered_list_item", "toggle", "to_do"
    ]:
        if btype == key:
            rt = block.get(key, {}).get("rich_text", [])
            texts.append(" ".join(t.get("plain_text", "") for t in rt if t.get("plain_text")))

    # caption
    if isinstance(content, dict) and "caption" in content:
        cap = content.get("caption", [])
        texts.append(" ".join(t.get("plain_text", "") for t in cap if t.get("plain_text")))

    # synced_block
    if btype == "synced_block":
        sf = block.get("synced_block", {}).get("synced_from")
        if sf:
            original_id = sf.get("block_id")
            if original_id:
                try:
                    children = notion_call(
                        notion.blocks.children.list,
                        block_id=original_id
                    ).get("results", [])
                    for child in children:
                        texts.append(extract_all_text_from_block(child))
                except Exception:
                    pass

    # table
    if btype == "table":
        try:
            rows = notion_call(
                notion.blocks.children.list,
                block_id=block["id"]
            ).get("results", [])
            for row in rows:
                if row.get("type") == "table_row":
                    cells = row["table_row"]["cells"]
                    for cell in cells:
                        texts.append(" ".join(t.get("plain_text", "") for t in cell if t.get("plain_text")))
        except Exception:
            pass

    # recursion into children
    if block.get("has_children"):
        try:
            children = notion_call(
                notion.blocks.children.list,
                block_id=block["id"]
            ).get("results", [])
            for child in children:
                texts.append(extract_all_text_from_block(child))
        except Exception:
            pass

    return " ".join(t for t in texts if t).strip()

# ===========================================================
# Parent resolution for nested blocks in columns
# ===========================================================

def resolve_block_parent_to_page(block_id):
    visited = set()
    while True:
        if block_id in visited:
            return None
        visited.add(block_id)
        try:
            blk = notion_call(notion.blocks.retrieve, block_id=block_id)
        except Exception:
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
            current = page_index.get(pid) or notion_call(notion.pages.retrieve, page_id=pid)
            continue

        elif ptype == "block_id":
            bid = parent["block_id"]
            resolved = resolve_block_parent_to_page(bid)
            if resolved == root_id:
                return True
            if not resolved or resolved in visited:
                return False
            visited.add(resolved)
            current = page_index.get(resolved) or notion_call(notion.pages.retrieve, page_id=resolved)
            continue

        else:
            return False

# ===========================================================
# Language helpers
# ===========================================================

def detect_lang(text):
    try:
        return detect(text)
    except Exception:
        return "unknown"

def count_words(text):
    return len(re.findall(r'\b\w+\b', text))

# ===========================================================
# Analyze page
# ===========================================================

def analyze_page(page_id):
    ru = 0
    en = 0

    blocks = get_blocks(page_id)
    for block in blocks:
        if block.get("type") == "child_page":
            continue
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
# MAIN WITH BATCHING
# ===========================================================

def main():
    start = time.time()

    print("Fetching all pages in workspace...")
    all_pages = get_all_pages_in_workspace()
    page_index = {normalize_id(p["id"]): p for p in all_pages}

    # select only pages under root
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
    batches = [selected[i:i + batch_size] for i in range(0, len(selected), batch_size)]

    results = []

    for bi, batch in enumerate(batches, start=1):
        print(f"Processing batch {bi}/{len(batches)} (size={len(batch)})")

        for p in batch:
            pid = normalize_id(p["id"])
            page = page_index.get(pid) or notion_call(notion.pages.retrieve, page_id=pid)
            title = get_title(page)
            url = make_url(pid)

            author_info = page.get("created_by", {}) or {}
            author = author_info.get("name")
            if not author:
                uid = author_info.get("id")
                if uid:
                    try:
                        user = notion_call(notion.users.retrieve, user_id=uid)
                        author = user.get("name")
                    except Exception:
                        author = "(unknown)"
            if not author:
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

        # пауза между батчами, чтобы уменьшить шанс 429
        time.sleep(1.0)

    if not results:
        print("No pages to save.")
        return

    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        w.writeheader()
        w.writerows(results)

    print(f"\nSaved {len(results)} rows → {fname}")
    print(f"Done in {time.time() - start:.1f}s")

if __name__ == "__main__":
    main()
