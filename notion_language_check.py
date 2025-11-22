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

def make_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"

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
    props = page.get("properties", {}) or {}
    for prop in props.values():
        if prop.get("type") == "title":
            parts = [t.get("plain_text", "") for t in prop.get("title", [])]
            if parts:
                return "".join(parts)
    try:
        blk = notion.blocks.retrieve(block_id=page["id"])
        if blk.get("type") == "child_page":
            return blk["child_page"].get("title", "(untitled)")
    except Exception:
        pass
    return "(untitled)"

# ===========================================================
# Blocks fetch (with pagination)
# ===========================================================

def get_blocks(block_id):
    blocks = []
    cursor = None

    while True:
        try:
            resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)
        except Exception as e:
            print(f"Can't get blocks for {block_id}: {e}")
            break

        blocks.extend(resp.get("results", []))

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return blocks

# ===========================================================
# FULL TEXT EXTRACTOR (columns, tables, synced blocks)
# ===========================================================

def extract_all_text_from_block(block):
    texts = []
    btype = block.get("type")
    content = block.get(btype, {}) if btype else {}

    # 1) Колонки: column_list / column → рекурсивно обходим детей
    if btype in ("column_list", "column"):
        try:
            children = notion.blocks.children.list(block["id"]).get("results", [])
            for child in children:
                texts.append(extract_all_text_from_block(child))
        except Exception:
            pass
        return " ".join(t for t in texts if t).strip()

    # 2) rich_text в content
    if isinstance(content, dict) and "rich_text" in content:
        rich = content.get("rich_text", [])
        if rich:
            texts.append(" ".join(t.get("plain_text", "") for t in rich if t.get("plain_text")))

    # 3) стандартные текстовые блоки
    for key in [
        "paragraph", "heading_1", "heading_2", "heading_3",
        "quote", "callout", "bulleted_list_item",
        "numbered_list_item", "toggle", "to_do"
    ]:
        if btype == key:
            rt = block.get(key, {}).get("rich_text", [])
            if rt:
                texts.append(" ".join(t.get("plain_text", "") for t in rt if t.get("plain_text")))

    # 4) подписи (caption)
    if isinstance(content, dict) and "caption" in content:
        cap = content.get("caption", [])
        if cap:
            texts.append(" ".join(t.get("plain_text", "") for t in cap if t.get("plain_text")))

    # 5) synced_block → если это ссылка, тянем текст из оригинала
    if btype == "synced_block":
        sf = block.get("synced_block", {}).get("synced_from")
        if sf:
            original_id = sf.get("block_id")
            if original_id:
                try:
                    children = notion.blocks.children.list(original_id).get("results", [])
                    for child in children:
                        texts.append(extract_all_text_from_block(child))
                except Exception:
                    pass

    # 6) таблица
    if btype == "table":
        try:
            rows = notion.blocks.children.list(block["id"]).get("results", [])
            for row in rows:
                if row.get("type") == "table_row":
                    cells = row["table_row"].get("cells", [])
                    for cell in cells:
                        texts.append(" ".join(t.get("plain_text", "") for t in cell if t.get("plain_text")))
        except Exception:
            pass

    # 7) рекурсивно обходим детей (для toggle, callout, списков и т.п.)
    if block.get("has_children"):
        try:
            children = notion.blocks.children.list(block["id"]).get("results", [])
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
            blk = notion.blocks.retrieve(block_id=block_id)
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
# Is page child of root (строгая цепочка page/block → page → root)
# ===========================================================

def is_child_of_root(page, root_id, page_index):
    visited = set()
    current = page

    while True:
        parent = current.get("parent", {}) or {}
        ptype = parent.get("type")

        # page → page цепочка
        if ptype == "page_id":
            pid = normalize_id(parent["page_id"])
            if pid == root_id:
                return True
            if pid in visited:
                return False
            visited.add(pid)
            current = page_index.get(pid) or notion.pages.retrieve(pid)
            continue

        # page → block → ... → page → root
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

        # всё остальное выкидываем
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

    print("Fetching ALL pages in workspace (once)...")
    all_pages = get_all_pages_in_workspace()
    page_index = {normalize_id(p["id"]): p for p in all_pages}

    # 1. выбрать только root + его потомков
    selected = []
    for p in all_pages:
        pid = normalize_id(p["id"])
        if pid == ROOT_PAGE_ID:
            selected.append(p)
            continue
        try:
            if is_child_of_root(p, ROOT_PAGE_ID, page_index):
                selected.append(p)
        except Exception as e:
            print(f"Error checking ancestry for {pid}: {e}")

    print(f"Total pages under root: {len(selected)}")

    # 2. батчи по 20
    batch_size = 20
    batches = [selected[i:i + batch_size] for i in range(0, len(selected), batch_size)]

    results = []

    for bi, batch in enumerate(batches, start=1):
        print(f"Processing batch {bi}/{len(batches)}, size={len(batch)}")

        for p in batch:
            pid = normalize_id(p["id"])
            page = page_index.get(pid) or notion.pages.retrieve(pid)

            title = get_title(page)
            url = make_url(pid)

            # автор
            author_info = page.get("created_by", {}) or {}
            author = author_info.get("name")
            if not author:
                uid = author_info.get("id")
                if uid:
                    try:
                        user = notion.users.retrieve(uid)
                        author = user.get("name")
                    except Exception:
                        author = None
            if not author:
                author = "(unknown)"

            # анализ текста
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

        # лёгкая пауза между батчами, чтобы меньше ловить rate limit
        time.sleep(0.5)

    # сортировка: сначала % English, потом % Russian — по убыванию
    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    # 3. один финальный CSV
    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["Page Title", "Page URL", "Author", "% Russian", "% English"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved {len(results)} rows → {fname}")
    print(f"Done in {time.time() - start:.1f}s")

if __name__ == "__main__":
    main()
