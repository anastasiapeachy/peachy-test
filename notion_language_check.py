import os
import csv
import re
import time
from notion_client import Client
from langdetect import detect

# ============================
# ENV VARIABLES
# ============================
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID env vars")


# ============================
# HELPERS
# ============================

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


def make_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_blocks(block_id):
    """Fetch all direct children blocks."""
    blocks = []
    cursor = None
    while True:
        try:
            resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)
        except Exception:
            break
        blocks.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return blocks


# ===================================================================
# 1) FULL RECURSIVE PAGE TREE WALKER (NO search(), NO workspace crawl)
# ===================================================================

def get_all_descendant_pages(root_page_id):
    """
    Recursively walks ONLY the subtree of ROOT_PAGE_ID.
    Returns page_id list NEVER containing external pages.
    """
    visited = set()
    collected = []

    def walk(page_id):
        if page_id in visited:
            return
        visited.add(page_id)

        collected.append(page_id)   # add page itself

        # read blocks of this page
        try:
            blocks = get_blocks(page_id)
        except:
            return

        for block in blocks:
            btype = block.get("type")

            # --- CASE 1: normal child_page ---
            if btype == "child_page":
                walk(normalize_id(block["id"]))

            # --- CASE 2: inline database ---
            if btype == "child_database":
                db_id = block["child_database"]["database_id"]
                cursor = None
                try:
                    while True:
                        q = notion.databases.query(database_id=db_id, start_cursor=cursor)
                        for row in q.get("results", []):
                            walk(normalize_id(row["id"]))
                        if not q.get("has_more"):
                            break
                        cursor = q.get("next_cursor")
                except:
                    pass

            # --- CASE 3: synced block referencing other block ---
            if btype == "synced_block":
                sf = block.get("synced_block", {}).get("synced_from")
                if sf and sf.get("block_id"):
                    try:
                        ref_children = notion.blocks.children.list(sf["block_id"]).get("results", [])
                        for c in ref_children:
                            if c.get("type") == "child_page":
                                walk(normalize_id(c["id"]))
                    except:
                        pass

            # --- CASE 4: nested columns, toggles, etc. ---
            if block.get("has_children"):
                try:
                    children = notion.blocks.children.list(block["id"]).get("results", [])
                    for c in children:
                        if c.get("type") == "child_page":
                            walk(normalize_id(c["id"]))
                except:
                    pass

    walk(root_page_id)
    return collected


# ===================================================================
# 2) EXTRACT TEXT FROM BLOCKS (INCLUDING COLUMNS)
# ===================================================================

def extract_all_text_from_block(block):
    texts = []
    btype = block.get("type")
    content = block.get(btype, {}) if btype else {}

    # --- FIX: column_list / column ---
    if btype in ("column_list", "column"):
        try:
            children = notion.blocks.children.list(block["id"]).get("results", [])
            for child in children:
                texts.append(extract_all_text_from_block(child))
        except:
            pass
        return " ".join(t for t in texts if t).strip()

    # 1) generic rich_text
    if isinstance(content, dict) and "rich_text" in content:
        rt = content.get("rich_text", [])
        if rt:
            texts.append(" ".join(t.get("plain_text", "") for t in rt if t.get("plain_text")))

    # 2) specific block types with rich text
    for t in [
        "paragraph", "heading_1", "heading_2", "heading_3",
        "quote", "callout", "bulleted_list_item",
        "numbered_list_item", "toggle", "to_do"
    ]:
        if btype == t:
            rt = block.get(t, {}).get("rich_text", [])
            texts.append(" ".join(tt.get("plain_text", "") for tt in rt))

    # 3) captions
    if isinstance(content, dict) and "caption" in content:
        cap = content.get("caption", [])
        texts.append(" ".join(t.get("plain_text", "") for t in cap))

    # 4) equations
    if btype == "equation":
        eq = block.get("equation", {}).get("expression")
        if eq:
            texts.append(eq)

    # 5) synced blocks with external references
    if btype == "synced_block":
        sf = block.get("synced_block", {}).get("synced_from")
        if sf and sf.get("block_id"):
            try:
                external = notion.blocks.children.list(sf["block_id"]).get("results", [])
                for c in external:
                    texts.append(extract_all_text_from_block(c))
            except:
                pass

    # 6) tables
    if btype == "table":
        try:
            rows = notion.blocks.children.list(block["id"]).get("results", [])
            for row in rows:
                if row.get("type") == "table_row":
                    cells = row["table_row"]["cells"]
                    for cell in cells:
                        texts.append(" ".join(t.get("plain_text", "") for t in cell))
        except:
            pass

    # 7) deep recursion
    if block.get("has_children"):
        try:
            children = notion.blocks.children.list(block["id"]).get("results", [])
            for c in children:
                texts.append(extract_all_text_from_block(c))
        except:
            pass

    return " ".join(t for t in texts if t).strip()


# ===================================================================
# 3) DETECT LANG + WORD COUNT
# ===================================================================

def detect_lang(text):
    try:
        return detect(text)
    except:
        return "unknown"


def count_words(text):
    return len(re.findall(r'\b\w+\b', text))


# ===================================================================
# 4) EXTRACT TEXT FROM PROPERTIES (DB PAGES)
# ===================================================================

def extract_text_from_properties(props):
    if not isinstance(props, dict):
        return ""
    collected = []
    for p in props.values():
        t = p.get("type")

        if t == "title":
            collected.append(" ".join(tt.get("plain_text", "") for tt in p.get("title", [])))
        elif t == "rich_text":
            collected.append(" ".join(tt.get("plain_text", "") for tt in p.get("rich_text", [])))
        elif t == "select" and p.get("select"):
            collected.append(p["select"]["name"])
        elif t == "multi_select":
            collected.extend(item["name"] for item in p.get("multi_select", []))
        elif t == "status" and p.get("status"):
            collected.append(p["status"]["name"])
        elif t == "formula":
            f = p.get("formula", {})
            if f.get("type") == "string" and f.get("string"):
                collected.append(f["string"])
        elif t == "people":
            for u in p.get("people", []):
                if u.get("name"):
                    collected.append(u["name"])

    return " ".join(collected).strip()


# ===================================================================
# 5) ANALYZE SINGLE PAGE
# ===================================================================

def analyze_page(page_id):
    ru = 0
    en = 0

    # properties
    try:
        p = notion.pages.retrieve(page_id=page_id)
        prop_text = extract_text_from_properties(p.get("properties", {}))
        lang = detect_lang(prop_text)
        words = count_words(prop_text)
        if lang == "ru": ru += words
        elif lang == "en": en += words
    except:
        pass

    # blocks
    try:
        blocks = get_blocks(page_id)
    except:
        return ru, en

    for block in blocks:
        if block.get("type") == "child_page":
            continue
        text = extract_all_text_from_block(block)
        lang = detect_lang(text)
        words = count_words(text)
        if lang == "ru": ru += words
        elif lang == "en": en += words

    return ru, en


# ===================================================================
# 6) MAIN
# ===================================================================

def main():
    start = time.time()

    print(f"Walking only subtree of ROOT: {ROOT_PAGE_ID}")

    # Get only allowed pages
    page_ids = get_all_descendant_pages(ROOT_PAGE_ID)
    print(f"Found {len(page_ids)} pages inside root tree")

    results = []

    for pid in page_ids:
        try:
            p = notion.pages.retrieve(page_id=pid)
        except:
            continue

        # title
        title = "(untitled)"
        props = p.get("properties", {})
        for prop in props.values():
            if prop.get("type") == "title":
                t = "".join(tt.get("plain_text", "") for tt in prop["title"])
                if t.strip():
                    title = t
        url = make_url(pid)

        # author
        author_info = p.get("created_by", {}) or {}
        author = author_info.get("name")
        if not author and author_info.get("id"):
            try:
                u = notion.users.retrieve(user_id=author_info["id"])
                author = u.get("name")
            except:
                author = "(unknown)"
        if not author:
            author = "(unknown)"

        # language stats
        ru, en = analyze_page(pid)
        total = ru + en
        ru_pct = round((ru / total * 100) if total else 0, 2)
        en_pct = round((en / total * 100) if total else 0, 2)

        results.append({
            "Page Title": title,
            "Page URL": url,
            "Author": author,
            "% Russian": ru_pct,
            "% English": en_pct
        })

    # sort
    results.sort(key=lambda r: (r["% English"], r["% Russian"]), reverse=True)

    # write CSV
    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "Page Title", "Page URL", "Author", "% Russian", "% English"
        ])
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved {len(results)} rows to {fname}")
    print(f"Done in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
