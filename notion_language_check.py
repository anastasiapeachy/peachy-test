import os
import csv
import re
import time
from notion_client import Client

# =============================
# Settings
# =============================

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID env vars")

# Normalize ID
def normalize_id(raw_id):
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip()
    # strip all non-hex chars
    cleaned = re.sub(r"[^0-9a-fA-F]", "", s)
    # if 32-character hex -> OK
    if len(cleaned) == 32:
        return cleaned.lower()
    return cleaned.lower()

ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)

notion = Client(auth=NOTION_TOKEN)

# =============================
# PROFESSIONAL LANGUAGE DETECTOR
# =============================

RU_CHARS = set("абвгдеёжзийклмнопрстуфхцчшщьыъэюя")
EN_CHARS = set("abcdefghijklmnopqrstuvwxyz")

def detect_lang_pro(text: str):
    """Professional heuristic language detection (EN vs RU)."""
    if not text:
        return "unknown"

    t = text.lower()

    ru_count = sum(c in RU_CHARS for c in t)
    en_count = sum(c in EN_CHARS for c in t)

    # If both exist, choose the dominant script
    if ru_count > en_count:
        return "ru"
    if en_count > ru_count:
        return "en"

    # fallback
    if ru_count > 0:
        return "ru"
    if en_count > 0:
        return "en"

    return "unknown"


def count_words(text: str):
    return len(re.findall(r"\b\w+\b", text))


# =============================
# PAGE GATHERING
# =============================

def get_all_pages():
    """Retrieve all pages from workspace."""
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


def get_title(page):
    """Extract title from page or fallback."""
    props = page.get("properties", {}) or {}
    for prop in props.values():
        if prop.get("type") == "title":
            parts = [t.get("plain_text", "") for t in prop.get("title", [])]
            if parts:
                return "".join(parts)

    # fallback for child_page
    try:
        blk = notion.blocks.retrieve(block_id=page["id"])
        if blk.get("type") == "child_page":
            return blk["child_page"].get("title", "(untitled)")
    except:
        pass

    return "(untitled)"


def make_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_blocks(block_id):
    """Fetch direct children of a block/page."""
    blocks = []
    cursor = None
    while True:
        resp = notion.blocks.children.list(
            block_id=block_id,
            start_cursor=cursor
        )
        blocks.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return blocks


# =============================
# TEXT EXTRACTION ENGINE
# =============================

RICH_TEXT_TYPES = {
    "paragraph",
    "heading_1",
    "heading_2",
    "heading_3",
    "quote",
    "callout",
    "bulleted_list_item",
    "numbered_list_item",
    "toggle",
    "to_do"
}

def extract_richtext(rt_list):
    return " ".join(
        t.get("plain_text", "")
        for t in (rt_list or [])
        if t.get("plain_text")
    )


def extract_all_text_from_block(block):
    """Fully recursive, stable, column-aware text extractor."""

    texts = []
    btype = block.get("type")
    if not btype:
        return ""

    content = block.get(btype, {}) or {}

    # 1) COLUMN FIX
    if btype in ("column_list", "column"):
        children = get_blocks(block["id"])
        for child in children:
            texts.append(extract_all_text_from_block(child))
        return " ".join(t for t in texts if t).strip()

    # 2) Standard rich_text blocks
    if btype in RICH_TEXT_TYPES:
        rt = content.get("rich_text", [])
        if rt:
            texts.append(extract_richtext(rt))

    # 3) caption
    if isinstance(content, dict) and "caption" in content:
        texts.append(extract_richtext(content.get("caption", [])))

    # 4) synced_block reference
    if btype == "synced_block":
        synced = content
        if synced.get("synced_from"):
            original_id = synced["synced_from"].get("block_id")
            if original_id:
                original_children = get_blocks(original_id)
                for ch in original_children:
                    texts.append(extract_all_text_from_block(ch))

    # 5) tables
    if btype == "table":
        rows = get_blocks(block["id"])
        for row in rows:
            if row.get("type") == "table_row":
                for cell in row["table_row"].get("cells", []):
                    texts.append(extract_richtext(cell))

    # 6) Deep recursion
    if block.get("has_children"):
        children = get_blocks(block["id"])
        for child in children:
            texts.append(extract_all_text_from_block(child))

    return " ".join(t for t in texts if t).strip()


# =============================
# PROPERTY TEXT EXTRACTOR
# =============================

def extract_text_from_properties(properties):
    texts = []
    if not isinstance(properties, dict):
        return ""

    for prop in properties.values():
        ptype = prop.get("type")

        if ptype == "title":
            texts.append(extract_richtext(prop.get("title")))

        elif ptype == "rich_text":
            texts.append(extract_richtext(prop.get("rich_text")))

        elif ptype == "select" and prop.get("select"):
            texts.append(prop["select"].get("name", ""))

        elif ptype == "multi_select":
            for item in prop.get("multi_select", []):
                texts.append(item.get("name", ""))

        elif ptype == "status" and prop.get("status"):
            texts.append(prop["status"].get("name", ""))

        elif ptype == "formula":
            f = prop.get("formula", {})
            if f.get("type") == "string" and f.get("string"):
                texts.append(f.get("string"))

        elif ptype == "rollup":
            r = prop.get("rollup", {})
            if r.get("type") == "array":
                for item in r.get("array", []):
                    if "title" in item:
                        texts.append(extract_richtext(item["title"]))
                    if "rich_text" in item:
                        texts.append(extract_richtext(item["rich_text"]))

        elif ptype == "people":
            for u in prop.get("people", []):
                if u.get("name"):
                    texts.append(u["name"])

        elif ptype in ("number", "url", "email", "phone") and prop.get(ptype):
            texts.append(str(prop[ptype]))

    return " ".join(t for t in texts if t).strip()


# =============================
# HIERARCHY CHECK
# =============================

def is_child_of_root(page, root_id, page_index):
    """
    Strict mode (A):
    - Only page_id and database_id are allowed in the chain.
    - ANY block_id parent → immediately return False.
    """

    visited = set()
    current = page

    while True:
        parent = current.get("parent", {}) or {}
        ptype = parent.get("type")

        # --- Direct page parent ---
        if ptype == "page_id":
            pid = normalize_id(parent.get("page_id"))
            if pid == root_id:
                return True
            if pid in visited:
                return False
            visited.add(pid)

            # Move upward
            current = page_index.get(pid) or notion.pages.retrieve(page_id=pid)
            continue

        # --- Database parent (allowed) ---
        elif ptype == "database_id":
            dbid = parent.get("database_id")
            try:
                db = notion.databases.retrieve(database_id=dbid)
                db_parent = db.get("parent", {})

                # Only allow page_id as parent of DB
                if db_parent.get("type") != "page_id":
                    return False

                pid = normalize_id(db_parent.get("page_id"))
                if pid == root_id:
                    return True

                if pid in visited:
                    return False
                visited.add(pid)

                current = page_index.get(pid) or notion.pages.retrieve(page_id=pid)
                continue

            except Exception:
                return False

        # --- Block parent (STRICT MODE) ---
        elif ptype == "block_id":
            # ❗ Strict mode: ANY block_id → NOT child of root
            return False

        # --- Anything else → not a child ---
        else:
            return False

        # block parent
        if ptype == "block_id":
            bid = parent.get("block_id")
            try:
                blk = notion.blocks.retrieve(block_id=bid)
                current = {"id": bid, "parent": blk.get("parent", {})}
                continue
            except:
                return False

        return False


# =============================
# PAGE ANALYSIS
# =============================

def analyze_page(page_id):
    ru = 0
    en = 0
    unreadable = False

    # 1) Props
    try:
        page = notion.pages.retrieve(page_id=page_id)
        props_text = extract_text_from_properties(page.get("properties", {}))

        if props_text:
            lang = detect_lang_pro(props_text)
            words = count_words(props_text)
            if lang == "ru":
                ru += words
            elif lang == "en":
                en += words

    except Exception:
        props_text = ""

    # 2) Content blocks
    blocks = get_blocks(page_id)

    if not props_text and not blocks:
        unreadable = True

    for block in blocks:
        if block.get("type") == "child_page":
            continue

        text = extract_all_text_from_block(block)
        if not text:
            continue

        lang = detect_lang_pro(text)
        words = count_words(text)

        if lang == "ru":
            ru += words
        elif lang == "en":
            en += words

    return ru, en, unreadable


# =============================
# MAIN
# =============================

def main():
    start = time.time()
    print("Fetching pages...")
    pages = get_all_pages()
    print(f"Total pages in workspace: {len(pages)}")

    page_index = {normalize_id(p["id"]): p for p in pages}

    # Filter pages inside root
    selected = []
    for p in pages:
        pid = normalize_id(p["id"])
        if pid == ROOT_PAGE_ID or is_child_of_root(p, ROOT_PAGE_ID, page_index):
            selected.append(p)

    print(f"Pages under root: {len(selected)}")

    results = []
    unreadable_pages = []

    for p in selected:
        pid = normalize_id(p["id"])
        title = get_title(p)
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

        ru, en, unreadable = analyze_page(pid)

        if unreadable:
            unreadable_pages.append((title, url))

        total = ru + en
        ru_pct = round((ru / total * 100), 2) if total else 0
        en_pct = round((en / total * 100), 2) if total else 0

        results.append({
            "Page Title": title,
            "Page URL": url,
            "Author": author,
            "% Russian": ru_pct,
            "% English": en_pct
        })

    # sort by English desc
    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Page Title", "Page URL", "Author", "% Russian", "% English"]
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved CSV: {fname}")

    if unreadable_pages:
        print("\n⚠ Unreadable pages (not shared with integration):")
        for title, url in unreadable_pages:
            print(f" - {title}: {url}")

    print(f"\nDone in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
