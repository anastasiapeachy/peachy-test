import os
import csv
import re
import time
from notion_client import Client
from langdetect import detect

# grab env vars
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")  # now correct

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID env vars")

# clean up page IDs - handle URLs, dashes, etc
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

def get_all_pages():
    """Pull all pages from workspace using search"""
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
    except:
        pass

    return "(untitled)"

def make_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"

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

def extract_text(block):
    btype = block.get("type")
    if btype and isinstance(block.get(btype), dict):
        rich = block[btype].get("rich_text", [])
        return "".join([t.get("plain_text", "") for t in rich]).strip()
    return ""

# ===========================================================
# Extract ALL plain text recursively (tables, columns, captions)
# ===========================================================

def extract_all_text_from_block(block):
    """Recursively extracts all human-readable text from a Notion block, including deeply nested structures."""
    texts = []

    btype = block.get("type")

    # 1. Standard rich_text
    rich_text = block.get(btype, {}).get("rich_text")
    if rich_text:
        texts.append(" ".join(t.get("plain_text", "") for t in rich_text))

    # 2. headings, paragraphs, callouts etc
    for key in [
        "paragraph", "heading_1", "heading_2", "heading_3", "quote", "callout",
        "bulleted_list_item", "numbered_list_item", "toggle"
    ]:
        if btype == key:
            rt = block.get(key, {}).get("rich_text", [])
            texts.append(" ".join(t.get("plain_text", "") for t in rt))

    # 3. captions
    if "caption" in block.get(btype, {}):
        cap = block[btype]["caption"]
        if cap:
            texts.append(" ".join(t.get("plain_text", "") for t in cap))

    # 4. equation
    if btype == "equation":
        eq = block.get("equation", {}).get("expression")
        if eq:
            texts.append(eq)

    # 5. synced_block
    if btype == "synced_block":
        synced = block.get("synced_block", {})
        if synced.get("synced_from") is not None:
            try:
                original_id = synced["synced_from"]["block_id"]
                children = notion.blocks.children.list(original_id).get("results", [])
                for child in children:
                    texts.append(extract_all_text_from_block(child))
            except:
                pass

    # 6. table
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

    # 7. children recursion
    try:
        if block.get("has_children"):
            children = notion.blocks.children.list(block["id"]).get("results", [])
            for child in children:
                texts.append(extract_all_text_from_block(child))
    except:
        pass

    return " ".join(t for t in texts if t).strip()


def detect_lang(text):
    try:
        return detect(text)
    except:
        return "unknown"

def count_words(text):
    return len(re.findall(r'\b\w+\b', text))

def is_child_of_root(page, root_id, page_index):
    visited = set()
    current = page
    while True:
        parent = current.get("parent", {}) or {}
        ptype = parent.get("type")

        if ptype == "page_id":
            pid = normalize_id(parent.get("page_id"))
            if pid == root_id:
                return True
            if pid in visited:
                return False
            visited.add(pid)
            current = page_index.get(pid) or notion.pages.retrieve(page_id=pid)
            continue

        elif ptype == "database_id":
            dbid = parent.get("database_id")
            try:
                db = notion.databases.retrieve(database_id=dbid)
                db_parent = db.get("parent", {})
                if db_parent.get("type") == "page_id":
                    pid = normalize_id(db_parent.get("page_id"))
                    if pid == root_id:
                        return True
                    if pid in visited:
                        return False
                    visited.add(pid)
                    current = page_index.get(pid) or notion.pages.retrieve(page_id=pid)
                    continue
                return False
            except:
                return False

        elif ptype == "block_id":
            bid = parent.get("block_id")
            try:
                blk = notion.blocks.retrieve(block_id=bid)
                current = {"id": bid, "parent": blk.get("parent", {})}
                continue
            except:
                return False

        else:
            return False


# ===========================================================
# FIX: Use extract_all_text_from_block()
# ===========================================================

def analyze_page(page_id):
    ru = 0
    en = 0

    blocks = get_blocks(page_id)

    for block in blocks:
        if block.get("type") == "child_page":
            continue

        text = extract_all_text_from_block(block)  # ← FIXED

        if not text:
            continue

        lang = detect_lang(text)
        words = count_words(text)

        if lang == "ru":
            ru += words
        elif lang == "en":
            en += words

    return ru, en


def main():
    start = time.time()

    print("Fetching all pages...")
    pages = get_all_pages()
    print(f"Found {len(pages)} total pages")

    page_index = {}
    for p in pages:
        pid = normalize_id(p.get("id"))
        page_index[pid] = p

    selected = []
    for p in pages:
        pid = normalize_id(p.get("id"))

        if pid == ROOT_PAGE_ID:
            selected.append(p)
            continue

        try:
            if is_child_of_root(p, ROOT_PAGE_ID, page_index):
                selected.append(p)
        except Exception as e:
            print(f"Error checking {pid}: {e}")

    print(f"Found {len(selected)} pages under root")

    results = []
    for p in selected:
        pid = normalize_id(p.get("id"))
        title = get_title(p)
        url = make_url(pid)

        # --------- FIXED AUTHOR HANDLING ----------
        author_info = p.get("created_by", {}) or {}
        author = author_info.get("name")

        if not author:
            uid = author_info.get("id")
            if uid:
                try:
                    user_data = notion.users.retrieve(user_id=uid)
                    author = user_data.get("name")
                except:
                    author = None

        if not author:
            author = "(unknown)"
        # -------------------------------------------

        ru, en = analyze_page(pid)
        total = ru + en
        ru_pct = (ru / total * 100) if total else 0
        en_pct = (en / total * 100) if total else 0

        results.append({
            "Page Title": title,
            "Page URL": url,
            "Author": author,
            "% Russian": round(ru_pct, 2),
            "% English": round(en_pct, 2)
        })

    results.sort(key=lambda x: x["% English"], reverse=True)

    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Page Title", "Page URL", "Author", "% Russian", "% English"]
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved {len(results)} rows to {fname}")
    print(f"Took {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
