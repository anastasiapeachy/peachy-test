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
# Fetch all pages in workspace (search API)
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
# Page title
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
    except:
        pass

    return "(untitled)"


def make_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


# ===========================================================
# Basic block fetch
# ===========================================================

def get_blocks(block_id):
    blocks = []
    cursor = None
    while True:
        try:
            resp = notion.blocks.children.list(
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
    return blocks


def extract_all_text_from_block(block):
    texts = []
    btype = block.get("type")
    content = block.get(btype, {}) if btype else {}

    # 1) Handle column_list & column properly
    if btype in ("column_list", "column"):
        try:
            children = notion.blocks.children.list(block["id"]).get("results", [])
            for child in children:
                texts.append(extract_all_text_from_block(child))
        except:
            pass
        return " ".join(t for t in texts if t).strip()

    # 2) rich_text
    if isinstance(content, dict) and "rich_text" in content:
        rt = content.get("rich_text", [])
        texts.append(" ".join(t.get("plain_text", "") for t in rt if t.get("plain_text")))

    # 3) blocks that store rich_text inside a nested key
    for key in (
        "paragraph", "heading_1", "heading_2", "heading_3", "quote", "callout",
        "bulleted_list_item", "numbered_list_item", "toggle", "to_do"
    ):
        if btype == key:
            rt = block[key].get("rich_text", [])
            texts.append(" ".join(t.get("plain_text", "") for t in rt if t.get("plain_text")))

    # 4) caption (images, files, embeds)
    if isinstance(content, dict) and "caption" in content:
        caption = content.get("caption", [])
        texts.append(" ".join(t.get("plain_text", "") for t in caption if t.get("plain_text")))

    # 5) equations
    if btype == "equation":
        eq = block["equation"].get("expression")
        if eq:
            texts.append(eq)

    # 6) table rows
    if btype == "table":
        try:
            rows = notion.blocks.children.list(block["id"]).get("results", [])
            for row in rows:
                if row.get("type") == "table_row":
                    cells = row["table_row"]["cells"]
                    for cell in cells:
                        texts.append(" ".join(t.get("plain_text", "") for t in cell if t.get("plain_text")))
        except:
            pass

    # 7) synced_block reference
    if btype == "synced_block":
        sf = block["synced_block"].get("synced_from")
        if sf:
            original = sf.get("block_id")
            if original:
                try:
                    orig_children = notion.blocks.children.list(original).get("results", [])
                    for child in orig_children:
                        texts.append(extract_all_text_from_block(child))
                except:
                    pass

    # 8) Deep children recursion
    if block.get("has_children"):
        try:
            children = notion.blocks.children.list(block["id"]).get("results", [])
            for child in children:
                texts.append(extract_all_text_from_block(child))
        except:
            pass

    return " ".join(t for t in texts if t).strip()


def extract_text_from_properties(properties):
    texts = []
    if not isinstance(properties, dict):
        return ""

    for prop in properties.values():
        ptype = prop.get("type")

        if ptype == "title":
            texts.append(" ".join(
                t.get("plain_text", "") for t in prop.get("title", [])
            ))

        elif ptype == "rich_text":
            texts.append(" ".join(
                t.get("plain_text", "") for t in prop.get("rich_text", [])
            ))

        elif ptype == "select":
            sel = prop.get("select")
            if sel:
                texts.append(sel.get("name", ""))

        elif ptype == "multi_select":
            for item in prop.get("multi_select", []):
                texts.append(item.get("name", ""))

        elif ptype == "status":
            st = prop.get("status")
            if st:
                texts.append(st.get("name", ""))

        elif ptype == "formula":
            formula = prop.get("formula", {})
            if formula.get("type") == "string":
                if formula.get("string"):
                    texts.append(formula.get("string"))

        elif ptype == "rollup":
            roll = prop.get("rollup", {})
            if roll.get("type") == "array":
                for item in roll.get("array", []):
                    if "title" in item:
                        texts.append(" ".join(t.get("plain_text", "") for t in item["title"]))
                    if "rich_text" in item:
                        texts.append(" ".join(t.get("plain_text", "") for t in item["rich_text"]))

        elif ptype == "people":
            for u in prop.get("people", []):
                if u.get("name"):
                    texts.append(u.get("name"))

        elif ptype in ("number", "url", "email", "phone"):
            val = prop.get(ptype)
            if val:
                texts.append(str(val))

    return " ".join(t for t in texts if t).strip()


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
# Parent resolution (no databases)
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
            return normalize_id(parent.get("page_id"))

        elif ptype == "block_id":
            block_id = parent.get("block_id")
            continue

        else:
            return None


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

        elif ptype == "block_id":
            block_id = parent.get("block_id")
            resolved = resolve_block_parent_to_page(block_id)

            if not resolved:
                return False

            if resolved == root_id:
                return True

            if resolved in visited:
                return False
            visited.add(resolved)

            current = page_index.get(resolved) or notion.pages.retrieve(page_id=resolved)
            continue

        else:
            return False


# ===========================================================
# Page analysis
# ===========================================================

def analyze_page(page_id):
    ru = 0
    en = 0
    unreadable = False

    props_text = ""
    try:
        page = notion.pages.retrieve(page_id=page_id)
        props = page.get("properties", {}) or {}
        props_text = extract_text_from_properties(props)

        if props_text:
            lang = detect_lang(props_text)
            words = count_words(props_text)
            if lang == "ru":
                ru += words
            elif lang == "en":
                en += words
    except:
        pass

    blocks = get_blocks(page_id)

    if not props_text and len(blocks) == 0:
        unreadable = True

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

    return ru, en, unreadable


# ===========================================================
# MAIN
# ===========================================================

def main():
    start = time.time()
    unreadable_pages = []

    print("Fetching all pages...")
    pages = get_all_pages()
    print(f"Found {len(pages)} total pages")

    page_index = {normalize_id(p["id"]): p for p in pages}

    selected = []
    for p in pages:
        pid = normalize_id(p["id"])

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
        pid = normalize_id(p["id"])
        title = get_title(p)
        url = make_url(pid)

        # author
        author_info = p.get("created_by", {}) or {}
        author = author_info.get("name")

        if not author:
            uid = author_info.get("id")
            if uid:
                try:
                    user = notion.users.retrieve(user_id=uid)
                    author = user.get("name")
                except:
                    author = None
        if not author:
            author = "(unknown)"

        ru, en, unreadable = analyze_page(pid)

        if unreadable:
            print(f"⚠ Cannot read page: {title} — {url}")
            unreadable_pages.append((title, url))

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

    # Sort: English DESC, Russian DESC
    results.sort(
        key=lambda x: (x["% English"], x["% Russian"]),
        reverse=True
    )

    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Page Title", "Page URL",
                        "Author", "% Russian", "% English"]
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved {len(results)} rows to {fname}")

    if unreadable_pages:
        print("\n⚠ Pages that API could NOT read:")
        for t, u in unreadable_pages:
            print(f" - {t}: {u}")

    print(f"\nDone in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
