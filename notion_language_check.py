import os
import csv
import re
import time
from notion_client import Client
from langdetect import detect

# -----------------------------
# ENV VARS
# -----------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID env vars")


# -----------------------------
# HELPERS
# -----------------------------
def normalize_id(raw_id):
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip().replace("-", "")
    match = re.search(r"([0-9a-fA-F]{32})", s)
    if match:
        return match.group(1)
    return s


ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)
notion = Client(auth=NOTION_TOKEN)


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


# -----------------------------
# BLOCK TEXT EXTRACTION
# -----------------------------
def extract_all_text_from_block(block):
    texts = []
    btype = block.get("type")
    content = block.get(btype, {}) if btype else {}

    # ---- FIX column & column_list ----
    if btype in ("column_list", "column"):
        try:
            children = notion.blocks.children.list(block["id"]).get("results", [])
            for child in children:
                texts.append(extract_all_text_from_block(child))
        except:
            pass
        return " ".join(t for t in texts if t).strip()

    # rich text
    if isinstance(content, dict) and "rich_text" in content:
        rich = content.get("rich_text", [])
        texts.append(" ".join(t.get("plain_text", "") for t in rich if t.get("plain_text")))

    # paragraphs, headings, list items etc
    SIMPLE_TYPES = [
        "paragraph", "heading_1", "heading_2", "heading_3", "quote",
        "callout", "bulleted_list_item", "numbered_list_item",
        "toggle", "to_do"
    ]
    if btype in SIMPLE_TYPES:
        rt = block.get(btype, {}).get("rich_text", [])
        texts.append(" ".join(t.get("plain_text", "") for t in rt if t.get("plain_text")))

    # caption
    if isinstance(content, dict) and "caption" in content:
        cap = content.get("caption", [])
        texts.append(" ".join(t.get("plain_text", "") for t in cap if t.get("plain_text")))

    # equation
    if btype == "equation":
        eq = block.get("equation", {}).get("expression")
        if eq:
            texts.append(eq)

    # synced block
    if btype == "synced_block":
        synced = block.get("synced_block", {})
        sf = synced.get("synced_from")
        if sf:
            original_id = sf.get("block_id")
            try:
                children = notion.blocks.children.list(original_id).get("results", [])
                for child in children:
                    texts.append(extract_all_text_from_block(child))
            except:
                pass

    # table
    if btype == "table":
        try:
            rows = notion.blocks.children.list(block["id"]).get("results", [])
            for row in rows:
                if row.get("type") == "table_row":
                    cells = row["table_row"].get("cells", [])
                    for cell in cells:
                        texts.append(" ".join(t.get("plain_text", "") for t in cell if t.get("plain_text")))
        except:
            pass

    # deep recursion
    if block.get("has_children"):
        try:
            children = notion.blocks.children.list(block["id"]).get("results", [])
            for child in children:
                texts.append(extract_all_text_from_block(child))
        except:
            pass

    return " ".join(t for t in texts if t).strip()


def extract_text_from_properties(props):
    texts = []
    if not isinstance(props, dict):
        return ""

    for prop in props.values():
        ptype = prop.get("type")

        if ptype == "title":
            texts.append(" ".join(t.get("plain_text", "") for t in prop.get("title", [])))

        elif ptype == "rich_text":
            texts.append(" ".join(t.get("plain_text", "") for t in prop.get("rich_text", [])))

        elif ptype == "select" and prop.get("select"):
            texts.append(prop["select"].get("name", ""))

        elif ptype == "multi_select":
            for item in prop.get("multi_select", []):
                texts.append(item.get("name", ""))

        elif ptype == "status" and prop.get("status"):
            texts.append(prop["status"].get("name", ""))

        elif ptype == "formula" and prop.get("formula", {}).get("type") == "string":
            s = prop["formula"].get("string")
            if s:
                texts.append(s)

        elif ptype == "rollup":
            r = prop.get("rollup", {})
            if r.get("type") == "array":
                for item in r.get("array", []):
                    if "title" in item:
                        texts.append(" ".join(t.get("plain_text", "") for t in item["title"]))
                    if "rich_text" in item:
                        texts.append(" ".join(t.get("plain_text", "") for t in item["rich_text"]))

        elif ptype == "people":
            for u in prop.get("people", []):
                texts.append(u.get("name", ""))

        elif ptype in ("number", "url", "email", "phone"):
            val = prop.get(ptype)
            if val:
                texts.append(str(val))

    return " ".join(texts).strip()


def detect_lang(text):
    try:
        return detect(text)
    except:
        return "unknown"


def count_words(text):
    return len(re.findall(r"\b\w+\b", text))


# -----------------------------
# VARIANT C STRICT
# -----------------------------
def is_descendant_strict(page_id, root_id):
    """
    ONLY allow strict chain:
    page → page → page → ROOT
    No databases, no blocks, no synced, no linked pages.
    """
    current = page_id
    visited = set()

    while True:
        try:
            page = notion.pages.retrieve(page_id=current)
        except Exception:
            return False

        parent = page.get("parent", {})
        ptype = parent.get("type")

        if ptype == "page_id":
            pid = normalize_id(parent.get("page_id"))
            if pid == root_id:
                return True
            if pid in visited:
                return False
            visited.add(pid)
            current = pid
            continue

        # ANY other parent == invalid
        return False


# -----------------------------
# ANALYZE PAGES
# -----------------------------
def analyze_page(page_id):
    ru = 0
    en = 0
    unreadable = False

    # properties
    try:
        page = notion.pages.retrieve(page_id=page_id)
        props_text = extract_text_from_properties(page.get("properties", {}))
        if props_text:
            lang = detect_lang(props_text)
            words = count_words(props_text)
            if lang == "ru":
                ru += words
            elif lang == "en":
                en += words
    except:
        props_text = ""

    # blocks
    try:
        blocks = notion.blocks.children.list(page_id).get("results", [])
    except:
        blocks = []

    if not props_text and not blocks:
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


# -----------------------------
# MAIN
# -----------------------------
def main():
    start = time.time()

    print("Fetching all pages...")
    pages = get_all_pages()
    print(f"Found {len(pages)} pages in workspace")

    selected = []
    for p in pages:
        pid = normalize_id(p["id"])

        if pid == ROOT_PAGE_ID:
            selected.append(p)
            continue

        if is_descendant_strict(pid, ROOT_PAGE_ID):
            selected.append(p)

    print(f"Selected {len(selected)} strict descendant pages")

    results = []
    unreadable_pages = []

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
                    user_data = notion.users.retrieve(user_id=uid)
                    author = user_data.get("name")
                except:
                    author = None
        if not author:
            author = "(unknown)"

        ru, en, unreadable = analyze_page(pid)

        if unreadable:
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

    # sort
    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    # save CSV
    fname = "notion_language_percentages.csv"
    with open(fname, "w", encoding="utf-8", newline=""):
        writer = csv.DictWriter(
            f,
            fieldnames=["Page Title", "Page URL", "Author", "% Russian", "% English"]
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"Saved {len(results)} rows to {fname}")

    if unreadable_pages:
        print("\nUnreadable pages (likely missing integration permissions):")
        for title, url in unreadable_pages:
            print(f" - {title}: {url}")

    print(f"\nDone in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
