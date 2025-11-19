import os
import csv
import re
import time
from notion_client import Client
from langdetect import detect

# grab env vars
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID env vars")


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
    # оставляем, вдруг пригодится — но больше не используем
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
    """Fetch all child blocks of a block/page, paginated"""
    blocks = []
    cursor = None
    while True:
        try:
            resp = notion.blocks.children.list(
                block_id=block_id, start_cursor=cursor)
        except Exception as e:
            print(f"Can't get blocks for {block_id}: {e}")
            break

        blocks.extend(resp.get("results", []))

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return blocks


# ===================================================================
# FULL RECURSIVE TEXT EXTRACTOR (INCLUDING COLUMN FIX)
# ===================================================================
def extract_all_text_from_block(block):
    texts = []
    btype = block.get("type")
    content = block.get(btype, {}) if btype else {}

    # FIX: Handle column_list and column correctly
    if btype in ("column_list", "column"):
        try:
            children = notion.blocks.children.list(block["id"]).get("results", [])
            for child in children:
                texts.append(extract_all_text_from_block(child))
        except:
            pass
        return " ".join(t for t in texts if t).strip()

    # 1) rich_text
    if isinstance(content, dict) and "rich_text" in content:
        rich_text = content.get("rich_text", [])
        if rich_text:
            texts.append(" ".join(
                t.get("plain_text", "") for t in rich_text if t.get("plain_text"))
            )

    # 2) types with rich_text
    for key in [
        "paragraph", "heading_1", "heading_2", "heading_3", "quote", "callout",
        "bulleted_list_item", "numbered_list_item", "toggle", "to_do"
    ]:
        if btype == key:
            rt = block.get(key, {}).get("rich_text", [])
            if rt:
                texts.append(" ".join(
                    t.get("plain_text", "") for t in rt if t.get("plain_text"))
                )

    # 3) caption
    if isinstance(content, dict) and "caption" in content:
        cap = content.get("caption", [])
        if cap:
            texts.append(" ".join(
                t.get("plain_text", "") for t in cap if t.get("plain_text"))
            )

    # 4) equation
    if btype == "equation":
        eq = block.get("equation", {}).get("expression")
        if eq:
            texts.append(eq)

    # 5) synced block reference
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

    # 6) table rows
    if btype == "table":
        try:
            rows = notion.blocks.children.list(block["id"]).get("results", [])
            for row in rows:
                if row.get("type") == "table_row":
                    cells = row["table_row"].get("cells", [])
                    for cell in cells:
                        texts.append(" ".join(
                            t.get("plain_text", "") for t in cell if t.get("plain_text"))
                        )
        except:
            pass

    # 7) recurse deeply into children
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
                t.get("plain_text", "") for t in prop.get("title", []))
            )

        elif ptype == "rich_text":
            texts.append(" ".join(
                t.get("plain_text", "") for t in prop.get("rich_text", []))
            )

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
            f = prop.get("formula", {})
            if f.get("type") == "string" and f.get("string"):
                texts.append(f.get("string"))

        elif ptype == "rollup":
            r = prop.get("rollup", {})
            if r.get("type") == "array":
                for item in r.get("array", []):
                    if "title" in item:
                        texts.append(" ".join(
                            t.get("plain_text", "") for t in item["title"]))
                    if "rich_text" in item:
                        texts.append(" ".join(
                            t.get("plain_text", "") for t in item["rich_text"]))

        elif ptype == "people":
            for u in prop.get("people", []):
                if u.get("name"):
                    texts.append(u.get("name"))

        elif ptype in ("number", "url", "email", "phone"):
            if prop.get(ptype):
                texts.append(str(prop.get(ptype)))

    return " ".join(t for t in texts if t).strip()


def detect_lang(text):
    try:
        return detect(text)
    except:
        return "unknown"


def count_words(text):
    return len(re.findall(r'\b\w+\b', text))


def is_child_of_root(page, root_id, page_index):
    # оставляем, но больше не используем
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
            current = page_index.get(
                pid) or notion.pages.retrieve(page_id=pid)
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
                    current = page_index.get(
                        pid) or notion.pages.retrieve(page_id=pid)
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


def analyze_page(page_id):
    ru = 0
    en = 0
    unreadable = False

    # 1) properties
    props_text = ""
    try:
        page = notion.pages.retrieve(page_id=page_id)
        props = page.get("properties", {}) or {}
        if props:
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

    # 2) blocks
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


def collect_pages_from_root(root_id):
    """
    BFS from ROOT_PAGE_ID через child_page и link_to_page блоки,
    чтобы захватить все подстраницы, даже если они лежат внутри колонок.
    """
    visited = set()
    pages = {}
    queue = [root_id]

    while queue:
        current_id = queue.pop(0)
        if current_id in visited:
            continue
        visited.add(current_id)

        try:
            page = notion.pages.retrieve(page_id=current_id)
        except Exception as e:
            print(f"Cannot retrieve page {current_id}: {e}")
            continue

        pages[current_id] = page

        blocks = get_blocks(current_id)
        for block in blocks:
            btype = block.get("type")

            # настоящая подстраница
            if btype == "child_page":
                child_id = block.get("id")
                if child_id and child_id not in visited:
                    queue.append(child_id)

            # link_to_page (если используешь "Link to page" внутри root)
            elif btype == "link_to_page":
                ltp = block.get("link_to_page", {})
                if isinstance(ltp, dict) and ltp.get("type") == "page_id":
                    child_id = normalize_id(ltp.get("page_id"))
                    if child_id and child_id not in visited:
                        queue.append(child_id)

    return pages


def main():
    start = time.time()
    unreadable_pages = []

    print("Collecting pages via blocks tree from ROOT_PAGE_ID...")
    pages_map = collect_pages_from_root(ROOT_PAGE_ID)
    selected = list(pages_map.values())
    print(f"Found {len(selected)} pages under root (via child_page / link_to_page traversal)")

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
                    user_data = notion.users.retrieve(user_id=uid)
                    author = user_data.get("name")
                except:
                    author = None
        if not author:
            author = "(unknown)"

        ru, en, unreadable = analyze_page(pid)

        if unreadable:
            print(f"⚠ API cannot read page: {title} — {url}")
            print("   Reason: No properties AND no blocks. Likely not shared with integration.\n")
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

    # сортировка: сначала по % English, затем по % Russian (оба по убыванию)
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
        print("\n⚠ Pages that API could NOT read (likely missing integration permissions):")
        for title, url in unreadable_pages:
            print(f" - {title}: {url}")

    print(f"\nDone in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
