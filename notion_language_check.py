import os
import csv
import re
import time
from notion_client import Client
from langdetect import detect

# create requirements.txt if missing
if not os.path.exists("requirements.txt"):
    with open("requirements.txt", "w", encoding="utf-8") as req:
        req.write("notion-client\nlangdetect\n")

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("SLACK_WEBHOOK_URL")  # use this as root page id

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("NOTION_TOKEN or SLACK_WEBHOOK_URL (root page id) not set in env")

# normalize root id (remove hyphens if user passed dashed id or URL)
def normalize_id(maybe_id):
    if not isinstance(maybe_id, str):
        return maybe_id
    s = maybe_id.strip()
    # if it's a full URL with an id at end, try to extract hex id
    match = re.search(r"([0-9a-fA-F]{32})", s.replace("-", ""))
    if match:
        return match.group(1)
    # remove dashes
    return s.replace("-", "")

ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)

notion = Client(auth=NOTION_TOKEN)

# --- helpers ---
def get_all_pages_via_search():
    """Возвращает список page objects (полных), полученных через search (всех страниц workspace)."""
    pages = []
    next_cursor = None
    while True:
        try:
            resp = notion.search(
                query="",
                filter={"value": "page", "property": "object"},
                start_cursor=next_cursor
            )
        except Exception as e:
            print(f"Search error: {e}")
            break
        results = resp.get("results", [])
        pages.extend(results)
        if not resp.get("has_more"):
            break
        next_cursor = resp.get("next_cursor")
    return pages

def get_page_title_from_obj(page_obj):
    props = page_obj.get("properties", {}) or {}
    for prop in props.values():
        if prop.get("type") == "title":
            parts = [t.get("plain_text", "") for t in prop.get("title", [])]
            if parts:
                return "".join(parts)
    # fallback: try to retrieve as block if possible
    try:
        blk = notion.blocks.retrieve(block_id=page_obj["id"])
        if blk.get("type") == "child_page":
            return blk["child_page"].get("title", "(Без названия)")
    except Exception:
        pass
    return "(Без названия)"

def get_page_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"

def get_block_children(block_id):
    """Получаем все дочерние блоки страницы (без падения при ошибке)."""
    children = []
    next_cursor = None
    while True:
        try:
            resp = notion.blocks.children.list(block_id=block_id, start_cursor=next_cursor)
        except Exception as e:
            print(f"⚠️ Cannot get blocks for {block_id}: {e}")
            break
        children.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        next_cursor = resp.get("next_cursor")
    return children

def extract_text_from_block(block):
    text = ""
    btype = block.get("type")
    if btype and isinstance(block.get(btype), dict):
        rich = block[btype].get("rich_text", [])
        text = "".join([t.get("plain_text", "") for t in rich])
    return text.strip()

def detect_lang(text):
    try:
        return detect(text)
    except:
        return "unknown"

def count_words(text):
    return len(re.findall(r'\b\w+\b', text))

# --- core: check if page is descendant of root ---
def is_descendant(page_obj, root_id, pages_index):
    """
    Проверяет, является ли страница потомком root_id.
    Поднимаемся по parent chain. pages_index — dict id->page_obj (если есть).
    Если parent.type == 'page_id' и parent.page_id == root_id -> True.
    Иначе идём дальше вверх по родителям (через pages_index или API retrieve).
    """
    visited = set()
    current = page_obj
    while True:
        parent = current.get("parent", {}) or {}
        ptype = parent.get("type")
        # handle direct page parent
        if ptype == "page_id":
            pid = parent.get("page_id")
            if not pid:
                return False
            pid_norm = normalize_id(pid)
            if pid_norm == root_id:
                return True
            if pid_norm in visited:
                return False
            visited.add(pid_norm)
            # try to get parent page object from index, otherwise retrieve
            parent_obj = pages_index.get(pid_norm)
            if not parent_obj:
                try:
                    parent_obj = notion.pages.retrieve(page_id=pid_norm)
                except Exception:
                    return False
            current = parent_obj
            continue
        # parent could be workspace -> stop
        if ptype == "workspace":
            return False
        # parent could be database_id or block_id or something else -> in our case user said no databases,
        # but if it's database, then its parent might lead up further; try to fetch database to find its parent
        if ptype == "database_id":
            db_id = parent.get("database_id")
            if not db_id:
                return False
            # try to retrieve database to inspect its parent (rare, but safe)
            try:
                db = notion.databases.retrieve(database_id=db_id)
                db_parent = db.get("parent", {})
                if db_parent.get("type") == "page_id" and normalize_id(db_parent.get("page_id")) == root_id:
                    return True
                # if db parent is a page, set current to that page and continue loop
                if db_parent.get("type") == "page_id":
                    pid_norm = normalize_id(db_parent.get("page_id"))
                    if pid_norm in visited:
                        return False
                    visited.add(pid_norm)
                    parent_obj = pages_index.get(pid_norm) or notion.pages.retrieve(page_id=pid_norm)
                    current = parent_obj
                    continue
                return False
            except Exception:
                return False
        # other types (block_id etc.) — try to retrieve block and see its parent
        if ptype == "block_id":
            bid = parent.get("block_id")
            if not bid:
                return False
            try:
                blk = notion.blocks.retrieve(block_id=bid)
                # block object has parent too; convert to page-like object for next iteration
                blk_parent = blk.get("parent", {})
                if not blk_parent:
                    return False
                # construct a fake page_obj with parent to continue chain
                current = {"id": bid, "parent": blk_parent}
                continue
            except Exception:
                return False
        # unknown parent type — stop
        return False

# --- processing single page text ---
def analyze_page_text(page_id):
    """Собирает все текстовые блоки страницы (только этой страницы) и считает ru/en слова."""
    ru = 0
    en = 0
    blocks = get_block_children(page_id)
    for block in blocks:
        # skip child_page block content (subpage counted separately)
        if block.get("type") == "child_page":
            continue
        txt = extract_text_from_block(block)
        if not txt:
            continue
        lang = detect_lang(txt)
        words = count_words(txt)
        if lang == "ru":
            ru += words
        elif lang == "en":
            en += words
    return ru, en

# --- main ---
def main():
    start = time.time()
    pages = get_all_pages_via_search()
    print(f"Total pages discovered by search: {len(pages)}")

    # index by normalized id for fast lookup
    pages_index = {}
    for p in pages:
        pid_norm = normalize_id(p.get("id"))
        pages_index[pid_norm] = p

    selected_pages = []
    for p in pages:
        pid = normalize_id(p.get("id"))
        # include root itself
        if pid == ROOT_PAGE_ID:
            selected_pages.append(p)
            continue
        try:
            if is_descendant(p, ROOT_PAGE_ID, pages_index):
                selected_pages.append(p)
        except Exception as e:
            print(f"Error checking ancestry for {pid}: {e}")

    print(f"Pages that are descendants of root ({ROOT_PAGE_ID}): {len(selected_pages)}")

    results = []
    for p in selected_pages:
        pid = normalize_id(p.get("id"))
        title = get_page_title_from_obj(p)
        url = get_page_url(pid)
        ru_words, en_words = analyze_page_text(pid)
        total = ru_words + en_words
        ru_percent = (ru_words / total * 100) if total else 0
        en_percent = (en_words / total * 100) if total else 0
        results.append({
            "Page Title": title,
            "Page URL": url,
            "% Russian": round(ru_percent, 2),
            "% English": round(en_percent, 2)
        })

    # write csv
    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Page Title", "Page URL", "% Russian", "% English"])
        writer.writeheader()
        writer.writerows(results)

    print(f"Saved {len(results)} rows to {fname}")
    print(f"Elapsed: {time.time() - start:.1f}s")

if __name__ == "__main__":
    main()
