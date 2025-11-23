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
# Helpers: normalize ID
# ===========================================================

def normalize_id(raw_id: str) -> str:
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip()
    match = re.search(r"([0-9a-fA-F]{32})", s.replace("-", ""))
    if match:
        return match.group(1)
    return s.replace("-", "")


ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)


# ===========================================================
# Generic Notion call with retry (rate limit + 404)
# ===========================================================

def with_retry(api_call, *args, **kwargs):
    """
    Обертка для Notion API:
    - 429 → exponential backoff
    - 404 → вернуть None, а не падать
    """
    for attempt in range(5):
        try:
            return api_call(*args, **kwargs)
        except APIResponseError as e:
            # Rate limit
            if e.status == 429:
                wait = 2 ** attempt
                print(f"[Rate limit] {api_call.__name__}, sleep {wait}s...")
                time.sleep(wait)
                continue
            # Not found / not shared page
            if e.status == 404:
                print(f"[Not found] {api_call.__name__}: {e}")
                return None
            # other error → пробрасываем
            raise
    return None


# ===========================================================
# Fetch all pages in workspace (search API)
# ===========================================================

def get_all_pages_in_workspace():
    pages = []
    cursor = None

    while True:
        resp = with_retry(
            notion.search,
            query="",
            filter={"value": "page", "property": "object"},
            start_cursor=cursor
        )
        if resp is None:
            break

        pages.extend(resp.get("results", []))

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return pages


# ===========================================================
# Title / URL helpers
# ===========================================================

def get_title(page):
    props = page.get("properties", {}) or {}
    for prop in props.values():
        if prop.get("type") == "title":
            parts = [t.get("plain_text", "") for t in prop.get("title", [])]
            if parts:
                return "".join(parts)
    try:
        blk = with_retry(notion.blocks.retrieve, block_id=page["id"])
        if blk and blk.get("type") == "child_page":
            return blk["child_page"].get("title", "(untitled)")
    except Exception:
        pass
    return "(untitled)"


def make_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


# ===========================================================
# Blocks fetch (top-level, with pagination)
# ===========================================================

def get_blocks(block_id):
    blocks = []
    cursor = None

    while True:
        resp = with_retry(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor
        )
        if resp is None:
            break

        blocks.extend(resp.get("results", []))

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return blocks


# ===========================================================
# TEXT EXTRACTION — как в debug, но без print'ов
# ===========================================================

def extract_rich_text(rt_list):
    """Полный разбор rich_text (plain_text + mentions + href)."""
    collected = []
    for rt in rt_list:
        if not isinstance(rt, dict):
            continue

        # основной текст
        if "plain_text" in rt and rt["plain_text"]:
            collected.append(rt["plain_text"])

        # mentions — если вдруг есть читаемое имя
        if rt.get("type") == "mention":
            m = rt.get("mention", {})
            if "database" in m and m["database"].get("name"):
                collected.append(m["database"]["name"])
            if "page" in m and m["page"].get("title"):
                collected.append(m["page"]["title"])
            if "user" in m and m["user"].get("name"):
                collected.append(m["user"]["name"])

        # ссылки — берем текст ссылки
        href = rt.get("href")
        if href and rt.get("plain_text"):
            collected.append(rt["plain_text"])

    return " ".join(collected)


def extract_all_text_from_block(block):
    """
    Точный экстрактор текста из блока Notion, который мы тестировали
    на двух страницах (New Reports и MailChimp):
    - корректно обрабатывает column_list / column
    - правильно обходит children
    - учитывает code / caption / tables / callouts / list items и т.д.
    """
    texts = []

    btype = block.get("type")
    data = block.get(btype, {}) if btype else {}

    # контейнеры с rich_text
    rich_containers = [
        "paragraph", "heading_1", "heading_2", "heading_3",
        "heading_4", "heading_5", "heading_6",
        "quote", "callout", "bulleted_list_item",
        "numbered_list_item", "toggle", "to_do"
    ]
    if btype in rich_containers:
        rt = data.get("rich_text", [])
        texts.append(extract_rich_text(rt))

    # code blocks
    if btype == "code":
        rt = data.get("rich_text", [])
        texts.append(extract_rich_text(rt))

    # caption (image/file/video)
    cap = data.get("caption")
    if cap:
        texts.append(extract_rich_text(cap))

    # equation
    if btype == "equation" and "expression" in data:
        texts.append(data["expression"])

    # ----- SPECIAL: columns -----
    if btype in ("column_list", "column"):
        try:
            cursor = None
            while True:
                resp = with_retry(
                    notion.blocks.children.list,
                    block_id=block["id"],
                    start_cursor=cursor
                )
                if resp is None:
                    break
                for child in resp.get("results", []):
                    texts.append(extract_all_text_from_block(child))
                cursor = resp.get("next_cursor")
                if not cursor:
                    break
        except Exception:
            pass
        # для column / column_list выходим сразу,
        # чтобы не дублить children ниже
        return " ".join(t for t in texts if t).strip()

    # ----- tables -----
    if btype == "table":
        try:
            cursor = None
            while True:
                resp = with_retry(
                    notion.blocks.children.list,
                    block_id=block["id"],
                    start_cursor=cursor
                )
                if resp is None:
                    break
                for row in resp.get("results", []):
                    if row.get("type") == "table_row":
                        cells = row["table_row"].get("cells", [])
                        for cell in cells:
                            texts.append(extract_rich_text(cell))
                cursor = resp.get("next_cursor")
                if not cursor:
                    break
        except Exception:
            pass

    # ----- generic children recursion -----
    if block.get("has_children"):
        try:
            cursor = None
            while True:
                resp = with_retry(
                    notion.blocks.children.list,
                    block_id=block["id"],
                    start_cursor=cursor
                )
                if resp is None:
                    break
                for child in resp.get("results", []):
                    texts.append(extract_all_text_from_block(child))
                cursor = resp.get("next_cursor")
                if not cursor:
                    break
        except Exception:
            pass

    return " ".join(t for t in texts if t).strip()


# ===========================================================
# Parent resolution (чтобы находить страницы под root, в т.ч. в колонках)
# ===========================================================

def resolve_block_parent_to_page(block_id):
    visited = set()
    while True:
        if block_id in visited:
            return None
        visited.add(block_id)

        blk = with_retry(notion.blocks.retrieve, block_id=block_id)
        if blk is None:
            return None

        parent = blk.get("parent", {}) or {}
        ptype = parent.get("type")

        if ptype == "page_id":
            return normalize_id(parent.get("page_id"))

        if ptype == "block_id":
            block_id = parent.get("block_id")
            continue

        return None


def is_child_of_root(page, root_id, page_index):
    """
    Проверяем, является ли страница потомком ROOT_PAGE_ID:
    - прямой child page
    - child page внутри блока (в колонках и т.п.)
    """
    visited = set()
    current = page

    while True:
        parent = current.get("parent", {}) or {}
        ptype = parent.get("type")

        # обычный parent page
        if ptype == "page_id":
            pid = normalize_id(parent.get("page_id"))
            if pid == root_id:
                return True
            if pid in visited:
                return False
            visited.add(pid)

            parent_page = page_index.get(pid) or with_retry(
                notion.pages.retrieve,
                page_id=pid
            )
            if not parent_page:
                return False
            current = parent_page
            continue

        # parent — блок (column, toggle и т.п.)
        if ptype == "block_id":
            bid = parent.get("block_id")
            resolved = resolve_block_parent_to_page(bid)
            if not resolved:
                return False
            if resolved == root_id:
                return True
            if resolved in visited:
                return False
            visited.add(resolved)

            parent_page = page_index.get(resolved) or with_retry(
                notion.pages.retrieve,
                page_id=resolved
            )
            if not parent_page:
                return False
            current = parent_page
            continue

        # другие типы parent (database и т.п.) — не считаем
        return False


# ===========================================================
# Language helpers (из debug)
# ===========================================================

def detect_lang_safe(text: str) -> str:
    try:
        return detect(text)
    except Exception:
        return "unknown"


def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))


# ===========================================================
# Page analysis — ТОЧНЫЙ, как мы тестировали
# ===========================================================

def analyze_page(page_id):
    """
    Анализ страницы:
    - берем все top-level блоки страницы,
    - для каждого вытаскиваем весь текст (extract_all_text_from_block),
    - по каждому блоку определяем язык и считаем слова.
    """
    ru = 0
    en = 0

    blocks = get_blocks(page_id)
    if not blocks:
        return ru, en

    for block in blocks:
        # не спускаемся в child_page — они анализируются как отдельные страницы
        if block.get("type") == "child_page":
            continue

        text = extract_all_text_from_block(block)
        if not text or not text.strip():
            continue

        lang = detect_lang_safe(text)
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
    print(f"Total pages in workspace: {len(all_pages)}")

    page_index = {normalize_id(p["id"]): p for p in all_pages}

    # выбираем только root и его потомков
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
            print(f"Error while checking parent for {pid}: {e}")

    print(f"Total pages under root: {len(selected)}")

    if not selected:
        print("No pages under root, nothing to analyze.")
        return

    batch_size = 20
    batches = [selected[i:i + batch_size] for i in range(0, len(selected), batch_size)]

    results = []

    for bi, batch in enumerate(batches, start=1):
        print(f"\nProcessing batch {bi}/{len(batches)}, size={len(batch)}")

        for p in batch:
            pid = normalize_id(p["id"])

            page = page_index.get(pid) or with_retry(notion.pages.retrieve, page_id=pid)
            if not page:
                print(f"  ⚠ Skip page {pid} (not accessible)")
                continue

            title = get_title(page)
            url = make_url(pid)

            # author
            author_info = page.get("created_by", {}) or {}
            author = author_info.get("name")

            if not author:
                uid = author_info.get("id")
                if uid:
                    user = with_retry(notion.users.retrieve, user_id=uid)
                    if user:
                        author = user.get("name")

            if not author:
                author = "(unknown)"

            # language analysis
            ru, en = analyze_page(pid)
            total = ru + en
            ru_pct = (ru * 100 / total) if total else 0
            en_pct = (en * 100 / total) if total else 0

            results.append({
                "Page Title": title,
                "Page URL": url,
                "Author": author,
                "% Russian": round(ru_pct, 2),
                "% English": round(en_pct, 2),
            })

        # небольшой паузик между батчами (чуть помогает по rate limit)
        time.sleep(0.5)

    if not results:
        print("No results collected, nothing to save.")
        return

    # сортировка: сначала по English %, потом по Russian %
    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["Page Title", "Page URL", "Author", "% Russian", "% English"]
        )
        w.writeheader()
        w.writerows(results)

    print(f"\nSaved {len(results)} rows → {fname}")
    print(f"Done in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
