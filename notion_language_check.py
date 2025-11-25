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

# базовая задержка после каждого успешного запроса
API_DELAY = 0.3

# глобальные кэши
page_cache = {}
user_cache = {}

# ===========================================================
# SAFE NOTION CALL WRAPPER
# ===========================================================

def notion_call(func, **kwargs):
    """
    Безопасный вызов Notion API с обработкой rate limits:
    - учитывает Retry-After, если есть
    - использует экспоненциальную паузу
    - после каждого удачного запроса делает небольшую задержку
    """
    base_delay = API_DELAY
    max_retries = 7

    for attempt in range(max_retries):
        try:
            res = func(**kwargs)
            time.sleep(base_delay)
            return res

        except APIResponseError as e:
            if e.code == "rate_limited":
                retry_after = None
                # иногда Notion отдаёт заголовок Retry-After
                try:
                    retry_after = e.response.headers.get("Retry-After")
                except Exception:
                    retry_after = None

                if retry_after:
                    wait = float(retry_after)
                else:
                    # экспоненциальная задержка: 2, 4, 8, 10, 10...
                    wait = min(2 ** (attempt + 1), 10)

                print(f"[rate limit] waiting {wait}s (attempt {attempt+1}/{max_retries})...")
                time.sleep(wait)
                continue

            # другие ошибки не трогаем
            raise

    raise Exception("Too many retries — still rate limited")


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


def get_page(pid):
    """Получаем страницу из кэша или через Notion API."""
    pid_norm = normalize_id(pid)
    if pid_norm not in page_cache:
        page_cache[pid_norm] = notion_call(
            notion.pages.retrieve,
            page_id=pid_norm
        )
    return page_cache[pid_norm]


def get_user(uid):
    """Получаем пользователя из кэша или через Notion API."""
    if not uid:
        return None
    if uid not in user_cache:
        try:
            user_cache[uid] = notion_call(
                notion.users.retrieve,
                user_id=uid
            )
        except Exception:
            user_cache[uid] = None
    return user_cache[uid]


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

    # fallback через блок child_page (на случай, если properties пустые)
    try:
        blk = notion_call(notion.blocks.retrieve, block_id=page["id"])
        if blk.get("type") == "child_page":
            return blk["child_page"].get("title", "(untitled)")
    except Exception:
        pass

    return "(untitled)"


# ===========================================================
# Recursive blocks fetch (с page_size и без повторных запросов)
# ===========================================================

def get_blocks_recursive(block_id):
    """
    Рекурсивно забираем ВСЕ блоки страницы (включая вложенные),
    при этом каждый блок запрашиваем через children.list только один раз.
    """
    blocks = []
    cursor = None

    while True:
        resp = notion_call(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor,
            page_size=100  # максимум за раз
        )

        for block in resp.get("results", []):
            blocks.append(block)

            # если у блока есть дети — рекурсивно обрабатываем их
            if block.get("has_children"):
                blocks.extend(get_blocks_recursive(block["id"]))

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return blocks


# ===========================================================
# Text extraction per block
# ===========================================================

def extract_rich_text(rt_list):
    pieces = []
    for rt in rt_list:
        if not isinstance(rt, dict):
            continue
        if "plain_text" in rt and rt["plain_text"]:
            pieces.append(rt["plain_text"])
    return " ".join(pieces)


def block_own_text(block):
    """
    Берём ТОЛЬКО текст САМОГО блока, без детей.
    Дети обрабатываются отдельно через get_blocks_recursive.
    """
    texts = []
    btype = block.get("type")
    data = block.get(btype, {}) if btype else {}

    rich_containers = [
        "paragraph", "heading_1", "heading_2", "heading_3",
        "heading_4", "heading_5", "heading_6",
        "quote", "callout", "bulleted_list_item",
        "numbered_list_item", "toggle", "to_do"
    ]

    # обычные текстовые блоки
    if btype in rich_containers:
        rt = data.get("rich_text", [])
        texts.append(extract_rich_text(rt))

    # code
    if btype == "code":
        rt = data.get("rich_text", [])
        texts.append(extract_rich_text(rt))

    # caption (image, file, video, etc.)
    if isinstance(data, dict) and "caption" in data:
        cap = data.get("caption", [])
        texts.append(extract_rich_text(cap))

    # equation
    if btype == "equation":
        expr = data.get("expression")
        if expr:
            texts.append(expr)

    # table_row — берём текст из ячеек
    if btype == "table_row":
        cells = data.get("cells", [])
        for cell in cells:
            texts.append(extract_rich_text(cell))

    # column / column_list / synced_block — сами по себе текста не несут

    return " ".join(t for t in texts if t).strip()


# ===========================================================
# Parent resolution
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
        parent = blk.get("parent", {}) or {}
        ptype = parent.get("type")
        if ptype == "page_id":
            return normalize_id(parent.get("page_id"))
        if ptype == "block_id":
            block_id = parent.get("block_id")
            continue
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
            try:
                current = page_index.get(pid) or get_page(pid)
            except Exception:
                return False
            continue

        elif ptype == "block_id":
            bid = parent.get("block_id")
            resolved = resolve_block_parent_to_page(bid)
            if resolved == root_id:
                return True
            if not resolved or resolved in visited:
                return False
            visited.add(resolved)
            try:
                current = page_index.get(resolved) or get_page(resolved)
            except Exception:
                return False
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
    return len(re.findall(r"\b\w+\b", text))


# ===========================================================
# Analyze page
# ===========================================================

def analyze_page(page_id):
    ru = 0
    en = 0

    blocks = get_blocks_recursive(page_id)

    if not blocks:
        return ru, en, True, False  # ничего не прочитали

    has_content = False
    for block in blocks:
        # не лезем в child_page (другие статьи)
        if block.get("type") == "child_page":
            continue

        text = block_own_text(block)
        if not text.strip():
            continue

        has_content = True
        lang = detect_lang(text)
        words = count_words(text)

        if lang == "ru":
            ru += words
        elif lang == "en":
            en += words

    unreadable = (ru + en == 0)
    return ru, en, unreadable, has_content


# ===========================================================
# MAIN WITH BATCHING
# ===========================================================

def main():
    start = time.time()
    unreadable_pages = []
    title_only_pages = []

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
            print(f"Skip page {pid} in is_child_of_root: {e}")

    print(f"Total pages under root: {len(selected)}")

    batch_size = 20
    batches = [selected[i:i + batch_size] for i in range(0, len(selected), batch_size)]
    print(f"Total batches: {len(batches)}, batch size: {batch_size}")

    results = []

    for bi, batch in enumerate(batches, start=1):
        print(f"\nProcessing batch {bi}/{len(batches)} (size={len(batch)})")

        for p in batch:
            pid = normalize_id(p["id"])

            # нормальный объект страницы (через индекс или кэш)
            try:
                page = page_index.get(pid) or get_page(pid)
            except APIResponseError as e:
                print(f"Skip page {pid}: {e}")
                continue
            except Exception as e:
                print(f"Skip page {pid}: {e}")
                continue

            title = get_title(page)
            url = make_url(pid)

            # автор
            author_info = page.get("created_by", {}) or {}
            author = author_info.get("name")
            if not author:
                uid = author_info.get("id")
                user = get_user(uid)
                if user:
                    author = user.get("name")
            if not author:
                author = "(unknown)"

            # анализ текста
            ru, en, unreadable, has_content = analyze_page(pid)

            # Пропускаем страницы только с заголовком (без контента)
            if not has_content:
                print(f"⊘ Skipping title-only page: {title} — {url}")
                title_only_pages.append((title, url))
                continue

            if unreadable:
                print(f"⚠ Cannot read page: {title} — {url}")
                unreadable_pages.append((title, url))

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

        # пауза между батчами, чтобы Notion отдохнул
        time.sleep(1.0)

    # сортировка: по английскому, потом по русскому
    if results:
        results.sort(
            key=lambda x: (x["% English"], x["% Russian"]),
            reverse=True
        )

    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["Page Title", "Page URL", "Author", "% Russian", "% English"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(row)

    print(f"\nSaved {len(results)} rows → {fname}")

    if title_only_pages:
        print(f"\n⊘ Skipped {len(title_only_pages)} title-only pages (not included in CSV)")

    if unreadable_pages:
        print("\n⚠ Pages that API could NOT read:")
        for t, u in unreadable_pages:
            print(f" - {t}: {u}")

    print(f"\nDone in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
