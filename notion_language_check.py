import os
import re
import csv
import time
from datetime import datetime, timezone
from notion_client import Client
from notion_client.errors import APIResponseError

# ================================
# ENVIRONMENT
# ================================
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN is not set")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID is not set")

notion = Client(auth=NOTION_TOKEN)


# ================================
# SAFE REQUEST (429 / 5xx RETRIES)
# ================================
def safe_request(func, *args, **kwargs):
    max_retries = 8
    delay = 0.1
    backoff = 1.0

    for attempt in range(max_retries):
        try:
            # небольшой базовый sleep перед каждым запросом
            time.sleep(delay)
            return func(*args, **kwargs)
        except APIResponseError as e:
            status = e.status

            # Rate limit
            if status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limit. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue

            # Server errors
            if 500 <= status <= 599:
                print(f"[{status}] Server error. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue

            # другие ошибки — пробрасываем
            raise

    raise RuntimeError("Notion API not responding after retries")


# ================================
# HELPERS
# ================================
def notion_url(page_id: str) -> str:
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def normalize_id(raw_id: str) -> str:
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip()
    # вычищаем всё, что не 32-значный hex
    m = re.search(r"([0-9a-fA-F]{32})", s.replace("-", ""))
    if m:
        return m.group(1)
    return s.replace("-", "")


def get_page_info(page_id: str):
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    # Title
    title = "Untitled"
    props = page.get("properties", {}) or {}
    for prop in props.values():
        if prop.get("type") == "title" and prop.get("title"):
            title = prop["title"][0].get("plain_text", "Untitled")
            break

    # Author
    author = "(unknown)"
    created_by = page.get("created_by", {}) or {}
    if created_by.get("name"):
        author = created_by["name"]
    else:
        uid = created_by.get("id")
        if uid:
            try:
                user = safe_request(notion.users.retrieve, user_id=uid)
                if user.get("name"):
                    author = user["name"]
            except Exception:
                pass

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "author": author,
    }


def get_block_children(block_id: str):
    blocks = []
    cursor = None

    while True:
        resp = safe_request(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor
        )
        blocks.extend(resp.get("results", []))
        cursor = resp.get("next_cursor")
        if not cursor:
            break

    return blocks


def get_database_pages(database_id: str):
    pages = []
    cursor = None

    while True:
        resp = safe_request(
            notion.databases.query,
            database_id=database_id,
            start_cursor=cursor
        )
        pages.extend(resp.get("results", []))
        cursor = resp.get("next_cursor")
        if not cursor:
            break

    return pages


def is_empty_page(page_id: str) -> bool:
    """
    Используется для страниц, лежащих в database.
    Если у страницы нет ни одного блока — считаем её пустой и не включаем в анализ.
    """
    try:
        children = get_block_children(page_id)
        return len(children) == 0
    except Exception:
        # на всякий случай считаем, что лучше не отфильтровывать, если не уверены
        return False


# ================================
# TEXT EXTRACTION
# ================================
def extract_rich_text(rt_list):
    parts = []
    for rt in rt_list:
        if not isinstance(rt, dict):
            continue
        text = rt.get("plain_text")
        if text:
            parts.append(text)
    return " ".join(parts)


def extract_all_text_from_block(block):
    """
    Максимально полный и аккуратный сбор текста из блока,
    включая колонки, таблицы, вложенные блоки.
    """
    texts = []

    btype = block.get("type")
    data = block.get(btype, {}) if btype else {}

    # 1) Блоки с rich_text (обычные параграфы/заголовки/списки)
    rich_containers = [
        "paragraph", "heading_1", "heading_2", "heading_3",
        "heading_4", "heading_5", "heading_6",
        "quote", "callout", "bulleted_list_item",
        "numbered_list_item", "toggle", "to_do"
    ]
    if btype in rich_containers:
        rt = data.get("rich_text", [])
        texts.append(extract_rich_text(rt))

    # 2) Code block
    if btype == "code":
        rt = data.get("rich_text", [])
        texts.append(extract_rich_text(rt))

    # 3) Captions (image / file / video / embed)
    cap = data.get("caption")
    if cap:
        texts.append(extract_rich_text(cap))

    # 4) Equation
    if btype == "equation" and isinstance(data, dict):
        expr = data.get("expression")
        if expr:
            texts.append(expr)

    # 5) Колонки и списки колонок — отдельно, чтобы не дублировать детей
    if btype in ("column_list", "column"):
        try:
            cursor = None
            while True:
                resp = safe_request(
                    notion.blocks.children.list,
                    block_id=block["id"],
                    start_cursor=cursor
                )
                for child in resp.get("results", []):
                    texts.append(extract_all_text_from_block(child))
                cursor = resp.get("next_cursor")
                if not cursor:
                    break
        except Exception:
            pass
        # ВАЖНО: возвращаем здесь, чтобы не пройтись по детям второй раз
        return " ".join(t for t in texts if t).strip()

    # 6) Таблица
    if btype == "table":
        try:
            cursor = None
            while True:
                resp = safe_request(
                    notion.blocks.children.list,
                    block_id=block["id"],
                    start_cursor=cursor
                )
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

    # 7) Общая рекурсия по детям (кроме колонок, которые уже обработали)
    if block.get("has_children") and btype not in ("column_list", "column"):
        try:
            cursor = None
            while True:
                resp = safe_request(
                    notion.blocks.children.list,
                    block_id=block["id"],
                    start_cursor=cursor
                )
                for child in resp.get("results", []):
                    texts.append(extract_all_text_from_block(child))
                cursor = resp.get("next_cursor")
                if not cursor:
                    break
        except Exception:
            pass

    return " ".join(t for t in texts if t).strip()


# ================================
# LANGUAGE COUNTERS (RU / EN ПО СИМВОЛАМ)
# ================================
RU_RE = re.compile(r"[А-Яа-яЁё]")
EN_RE = re.compile(r"[A-Za-z]")


def count_ru_en_words(text: str):
    ru = 0
    en = 0

    # слова — последовательности букв/цифр/подчёркиваний
    tokens = re.findall(r"\b\w+\b", text, flags=re.UNICODE)

    for word in tokens:
        has_ru = bool(RU_RE.search(word))
        has_en = bool(EN_RE.search(word))

        if has_ru and not has_en:
            ru += 1
        elif has_en and not has_ru:
            en += 1
        # если и то и то / ни того ни того — игнорируем для простоты

    return ru, en


def analyze_page_language(page_id: str):
    """
    Возвращает (ru_words, en_words) для страницы.
    """
    ru_total = 0
    en_total = 0

    # верхнеуровневые блоки страницы
    top_blocks = get_block_children(page_id)

    for block in top_blocks:
        btype = block.get("type")

        # текст child_page / child_database не считаем здесь
        # они будут отдельными страницами в общем списке
        if btype in ("child_page", "child_database"):
            continue

        text = extract_all_text_from_block(block)
        if not text.strip():
            continue

        ru, en = count_ru_en_words(text)
        ru_total += ru
        en_total += en

    return ru_total, en_total


# ================================
# FULL RECURSIVE SCAN OF TREE UNDER ROOT
# ================================
def collect_pages(block_id: str, pages: list, seen_pages: set):
    """
    Рекурсивно собираем ВСЕ страницы и страницы из database под данным блоком.
    """
    children = get_block_children(block_id)

    for block in children:
        btype = block.get("type")

        # 1) Обычная дочерняя страница
        if btype == "child_page":
            pid = block["id"]
            if pid in seen_pages:
                continue
            try:
                info = get_page_info(pid)
                pages.append(info)
                seen_pages.add(pid)
                # рекурсивно смотрим внутрь страницы
                collect_pages(pid, pages, seen_pages)
            except Exception as e:
                print(f"Skip child_page {pid}: {e}")

        # 2) Database
        elif btype == "child_database":
            db_id = block["id"]
            try:
                db_pages = get_database_pages(db_id)
                for db_page in db_pages:
                    pid = db_page["id"]
                    if pid in seen_pages:
                        continue

                    # пропускаем пустые строки-болванки
                    try:
                        if is_empty_page(pid):
                            print(f"Skip empty database page: {pid}")
                            continue
                    except Exception:
                        pass

                    try:
                        info = get_page_info(pid)
                        pages.append(info)
                        seen_pages.add(pid)
                        # и внутри каждой страницы из базы тоже рекурсивно ищем child_page/child_database
                        collect_pages(pid, pages, seen_pages)
                    except Exception as e:
                        print(f"Skip db page {pid}: {e}")
            except Exception as e:
                print(f"Skip child_database {db_id}: {e}")

        # 3) Любые другие блоки, у которых есть дети
        elif block.get("has_children"):
            try:
                collect_pages(block["id"], pages, seen_pages)
            except Exception as e:
                print(f"Skip nested block {block['id']}: {e}")


# ================================
# MAIN
# ================================
def main():
    start = time.time()

    root_id = ROOT_PAGE_ID
    print(f"Root page: {root_id}")

    pages = []
    seen_pages = set()

    # Добавляем сам root (если он тоже статья)
    try:
        root_info = get_page_info(root_id)
        pages.append(root_info)
        seen_pages.add(root_id)
    except Exception as e:
        print(f"Cannot get root page info: {e}")

    print("Scanning Notion tree under root...")
    collect_pages(root_id, pages, seen_pages)

    print(f"Total discovered pages (including root if accessible): {len(pages)}")

    # Анализируем язык для каждой страницы
    results = []

    for idx, p in enumerate(pages, start=1):
        pid = p["id"]
        title = p["title"]
        url = p["url"]
        author = p["author"]

        print(f"[{idx}/{len(pages)}] Analyzing: {title}")

        try:
            ru, en = analyze_page_language(pid)
        except APIResponseError as e:
            print(f"  Skipped (API error {e.status}) page {pid}")
            continue
        except Exception as e:
            print(f"  Skipped (unexpected error) page {pid}: {e}")
            continue

        total = ru + en
        ru_pct = round(ru * 100.0 / total, 2) if total else 0.0
        en_pct = round(en * 100.0 / total, 2) if total else 0.0

        results.append({
            "Page Title": title,
            "Page URL": url,
            "Author": author,
            "Russian words": ru,
            "English words": en,
            "% Russian": ru_pct,
            "% English": en_pct,
        })

    # сортируем по % английского, потом по % русского
    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    if not results:
        print("No pages analyzed, nothing to save.")
        return

    fname = "notion_language_percentages.csv"
    with open(fname, "w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "Page Title", "Page URL", "Author",
            "Russian words", "English words",
            "% Russian", "% English"
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(results)

    print(f"\nSaved {len(results)} rows to {fname}")
    print(f"Done in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
