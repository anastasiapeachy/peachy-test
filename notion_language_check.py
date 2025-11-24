import os
import csv
import re
import time
from notion_client import Client
from notion_client.errors import APIResponseError
from langdetect import detect

# ======================================================
# ENV
# ======================================================
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN is not set")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID is not set")

notion = Client(auth=NOTION_TOKEN)


# ======================================================
# SAFE REQUEST (429 / 5xx retry)
# ======================================================
def safe_request(func, *args, **kwargs):
    max_retries = 8
    delay = 0.3
    backoff = 1

    for attempt in range(max_retries):
        try:
            time.sleep(delay)
            return func(*args, **kwargs)
        except APIResponseError as e:
            status = e.status

            # Rate limit
            if status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limit exceeded. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue

            # Server errors
            if 500 <= status <= 599:
                print(f"[{status}] Server error. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue

            # Other API errors — пробрасываем
            raise

    raise RuntimeError("Notion API not responding after retries")


# ======================================================
# HELPERS
# ======================================================
def notion_url(page_id: str) -> str:
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_block_children(block_id: str):
    """Безопасно получаем всех детей блока (с пагинацией)."""
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
    """Все страницы внутри child_database."""
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


def get_page_info(page_id: str):
    """title + url + author (без лишних запросов)."""
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    # Title
    title = "Untitled"
    if "properties" in page:
        for prop in page["properties"].values():
            if prop.get("type") == "title" and prop.get("title"):
                title = "".join(t.get("plain_text", "") for t in prop["title"])
                break

    # Author (по возможности — без отдельного users.retrieve)
    author = "(unknown)"
    created_by = page.get("created_by") or {}
    if isinstance(created_by, dict):
        author = created_by.get("name") or "(unknown)"

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "author": author,
    }


# ======================================================
# EMPTY PAGE CHECK
# ======================================================
def is_empty_page(page_id: str) -> bool:
    """
    Пустая страница — если у неё нет ни одного блока-контента.
    Используется для:
    - строк из database без контента (только title),
    - обычных пустых страниц.
    """
    try:
        children = get_block_children(page_id)
        return len(children) == 0
    except Exception:
        # Если что-то пошло не так, лучше считать, что страница НЕ пустая,
        # чтобы случайно не выкинуть нужный контент.
        return False


# ======================================================
# FULL RECURSIVE SCAN from ROOT_PAGE_ID
# ======================================================
def get_all_pages(root_page_id: str):
    """
    Рекурсивно обходим всё под ROOT_PAGE_ID:
    - child_page
    - child_database + их страницы
    - child_page/child_database внутри колонок/блоков
    Пустые страницы (только title, без блоков) не добавляем.
    """
    result_pages = []

    def _walk_blocks(block_id: str):
        blocks = get_block_children(block_id)

        for block in blocks:
            btype = block.get("type")

            # ---------- child_page ----------
            if btype == "child_page":
                pid = block["id"]
                try:
                    if is_empty_page(pid):
                        print(f"Skip empty child_page: {pid}")
                    else:
                        info = get_page_info(pid)
                        result_pages.append(info)
                        _walk_blocks(pid)
                except Exception as e:
                    print(f"Skip child_page {pid}: {e}")

            # ---------- child_database ----------
            elif btype == "child_database":
                db_id = block["id"]
                try:
                    db_pages = get_database_pages(db_id)
                    for db_page in db_pages:
                        pid = db_page["id"]

                        try:
                            if is_empty_page(pid):
                                print(f"Skip empty database page: {pid}")
                                continue
                        except Exception:
                            pass

                        try:
                            info = get_page_info(pid)
                            result_pages.append(info)
                            _walk_blocks(pid)
                        except Exception as e:
                            print(f"Skip db page {pid}: {e}")
                except Exception as e:
                    print(f"Skip child_database {db_id}: {e}")

            # ---------- nested blocks with children (columns, toggles etc.) ----------
            if block.get("has_children") and btype not in ("child_page", "child_database"):
                try:
                    _walk_blocks(block["id"])
                except Exception as e:
                    print(f"Skip nested block {block['id']}: {e}")

    # стартуем с корневой страницы
    _walk_blocks(root_page_id)
    return result_pages


# ======================================================
# LANGUAGE HELPERS
# ======================================================
def clean_for_lang(text: str) -> str:
    """Оставляем только буквы и пробелы для langdetect."""
    return "".join(ch if (ch.isalpha() or ch.isspace()) else " " for ch in text)


def detect_lang_safe(text: str) -> str:
    s = clean_for_lang(text)
    letters_only = s.replace(" ", "")

    # Очень короткие куски не детектируем
    if len(letters_only) < 30:
        return "unknown"

    try:
        return detect(s)
    except Exception:
        return "unknown"


def count_words(text: str) -> int:
    # Считаем только слова с латиницей/кириллицей
    tokens = re.findall(r"[A-Za-zА-Яа-яЁё]+", text)
    return len(tokens)


# ======================================================
# TEXT EXTRACTION (как в точном debug-скрипте)
# ======================================================
def extract_all_text_from_block(block) -> str:
    """
    Максимально полный сбор текста из блока Notion,
    включая:
    - параграфы/заголовки/списки/callout
    - code
    - caption
    - tables
    - колонки (column_list / column)
    - детей (has_children)
    """
    texts = []

    btype = block.get("type")
    data = block.get(btype, {}) if btype else {}

    def extract_rich_text(rt_list):
        collected = []
        for rt in rt_list:
            if not isinstance(rt, dict):
                continue

            if "plain_text" in rt and rt["plain_text"]:
                collected.append(rt["plain_text"])

            # mentions / href специально не удваиваем —
            # plain_text уже содержит отображаемый текст
        return " ".join(collected)

    # -------- 1. rich-text контейнеры --------
    rich_containers = [
        "paragraph",
        "heading_1", "heading_2", "heading_3", "heading_4", "heading_5", "heading_6",
        "quote", "callout",
        "bulleted_list_item", "numbered_list_item",
        "toggle", "to_do",
    ]
    if btype in rich_containers:
        rt = data.get("rich_text", [])
        texts.append(extract_rich_text(rt))

    # -------- 2. code --------
    if btype == "code":
        rt = data.get("rich_text", [])
        texts.append(extract_rich_text(rt))

    # -------- 3. caption (images/files/videos/…) --------
    cap = data.get("caption")
    if cap:
        texts.append(extract_rich_text(cap))

    # -------- 4. equations --------
    if btype == "equation" and isinstance(data, dict):
        expr = data.get("expression")
        if expr:
            texts.append(expr)

    # -------- 5. columns (column_list / column) --------
    if btype in ("column_list", "column"):
        try:
            children = get_block_children(block["id"])
            for child in children:
                texts.append(extract_all_text_from_block(child))
        except Exception:
            pass
        return " ".join(t for t in texts if t).strip()

    # -------- 6. table --------
    if btype == "table":
        try:
            rows = get_block_children(block["id"])
            for row in rows:
                if row.get("type") == "table_row":
                    cells = row["table_row"].get("cells", [])
                    for cell in cells:
                        texts.append(extract_rich_text(cell))
        except Exception:
            pass
        # у table дальше не рекурсируем — строки уже прошли
        return " ".join(t for t in texts if t).strip()

    # -------- 7. рекурсия в детей для остальных типов --------
    if block.get("has_children") and btype not in ("child_page", "child_database"):
        try:
            children = get_block_children(block["id"])
            for child in children:
                texts.append(extract_all_text_from_block(child))
        except Exception:
            pass

    return " ".join(t for t in texts if t).strip()


# ======================================================
# PAGE LANGUAGE ANALYSIS
# ======================================================
def analyze_language_for_page(page_id: str):
    """
    Возвращает:
    (ru_pct, en_pct, has_text)
    has_text = False, если вообще не нашли читаемых кусков.
    """
    blocks = get_block_children(page_id)
    if not blocks:
        return 0.0, 0.0, False

    ru_words = 0
    en_words = 0
    has_any_text = False

    for block in blocks:
        # child_page / child_database не считаем как контент
        if block.get("type") in ("child_page", "child_database"):
            continue

        text = extract_all_text_from_block(block)
        if not text or not text.strip():
            continue

        has_any_text = True
        lang = detect_lang_safe(text)
        words = count_words(text)

        if lang == "ru":
            ru_words += words
        elif lang == "en":
            en_words += words
        # другие языки — игнорируем для процентов

    if not has_any_text:
        return 0.0, 0.0, False

    total = ru_words + en_words
    if total == 0:
        # есть текст, но не ru/en → показываем 0/0
        return 0.0, 0.0, True

    ru_pct = ru_words * 100.0 / total
    en_pct = en_words * 100.0 / total
    return ru_pct, en_pct, True


# ======================================================
# MAIN
# ======================================================
def main():
    print("Scanning Notion subtree from ROOT_PAGE_ID…")
    pages_raw = get_all_pages(ROOT_PAGE_ID)
    print(f"Discovered pages (including duplicates): {len(pages_raw)}")

    # Убираем дубликаты по id
    by_id = {}
    for info in pages_raw:
        by_id[info["id"]] = info

    pages = list(by_id.values())
    pages.sort(key=lambda p: p["title"].lower())

    print(f"Unique pages to analyze: {len(pages)}")

    results = []

    for idx, info in enumerate(pages, start=1):
        pid = info["id"]
        print(f"\n[{idx}/{len(pages)}] {info['title']}")

        ru_pct, en_pct, has_text = analyze_language_for_page(pid)

        # Если вообще нет текста (после фильтрации блоков) —
        # не включаем в отчёт (как ты просила).
        if not has_text:
            print("  → no readable content, skip from report")
            continue

        print(f"  RU %: {round(ru_pct, 2)} | EN %: {round(en_pct, 2)}")

        results.append({
            "Page Title": info["title"],
            "Page URL": info["url"],
            "Author": info["author"],
            "% Russian": round(ru_pct, 2),
            "% English": round(en_pct, 2),
        })

    if not results:
        print("No pages with content found. CSV will not be created.")
        return

    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Page Title", "Page URL", "Author", "% Russian", "% English"]
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved {len(results)} rows to {fname}")


if __name__ == "__main__":
    main()
