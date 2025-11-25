import os
import csv
import re
import time
from datetime import datetime, timezone, timedelta
from notion_client import Client
from notion_client.errors import APIResponseError, HTTPResponseError
from langdetect import detect

# =====================================
# ENV
# =====================================

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN is not set")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID is not set")

notion = Client(auth=NOTION_TOKEN)

# =====================================
# SAFE REQUEST (Rate limits + 5xx)
# =====================================

def safe_request(func, *args, **kwargs):
    max_retries = 7
    delay = 1  # базовый бэкофф

    for attempt in range(max_retries):
        try:
            res = func(*args, **kwargs)
            # небольшая пауза после УСПЕШНОГО запроса, чтобы не спамить API
            time.sleep(0.2)
            return res

        except APIResponseError as e:
            status = getattr(e, "status", None)
            code = getattr(e, "code", None)

            # rate limit — и по статусу, и по коду
            if status == 429 or code == "rate_limited":
                retry_after = getattr(e, "headers", {}).get("Retry-After")
                if retry_after is not None:
                    try:
                        wait = int(retry_after)
                    except ValueError:
                        wait = 2
                else:
                    wait = (attempt + 1) * 2
                print(f"[rate limit] waiting {wait}s… (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                continue

            # серверные ошибки 5xx
            if status and 500 <= status <= 599:
                print(f"[{status}] server error, retry in {delay}s…")
                time.sleep(delay)
                delay = min(delay * 2, 10)
                continue

            # остальные ошибки — пробрасываем
            raise

        except HTTPResponseError:
            # например 502 Bad Gateway
            print(f"[HTTP error] retry in {delay}s…")
            time.sleep(delay)
            delay = min(delay * 2, 10)

    raise RuntimeError("Notion API not responding after retries")


# =====================================
# HELPERS
# =====================================

def normalize_id(raw_id: str) -> str:
    """Очищаем id от дефисов, оставляем 32 hex-символа."""
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip().replace("-", "")
    m = re.search(r"([0-9a-fA-F]{32})", s)
    return m.group(1) if m else s

ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)

def make_url(page_id: str) -> str:
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"

def get_page(page_id):
    return safe_request(notion.pages.retrieve, page_id=page_id)

def get_page_title(page) -> str:
    props = page.get("properties", {}) or {}
    for prop in props.values():
        if prop.get("type") == "title":
            parts = [t.get("plain_text", "") for t in prop.get("title", [])]
            if parts:
                return "".join(parts)
    return "(untitled)"

def get_children(block_id):
    """Все непосредственные дети блока/страницы (1 уровень)."""
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


# =====================================
# РЕКУРСИВНЫЙ СБОР ВСЕХ БЛОКОВ (как в удачном дебаге)
# =====================================

def get_blocks_recursive(block_id):
    """
    Забираем ВСЕ блоки внутри страницы/блока (включая колонки, таблицы, toggles и т.п.).
    Каждый блок через children.list запрашивается ровно один раз.
    """
    blocks = []
    cursor = None

    while True:
        resp = safe_request(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor
        )

        for block in resp.get("results", []):
            blocks.append(block)
            if block.get("has_children"):
                blocks.extend(get_blocks_recursive(block["id"]))

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return blocks


# =====================================
# ТЕКСТ И ЯЗЫК (точная версия)
# =====================================

def extract_rich_text(rt_list):
    pieces = []
    for rt in rt_list:
        if not isinstance(rt, dict):
            continue
        txt = rt.get("plain_text")
        if txt:
            pieces.append(txt)
    return " ".join(pieces)

def block_own_text(block) -> str:
    """
    ТОЛЬКО текст самого блока, без детей.
    Именно так мы считали в «правильно работающем» дебаге.
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

    if btype in rich_containers:
        rt = data.get("rich_text", [])
        texts.append(extract_rich_text(rt))

    if btype == "code":
        rt = data.get("rich_text", [])
        texts.append(extract_rich_text(rt))

    if isinstance(data, dict) and "caption" in data:
        cap = data.get("caption", [])
        texts.append(extract_rich_text(cap))

    if btype == "equation":
        expr = data.get("expression")
        if expr:
            texts.append(expr)

    if btype == "table_row":
        cells = data.get("cells", [])
        for cell in cells:
            texts.append(extract_rich_text(cell))

    # column / column_list / synced_block — текста нет, только вложенные дети

    return " ".join(t for t in texts if t).strip()

def detect_lang(text: str) -> str:
    try:
        return detect(text)
    except Exception:
        return "unknown"

def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))


# =====================================
# ПРОВЕРКА, ПУСТА ЛИ СТРАНИЦА (только title)
# =====================================

def is_empty_page(page_id: str) -> bool:
    """
    «Пустая» страница = у неё НЕТ ни одного блока (детей).
    Это как раз кейс: строку добавили в базу, но контента ещё нет.
    """
    try:
        children = get_children(page_id)
        return len(children) == 0
    except Exception:
        # если не смогли прочитать — считаем НЕ пустой, чтобы не потерять
        return False


# =====================================
# СБОР ВСЕХ СТРАНИЦ ПОД ROOT_PAGE_ID
# =====================================

def collect_all_pages(root_id: str):
    pages = []
    children = get_children(root_id)

    for block in children:
        btype = block.get("type")

        # --- обычная подстраница ---
        if btype == "child_page":
            pid = normalize_id(block["id"])
            pages.append(pid)
            pages.extend(collect_all_pages(pid))

        # --- база данных ---
        elif btype == "child_database":
            dbid = block["id"]
            cursor = None

        def query_db(dbid, cursor):
            return notion.databases.query(database_id=dbid, start_cursor=cursor)

        while True:
            resp = safe_request(query_db, dbid, cursor)

            for row in resp.get("results", []):
                pid = row["id"]
                if not is_empty_content(pid):
                    pages.append(pid)
                    pages.extend(collect_all_pages(pid))

            cursor = resp.get("next_cursor")
            if not cursor:
                break

        # --- вложенные блоки (колонки, toggles, synced и т.п.) ---
        if block.get("has_children") and btype not in ("child_page", "child_database"):
            pages.extend(collect_all_pages(block["id"]))

    return pages


# =====================================
# АНАЛИЗ ОДНОЙ СТРАНИЦЫ (как в «удачном» коде)
# =====================================

def analyze_page_language(page_id: str):
    ru = 0
    en = 0

    blocks = get_blocks_recursive(page_id)
    if not blocks:
        # нет блоков → вероятно «только title»
        return ru, en, True

    for block in blocks:
        # child_page — это другая отдельная страница, её мы анализируем отдельно
        if block.get("type") == "child_page":
            continue

        text = block_own_text(block)
        if not text.strip():
            continue

        lang = detect_lang(text)
        words = count_words(text)

        if lang == "ru":
            ru += words
        elif lang == "en":
            en += words

    unreadable = (ru + en == 0)
    return ru, en, unreadable


# =====================================
# MAIN
# =====================================

def main():
    start = time.time()
    print("Collecting all pages under ROOT_PAGE_ID…")

    page_ids = collect_all_pages(ROOT_PAGE_ID)
    # dedupe + сохранение порядка
    page_ids = list(dict.fromkeys(page_ids))

    print(f"Total pages discovered under root: {len(page_ids)}")

    results = []
    unreadable_pages = []

    for pid in page_ids:
        # ещё раз отсекаем «совсем пустые» страницы
        if is_empty_page(pid):
            continue

        try:
            page = get_page(pid)
        except Exception as e:
            print(f"Skip page {pid} (cannot retrieve): {e}")
            continue

        title = get_page_title(page)
        url = make_url(pid)

        # автор
        author_info = page.get("created_by", {}) or {}
        author = author_info.get("name")
        if not author:
            uid = author_info.get("id")
            if uid:
                try:
                    user = safe_request(notion.users.retrieve, user_id=uid)
                    author = user.get("name")
                except Exception:
                    author = None
        if not author:
            author = "(unknown)"

        ru, en, unreadable = analyze_page_language(pid)

        if unreadable:
            print(f"⚠ Cannot reliably read page: {title} — {url}")
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

    # сортируем по английскому, потом по русскому
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

    if unreadable_pages:
        print("\n⚠ Pages that API could NOT read (ru+en=0, но не пустые):")
        for t, u in unreadable_pages:
            print(f" - {t}: {u}")

    print(f"\nDone in {time.time() - start:.1f}s")

if __name__ == "__main__":
    main()
