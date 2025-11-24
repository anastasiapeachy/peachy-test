import os
import csv
import re
import time
from datetime import datetime, timezone, timedelta

from notion_client import Client
from notion_client.errors import APIResponseError
from langdetect import detect

# ==========================================
# ENV
# ==========================================

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN is not set")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID is not set")

notion = Client(auth=NOTION_TOKEN)

# если понадобится по времени фильтровать (пока не используем)
NOW_UTC = datetime.now(timezone.utc)


# ==========================================
# SAFE REQUEST (rate limits + 5xx)
# ==========================================

def safe_request(func, *args, **kwargs):
    """
    Обёртка над Notion API с ретраями при 429 и 5xx.
    """
    max_retries = 8
    base_delay = 0.3      # пауза перед каждым запросом
    backoff = 1.0         # экспоненциальный бэкофф для 5xx

    last_exc = None

    for attempt in range(max_retries):
        try:
            time.sleep(base_delay)
            return func(*args, **kwargs)
        except APIResponseError as e:
            last_exc = e
            status = e.status

            # 429 — перегрузка
            if status == 429:
                retry_after = 1
                # в e.headers бывает Retry-After
                headers = getattr(e, "headers", {}) or {}
                try:
                    retry_after = int(headers.get("Retry-After", 1))
                except Exception:
                    retry_after = 1
                print(f"[429] Rate limited. Sleeping {retry_after}s...")
                time.sleep(retry_after)
                continue

            # 5xx — временные проблемы на стороне Notion
            if 500 <= status <= 599:
                print(f"[{status}] Server error. Retry in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue

            # любые другие ошибки — пробрасываем
            raise

    raise RuntimeError(f"Notion API not responding after retries: {last_exc}")


# ==========================================
# HELPERS
# ==========================================

def normalize_id(raw_id: str) -> str:
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip()
    match = re.search(r"([0-9a-fA-F]{32})", s.replace("-", ""))
    if match:
        return match.group(1)
    return s.replace("-", "")


ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)


def notion_url(page_id: str) -> str:
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_page_info(page_id: str):
    """
    Берём title и url страницы.
    """
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    title = "Untitled"
    props = page.get("properties", {}) or {}
    for prop in props.values():
        if prop.get("type") == "title" and prop.get("title"):
            title = "".join(t.get("plain_text", "") for t in prop["title"])
            break

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
    }


def get_block_children(block_id: str):
    """
    Получаем ВСЕ дочерние блоки, с пагинацией, с safe_request.
    """
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

        # небольшая пауза между страницами результатов
        time.sleep(0.1)

    return blocks


def get_database_pages(database_id: str):
    """
    Получаем все страницы в child_database.
    """
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

        time.sleep(0.1)

    return pages


def is_empty_page(page_id: str) -> bool:
    """
    Страница из базы, у которой при открытии НЕТ контента (children),
    считается пустой и в отчёт не попадает.
    """
    try:
        children = get_block_children(page_id)
        return len(children) == 0
    except Exception:
        return False


# ==========================================
# ПОЛНЫЙ ОБХОД ВСЕХ СТРАНИЦ ПОД ROOT_PAGE_ID
# (child_page + child_database + вложенность)
# ==========================================

def get_all_pages(root_block_id: str):
    """
    Возвращает список dict:
    {
      "id": page_id,
      "title": title,
      "url": url
    }
    для всех страниц и страниц из баз внутри root.
    """
    pages = []
    seen = set()

    def _walk(block_id: str):
        children = get_block_children(block_id)

        for block in children:
            btype = block.get("type")
            bid = block.get("id")

            # -------------------------
            # child_page
            # -------------------------
            if btype == "child_page":
                pid = bid
                if pid in seen:
                    continue
                seen.add(pid)

                try:
                    info = get_page_info(pid)
                    pages.append(info)
                    _walk(pid)  # рекурсивно внутрь страницы
                except Exception as e:
                    print(f"Skip child_page {pid}: {e}")

            # -------------------------
            # child_database
            # -------------------------
            elif btype == "child_database":
                db_id = bid
                try:
                    db_pages = get_database_pages(db_id)
                    for db_page in db_pages:
                        pid = db_page["id"]
                        if pid in seen:
                            continue

                        # пропускаем пустые "строки" из базы
                        try:
                            if is_empty_page(pid):
                                print(f"Skip empty database page: {pid}")
                                continue
                        except Exception:
                            pass

                        seen.add(pid)
                        try:
                            info = get_page_info(pid)
                            pages.append(info)
                            _walk(pid)
                        except Exception as e:
                            print(f"Skip db page {pid}: {e}")
                except Exception as e:
                    print(f"Skip child_database {db_id}: {e}")

            # -------------------------
            # любой другой блок с детьми
            # -------------------------
            if block.get("has_children") and btype not in ("child_page", "child_database"):
                try:
                    _walk(bid)
                except Exception as e:
                    print(f"Skip nested block {bid}: {e}")

    _walk(root_block_id)
    return pages


# ==========================================
# LANGUAGE HELPERS
# ==========================================

def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text, flags=re.UNICODE))


def guess_lang_by_charset(text: str) -> str:
    cyr = len(re.findall(r"[А-Яа-яЁё]", text))
    lat = len(re.findall(r"[A-Za-z]", text))
    if cyr == 0 and lat == 0:
        return "unknown"
    return "ru" if cyr >= lat else "en"


def detect_lang_safe(text: str) -> str:
    """
    Детект языка по блоку:
    - для коротких текстов → по алфавиту (кириллица/латиница)
    - для нормальных → langdetect, но fallback в guess_lang_by_charset
    """
    if not text:
        return "unknown"

    txt = text.strip()
    if not txt:
        return "unknown"

    # очень короткие фрагменты — сразу по алфавиту
    words = txt.split()
    if len(txt) < 20 or len(words) < 3:
        return guess_lang_by_charset(txt)

    try:
        lang = detect(txt)
    except Exception:
        return guess_lang_by_charset(txt)

    if lang not in ("ru", "en"):
        return guess_lang_by_charset(txt)

    return lang


# ==========================================
# TEXT EXTRACTION (ТОЧНЫЙ, КАК В ДЕБАГЕ)
# ==========================================

def extract_rich_text(rt_list):
    collected = []
    for rt in rt_list or []:
        if not isinstance(rt, dict):
            continue

        # обычный случай
        if "plain_text" in rt and rt["plain_text"]:
            collected.append(rt["plain_text"])

        # ссылки / href — текст уже в plain_text, отдельно URL не считаем
        # mentions оставляем как есть, они тоже в plain_text

    return " ".join(collected)


def extract_all_text_from_block(block):
    """
    Полный рекурсивный сбор текста из блока.
    Учитываем:
    - paragraph / heading / list items / callout / toggle / to_do
    - code
    - caption
    - equation
    - table
    - column_list / column (правильный обход)
    """
    texts = []

    btype = block.get("type")
    data = block.get(btype, {}) if btype else {}

    # --------- rich-text контейнеры ----------
    rich_containers = [
        "paragraph", "heading_1", "heading_2", "heading_3", "heading_4", "heading_5", "heading_6",
        "quote", "callout", "bulleted_list_item", "numbered_list_item",
        "toggle", "to_do",
    ]
    if btype in rich_containers:
        rt = data.get("rich_text", [])
        texts.append(extract_rich_text(rt))

    # --------- code ----------
    if btype == "code":
        rt = data.get("rich_text", [])
        texts.append(extract_rich_text(rt))

    # --------- caption (image/file/video) ----------
    cap = data.get("caption")
    if cap:
        texts.append(extract_rich_text(cap))

    # --------- equation ----------
    if btype == "equation" and isinstance(data, dict):
        expr = data.get("expression")
        if expr:
            texts.append(expr)

    # --------- SPECIAL: column_list / column ----------
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

        # ВАЖНО: для колонок выходим здесь, чтобы не продублировать детей ниже
        return " ".join(t for t in texts if t).strip()

    # --------- table ----------
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

    # --------- общая рекурсия по детям ----------
    if block.get("has_children"):
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


# ==========================================
# PAGE ANALYSIS
# ==========================================

def analyze_page(page_id: str):
    """
    Возвращает (ru_words, en_words).
    Анализируем блоки отдельными кусками, как в дебаг-скрипте.
    """
    ru = 0
    en = 0

    blocks = get_block_children(page_id)
    if not blocks:
        return ru, en

    for block in blocks:
        btype = block.get("type")

        # пропускаем child_page / child_database — они считаются как отдельные страницы
        if btype in ("child_page", "child_database"):
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
        # остальные языки нам не интересны — игнорируем

    return ru, en


# ==========================================
# MAIN
# ==========================================

def main():
    start = time.time()

    print("Scanning Notion tree from ROOT_PAGE_ID...")
    pages = get_all_pages(ROOT_PAGE_ID)
    print(f"Discovered pages (including DB rows): {len(pages)}")

    # на всякий случай ещё раз уберём дубликаты
    unique = {}
    for p in pages:
        pid = normalize_id(p["id"])
        if pid not in unique:
            unique[pid] = {
                "id": pid,
                "title": p["title"],
                "url": p["url"],
            }

    pages_list = list(unique.values())
    print(f"Unique pages to analyze: {len(pages_list)}\n")

    results = []

    for idx, p in enumerate(pages_list, start=1):
        pid = p["id"]
        title = p["title"]
        url = p["url"]

        print(f"[{idx}/{len(pages_list)}] Analyzing: {title}")

        # автор
        author = "(unknown)"
        try:
            page_obj = safe_request(notion.pages.retrieve, page_id=pid)
            author_info = page_obj.get("created_by", {}) or {}
            author = author_info.get("name")
            if not author:
                uid = author_info.get("id")
                if uid:
                    try:
                        user = safe_request(notion.users.retrieve, user_id=uid)
                        author = user.get("name") or "(unknown)"
                    except Exception:
                        author = "(unknown)"
        except Exception:
            author = "(unknown)"

        ru, en = analyze_page(pid)
        total_words = ru + en

        ru_pct = round(ru / total_words * 100, 2) if total_words else 0.0
        en_pct = round(en / total_words * 100, 2) if total_words else 0.0

        results.append({
            "Page Title": title,
            "Page URL": url,
            "Author": author,
            "% Russian": ru_pct,
            "% English": en_pct,
        })

        # лёгкая пауза между страницами сверху к safe_request
        time.sleep(0.1)

    # сортируем: сначала больше английского, потом русского
    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    if results:
        fname = "notion_language_percentages.csv"
        with open(fname, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["Page Title", "Page URL", "Author", "% Russian", "% English"],
            )
            writer.writeheader()
            writer.writerows(results)

        print(f"\nSaved {len(results)} rows → {fname}")
    else:
        print("\nNo pages to save (results list is empty).")

    print(f"Done in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
