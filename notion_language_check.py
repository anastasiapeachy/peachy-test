import os
import csv
import re
import time
from notion_client import Client

# ================== ENV ==================

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID env vars")

# ================== NOTION CLIENT ==================

def normalize_id(raw_id: str) -> str:
    """Нормализуем page_id: выдёргиваем 32-значный hex или убираем дефисы."""
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip()
    m = re.search(r"([0-9a-fA-F]{32})", s.replace("-", ""))
    if m:
        return m.group(1)
    return s.replace("-", "")

ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)
notion = Client(auth=NOTION_TOKEN)

# ================== HELPERS ==================

CYR_RE = re.compile(r"[А-Яа-яЁё]")
LAT_RE = re.compile(r"[A-Za-z]")
WORD_RE = re.compile(r"\b\w+\b", re.UNICODE)


def notion_url(page_id: str) -> str:
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_page_meta(page_id: str) -> dict:
    """Получаем title + author для страницы."""
    page = notion.pages.retrieve(page_id=page_id)

    # title
    title = "(untitled)"
    props = page.get("properties", {}) or {}
    for prop in props.values():
        if prop.get("type") == "title":
            parts = [t.get("plain_text", "") for t in prop.get("title", [])]
            if parts:
                title = "".join(parts)
                break

    # author
    author = "(unknown)"
    author_info = page.get("created_by", {}) or {}
    if author_info.get("name"):
        author = author_info["name"]
    else:
        uid = author_info.get("id")
        if uid:
            try:
                user = notion.users.retrieve(user_id=uid)
                if user.get("name"):
                    author = user["name"]
            except Exception:
                pass

    return {
        "id": normalize_id(page_id),
        "title": title,
        "author": author,
        "url": notion_url(page_id),
        "properties": page.get("properties", {}) or {}
    }


def get_blocks(block_id: str) -> list:
    """Все дочерние блоки (только один уровень, с пагинацией)."""
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

# ================== ТЕКСТ ИЗ PROPERTIES ==================


def extract_text_from_properties(properties: dict) -> str:
    """Текст из свойств страницы (title, rich_text, select, multi_select и т.п.)."""
    texts = []
    if not isinstance(properties, dict):
        return ""

    for prop in properties.values():
        ptype = prop.get("type")

        if ptype == "title":
            vals = prop.get("title", []) or []
            texts.append(" ".join(t.get("plain_text", "") for t in vals if t.get("plain_text")))

        elif ptype == "rich_text":
            vals = prop.get("rich_text", []) or []
            texts.append(" ".join(t.get("plain_text", "") for t in vals if t.get("plain_text")))

        elif ptype == "select":
            sel = prop.get("select")
            if sel and sel.get("name"):
                texts.append(sel["name"])

        elif ptype == "multi_select":
            for item in prop.get("multi_select", []) or []:
                if item.get("name"):
                    texts.append(item["name"])

        elif ptype == "status":
            st = prop.get("status")
            if st and st.get("name"):
                texts.append(st["name"])

        elif ptype == "people":
            for u in prop.get("people", []) or []:
                if u.get("name"):
                    texts.append(u["name"])

        elif ptype in ("number", "url", "email", "phone"):
            val = prop.get(ptype)
            if val:
                texts.append(str(val))

    return " ".join(t for t in texts if t).strip()

# ================== ПОЛНЫЙ РЕКУРСИВНЫЙ ВЫТЯГИВАТЕЛЬ ТЕКСТА ==================


def extract_all_text_from_block(block: dict) -> str:
    """
    Рекурсивно вытаскиваем весь human-readable текст из блока:
    - параграфы, заголовки, списки, callout, to_do
    - caption у image/embed/файлов
    - equation
    - column_list / column (оба столбца)
    - synced_block (подтягиваем оригинальные дети)
    - таблицы (table + table_row)
    - любые дети (has_children)
    """
    texts = []
    btype = block.get("type")
    content = block.get(btype, {}) if btype else {}

    # Спец-обработка колонок: сразу углубляемся в детей
    if btype in ("column_list", "column"):
        try:
            children = notion.blocks.children.list(block["id"]).get("results", [])
            for ch in children:
                texts.append(extract_all_text_from_block(ch))
        except Exception:
            pass
        return " ".join(t for t in texts if t).strip()

    # 1) rich_text в самом типе
    if isinstance(content, dict) and "rich_text" in content:
        rich_text = content.get("rich_text", []) or []
        if rich_text:
            texts.append(" ".join(t.get("plain_text", "") for t in rich_text if t.get("plain_text")))

    # 2) типы с rich_text
    for key in [
        "paragraph", "heading_1", "heading_2", "heading_3",
        "quote", "callout", "bulleted_list_item",
        "numbered_list_item", "toggle", "to_do"
    ]:
        if btype == key:
            rt = block.get(key, {}).get("rich_text", []) or []
            if rt:
                texts.append(" ".join(t.get("plain_text", "") for t in rt if t.get("plain_text")))

    # 3) caption (image, embed, file и т.п.)
    if isinstance(content, dict) and "caption" in content:
        cap = content.get("caption", []) or []
        if cap:
            texts.append(" ".join(t.get("plain_text", "") for t in cap if t.get("plain_text")))

    # 4) equation
    if btype == "equation":
        eq = block.get("equation", {}).get("expression")
        if eq:
            texts.append(eq)

    # 5) synced_block → если ссылка на оригинал — забираем детей оригинала
    if btype == "synced_block":
        synced = block.get("synced_block", {}) or {}
        sf = synced.get("synced_from")
        if sf and isinstance(sf, dict):
            original_id = sf.get("block_id")
            if original_id:
                try:
                    children = notion.blocks.children.list(original_id).get("results", [])
                    for ch in children:
                        texts.append(extract_all_text_from_block(ch))
                except Exception:
                    pass

    # 6) table → строки и ячейки
    if btype == "table":
        try:
            rows = notion.blocks.children.list(block["id"]).get("results", [])
            for row in rows:
                if row.get("type") == "table_row":
                    cells = row["table_row"].get("cells", []) or []
                    for cell in cells:
                        if cell:
                            texts.append(" ".join(t.get("plain_text", "") for t in cell if t.get("plain_text")))
        except Exception:
            pass

    # 7) рекурсия по has_children (toggles, lists, всё остальное)
    if block.get("has_children"):
        try:
            children = notion.blocks.children.list(block["id"]).get("results", [])
            for ch in children:
                texts.append(extract_all_text_from_block(ch))
        except Exception:
            pass

    return " ".join(t for t in texts if t).strip()

# ================== ПОДСЧЁТ СЛОВ RU / EN ==================


def count_ru_en_words(text: str) -> tuple[int, int]:
    ru = 0
    en = 0
    if not text:
        return ru, en

    for w in WORD_RE.findall(text):
        has_cyr = bool(CYR_RE.search(w))
        has_lat = bool(LAT_RE.search(w))
        if has_cyr:
            ru += 1
        if has_lat:
            en += 1
    return ru, en

# ================== СБОР ВСЕХ СТРАНИЦ ПОД ROOT ==================


def collect_pages_under_root(root_page_id: str) -> list[dict]:
    """
    Рекурсивно обходим блоки под ROOT_PAGE_ID.
    Берём все child_page, куда бы они ни были засунуты (в том числе в несколько столбцов).
    """
    pages: dict[str, dict] = {}
    visited_blocks = set()

    def walk(block_id: str):
        if block_id in visited_blocks:
            return
        visited_blocks.add(block_id)

        cursor = None
        while True:
            resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)
            for block in resp.get("results", []):
                btype = block.get("type")

                if btype == "child_page":
                    pid = normalize_id(block["id"])
                    if pid not in pages:
                        try:
                            meta = get_page_meta(pid)
                            pages[pid] = meta
                            # рекурсивно обходим содержимое страницы
                            walk(pid)
                        except Exception as e:
                            print(f"Skipping page {pid}: {e}")

                # любой блок с детьми — идём внутрь (колонки, toggles и т.д.)
                if block.get("has_children"):
                    try:
                        walk(block["id"])
                    except Exception:
                        pass

            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
            time.sleep(0.1)

    # старт: сама root-страница
    # сначала добавим root как страницу
    try:
        root_meta = get_page_meta(root_page_id)
        pages[root_meta["id"]] = root_meta
    except Exception as e:
        print(f"Cannot retrieve root page meta: {e}")

    walk(root_page_id)
    return list(pages.values())

# ================== АНАЛИЗ ОДНОЙ СТРАНИЦЫ ==================


def analyze_page(page_id: str, properties: dict) -> tuple[int, int, bool]:
    """
    Возвращает (ru_words, en_words, unreadable_flag).
    unreadable=True, если вообще не нашли текста.
    """
    full_text_parts = []

    # properties
    props_text = extract_text_from_properties(properties)
    if props_text:
        full_text_parts.append(props_text)

    # blocks
    blocks = get_blocks(page_id)
    for block in blocks:
        # подстраницы считаем отдельно как отдельные страницы
        if block.get("type") == "child_page":
            continue
        txt = extract_all_text_from_block(block)
        if txt:
            full_text_parts.append(txt)

    full_text = "\n".join(part for part in full_text_parts if part).strip()
    if not full_text:
        return 0, 0, True

    ru, en = count_ru_en_words(full_text)
    return ru, en, False

# ================== MAIN ==================


def main():
    start = time.time()

    print(f"Collecting pages under ROOT_PAGE_ID={ROOT_PAGE_ID}...")
    pages = collect_pages_under_root(ROOT_PAGE_ID)
    print(f"Found {len(pages)} pages (including root).")

    results = []
    unreadable_pages = []

    for meta in pages:
        pid = meta["id"]
        title = meta["title"]
        url = meta["url"]
        author = meta["author"]

        ru, en, unreadable = analyze_page(pid, meta.get("properties", {}))
        if unreadable:
            print(f"⚠ No readable text on page: {title} — {url}")
            unreadable_pages.append((title, url))

        total = ru + en
        ru_pct = (ru / total * 100) if total else 0.0
        en_pct = (en / total * 100) if total else 0.0

        results.append({
            "Page Title": title,
            "Page URL": url,
            "Author": author,
            "% Russian": round(ru_pct, 2),
            "% English": round(en_pct, 2),
        })

    # сортировка: сначала по %English, потом по %Russian (оба по убыванию)
    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    out_name = "notion_language_percentages.csv"
    with open(out_name, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Page Title", "Page URL", "Author", "% Russian", "% English"]
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved {len(results)} rows to {out_name}")

    if unreadable_pages:
        print("\n⚠ Pages without readable text (no props and no blocks, или нет прав у интеграции):")
        for title, url in unreadable_pages:
            print(f" - {title}: {url}")

    print(f"\nDone in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
