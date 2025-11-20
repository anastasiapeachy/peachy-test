import os
import csv
import re
import time
from notion_client import Client
from langdetect import detect

# === Config from env ===
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID env vars")

# батч: по умолчанию 20 страниц за запуск
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))   # сколько страниц обрабатывать за запуск
BATCH_INDEX = int(os.getenv("BATCH_INDEX", "0"))  # номер батча (0, 1, 2, ...)


def normalize_id(raw_id: str) -> str:
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip()
    # вытащить 32-символьный hex из URL или id с дефисами
    match = re.search(r"([0-9a-fA-F]{32})", s.replace("-", ""))
    if match:
        return match.group(1)
    return s.replace("-", "")


ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)
notion = Client(auth=NOTION_TOKEN)


def get_title(page: dict) -> str:
    """Тайтл страницы из properties, fallback — child_page title."""
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
    except Exception:
        pass

    return "(untitled)"


def make_url(page_id: str) -> str:
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_blocks(block_or_page_id: str):
    """Все прямые children для блока/страницы, с пагинацией."""
    blocks = []
    cursor = None
    while True:
        try:
            resp = notion.blocks.children.list(
                block_id=block_or_page_id,
                start_cursor=cursor
            )
        except Exception as e:
            print(f"Can't get blocks for {block_or_page_id}: {e}")
            break

        blocks.extend(resp.get("results", []))

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return blocks


# ===================================================================
# ПОЛНЫЙ РЕКУРСИВНЫЙ ВЫТАСКИВАТЕЛЬ ТЕКСТА (включая колонки)
# ===================================================================
def extract_all_text_from_block(block: dict) -> str:
    texts = []
    btype = block.get("type")
    content = block.get(btype, {}) if btype else {}

    # 0) column_list / column — сначала рекурсивно обойти children
    if btype in ("column_list", "column"):
        try:
            children = notion.blocks.children.list(block["id"]).get("results", [])
            for child in children:
                texts.append(extract_all_text_from_block(child))
        except Exception:
            pass
        return " ".join(t for t in texts if t).strip()

    # 1) generic rich_text
    if isinstance(content, dict) and "rich_text" in content:
        rich_text = content.get("rich_text", [])
        if rich_text:
            texts.append(
                " ".join(
                    t.get("plain_text", "")
                    for t in rich_text
                    if t.get("plain_text")
                )
            )

    # 2) стандартные текстовые блоки
    for key in [
        "paragraph",
        "heading_1",
        "heading_2",
        "heading_3",
        "quote",
        "callout",
        "bulleted_list_item",
        "numbered_list_item",
        "toggle",
        "to_do",
    ]:
        if btype == key:
            rt = block.get(key, {}).get("rich_text", [])
            if rt:
                texts.append(
                    " ".join(
                        t.get("plain_text", "")
                        for t in rt
                        if t.get("plain_text")
                    )
                )

    # 3) caption (картинки, embed, bookmark и т.п.)
    if isinstance(content, dict) and "caption" in content:
        cap = content.get("caption", [])
        if cap:
            texts.append(
                " ".join(
                    t.get("plain_text", "")
                    for t in cap
                    if t.get("plain_text")
                )
            )

    # 4) equation
    if btype == "equation":
        eq = block.get("equation", {}).get("expression")
        if eq:
            texts.append(eq)

    # 5) synced_block (ссылка на другой блок)
    if btype == "synced_block":
        synced = block.get("synced_block", {})
        sf = synced.get("synced_from")
        if sf:
            original_id = sf.get("block_id")
            try:
                children = notion.blocks.children.list(original_id).get("results", [])
                for child in children:
                    texts.append(extract_all_text_from_block(child))
            except Exception:
                pass

    # 6) table → пройти по строкам/ячейкам
    if btype == "table":
        try:
            rows = notion.blocks.children.list(block["id"]).get("results", [])
            for row in rows:
                if row.get("type") == "table_row":
                    cells = row["table_row"].get("cells", [])
                    for cell in cells:
                        texts.append(
                            " ".join(
                                t.get("plain_text", "")
                                for t in cell
                                if t.get("plain_text")
                            )
                        )
        except Exception:
            pass

    # 7) рекурсивно обойти children (колонки, toggles, списки, всё подряд)
    if block.get("has_children"):
        try:
            children = notion.blocks.children.list(block["id"]).get("results", [])
            for child in children:
                texts.append(extract_all_text_from_block(child))
        except Exception:
            pass

    return " ".join(t for t in texts if t).strip()


def extract_text_from_properties(properties: dict) -> str:
    """Текст из properties (title, rich_text, select, people, и т.д.)."""
    texts = []
    if not isinstance(properties, dict):
        return ""

    for prop in properties.values():
        ptype = prop.get("type")

        if ptype == "title":
            texts.append(
                " ".join(t.get("plain_text", "") for t in prop.get("title", []))
            )
        elif ptype == "rich_text":
            texts.append(
                " ".join(
                    t.get("plain_text", "") for t in prop.get("rich_text", [])
                )
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
                        texts.append(
                            " ".join(
                                t.get("plain_text", "")
                                for t in item["title"]
                            )
                        )
                    if "rich_text" in item:
                        texts.append(
                            " ".join(
                                t.get("plain_text", "")
                                for t in item["rich_text"]
                            )
                        )
        elif ptype == "people":
            for u in prop.get("people", []):
                if u.get("name"):
                    texts.append(u.get("name"))
        elif ptype in ("number", "url", "email", "phone"):
            if prop.get(ptype):
                texts.append(str(prop.get(ptype)))

    return " ".join(t for t in texts if t).strip()


def detect_lang(text: str) -> str:
    try:
        return detect(text)
    except Exception:
        return "unknown"


def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))


def gather_all_subpages(root_page_id: str):
    """
    Идём от ROOT_PAGE_ID вниз по child_page блокам,
    включая страницы внутри колонок/тогглов и т.п.
    Возвращаем список page_id, включая root.
    """
    collected = []
    visited_pages = set()

    def walk_page(page_id: str):
        page_id_norm = normalize_id(page_id)
        if page_id_norm in visited_pages:
            return
        visited_pages.add(page_id_norm)
        collected.append(page_id_norm)

        # ищем child_page во ВСЕМ дереве блоков
        stack = get_blocks(page_id_norm)
        while stack:
            block = stack.pop()
            btype = block.get("type")

            # настоящая подстраница Notion
            if btype == "child_page":
                child_page_id = normalize_id(block["id"])
                if child_page_id not in visited_pages:
                    walk_page(child_page_id)

            # углубляемся в блоки с детьми (колонки, toggles и т.д.)
            if block.get("has_children") and btype != "child_page":
                try:
                    children = notion.blocks.children.list(block["id"]).get("results", [])
                    stack.extend(children)
                except Exception:
                    pass

    walk_page(root_page_id)
    return collected


def analyze_page(page_id: str):
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
    except Exception:
        pass

    # 2) blocks
    blocks = get_blocks(page_id)

    if not props_text and len(blocks) == 0:
        unreadable = True

    for block in blocks:
        # подстраницы считаем отдельно
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


def main():
    start = time.time()
    unreadable_pages = []

    print(f"ROOT_PAGE_ID: {ROOT_PAGE_ID}")
    print("Collecting subpages via child_page tree...")
    all_page_ids = gather_all_subpages(ROOT_PAGE_ID)
    total_pages = len(all_page_ids)
    print(f"Total pages under root: {total_pages}")

    # батчинг: берём только нужный кусок списка
    if BATCH_SIZE > 0:
        start_idx = BATCH_INDEX * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, total_pages)
        page_ids = all_page_ids[start_idx:end_idx]
        print(
            f"Processing batch {BATCH_INDEX} "
            f"({start_idx}..{end_idx - 1}) of {total_pages} pages"
        )
    else:
        page_ids = all_page_ids
        print("Processing all pages in one run")

    results = []
    for page_id in page_ids:
        try:
            page = notion.pages.retrieve(page_id=page_id)
        except Exception as e:
            print(f"Cannot retrieve page {page_id}: {e}")
            unreadable_pages.append((f"(id:{page_id})", make_url(page_id)))
            continue

        title = get_title(page)
        url = make_url(page_id)

        # автор
        author_info = page.get("created_by", {}) or {}
        author = author_info.get("name")
        if not author:
            uid = author_info.get("id")
            if uid:
                try:
                    user_data = notion.users.retrieve(user_id=uid)
                    author = user_data.get("name")
                except Exception:
                    author = None
        if not author:
            author = "(unknown)"

        ru, en, unreadable = analyze_page(page_id)

        if unreadable:
            print(f"⚠ API cannot read page: {title} — {url}")
            unreadable_pages.append((title, url))

        total = ru + en
        ru_pct = (ru / total * 100) if total else 0
        en_pct = (en / total * 100) if total else 0

        results.append(
            {
                "Page Title": title,
                "Page URL": url,
                "Author": author,
                "% Russian": round(ru_pct, 2),
                "% English": round(en_pct, 2),
            }
        )

    # сортировка: сначала по % английского, потом по % русского
    results.sort(
        key=lambda x: (x["% English"], x["% Russian"]),
        reverse=True,
    )

    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "Page Title",
                "Page URL",
                "Author",
                "% Russian",
                "% English",
            ],
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
