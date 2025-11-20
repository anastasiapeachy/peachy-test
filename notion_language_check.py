import os
import csv
import re
import time
from notion_client import Client
from langdetect import detect

# ==========================
# Env and basic setup
# ==========================

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID env vars")


def normalize_id(raw_id: str) -> str:
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip()
    # Позволяем передавать и URL, и 32-hex ID
    m = re.search(r"([0-9a-fA-F]{32})", s.replace("-", ""))
    if m:
        return m.group(1)
    return s.replace("-", "")


ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)
notion = Client(auth=NOTION_TOKEN)


# ==========================
# Helpers
# ==========================

def notion_url(page_id: str) -> str:
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_page_basic_info(page_id: str) -> dict:
    """Вернёт id, title, url, author для страницы."""
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

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "author": author,
    }


def get_all_pages_from_root(root_page_id: str) -> list:
    """
    Обходит блоки, начиная с root_page_id, и собирает ВСЕ child_page-страницы,
    включая те, что лежат в колонках, списках, toggles и т.д.
    Только то, что доступно из root.
    """
    pages = {}
    visited_blocks = set()

    def walk_blocks(block_or_page_id: str):
        cursor = None
        while True:
            try:
                resp = notion.blocks.children.list(
                    block_id=block_or_page_id,
                    start_cursor=cursor
                )
            except Exception as e:
                print(f"Can't list children for {block_or_page_id}: {e}")
                return

            for block in resp.get("results", []):
                bid = block.get("id")
                if not bid or bid in visited_blocks:
                    continue
                visited_blocks.add(bid)

                btype = block.get("type")

                # child_page = отдельная страница
                if btype == "child_page":
                    page_id = block["id"]
                    if page_id not in pages:
                        try:
                            info = get_page_basic_info(page_id)
                            pages[page_id] = info
                        except Exception as e:
                            print(f"Skipping page {page_id}: {e}")
                            # всё равно пойдём внутрь, если сможем
                    # рекурсивно обходим её содержимое, чтобы найти её подстраницы
                    walk_blocks(page_id)

                # любой блок с has_children=True (в т.ч. columns, toggles и т.п.)
                if block.get("has_children"):
                    walk_blocks(bid)

            cursor = resp.get("next_cursor")
            if not cursor:
                break
            time.sleep(0.1)

    # добавляем сам root
    root_info = get_page_basic_info(root_page_id)
    pages[root_page_id] = root_info

    # и обходим его блоки, чтобы найти все вложенные страницы
    walk_blocks(root_page_id)

    return list(pages.values())


# ==========================
# Text extraction
# ==========================

def extract_all_text_from_block(block: dict) -> str:
    """
    Рекурсивно вытягивает ВЕСЬ читаемый текст из блока,
    включая содержимое в колонках, списках, callout’ах, таблицах и т.п.
    """
    texts = []
    btype = block.get("type")
    content = block.get(btype, {}) if btype else {}

    # 1) generic rich_text
    if isinstance(content, dict) and "rich_text" in content:
        rich_text = content.get("rich_text", [])
        if rich_text:
            texts.append(" ".join(
                t.get("plain_text", "") for t in rich_text if t.get("plain_text")
            ))

    # 2) явные типы с rich_text
    for key in [
        "paragraph", "heading_1", "heading_2", "heading_3",
        "quote", "callout", "bulleted_list_item",
        "numbered_list_item", "toggle", "to_do"
    ]:
        if btype == key:
            rt = block.get(key, {}).get("rich_text", [])
            if rt:
                texts.append(" ".join(
                    t.get("plain_text", "") for t in rt if t.get("plain_text")
                ))

    # 3) caption (image, video, file, embed, bookmark и др.)
    if isinstance(content, dict) and "caption" in content:
        cap = content.get("caption", [])
        if cap:
            texts.append(" ".join(
                t.get("plain_text", "") for t in cap if t.get("plain_text")
            ))

    # 4) equation
    if btype == "equation":
        eq = block.get("equation", {}).get("expression")
        if eq:
            texts.append(eq)

    # 5) table – читаем строки и ячейки
    if btype == "table":
        try:
            cursor = None
            while True:
                resp = notion.blocks.children.list(
                    block_id=block["id"],
                    start_cursor=cursor
                )
                for row in resp.get("results", []):
                    if row.get("type") == "table_row":
                        cells = row["table_row"].get("cells", [])
                        for cell in cells:
                            if cell:
                                texts.append(" ".join(
                                    t.get("plain_text", "") for t in cell if t.get("plain_text")
                                ))
                cursor = resp.get("next_cursor")
                if not cursor:
                    break
        except Exception:
            pass

    # 6) synced_block – если ссылается на оригинальный блок, читаем его детей
    if btype == "synced_block":
        synced = block.get("synced_block", {})
        sf = synced.get("synced_from")
        if sf and isinstance(sf, dict):
            original_id = sf.get("block_id")
            if original_id:
                try:
                    cursor = None
                    while True:
                        resp = notion.blocks.children.list(
                            block_id=original_id,
                            start_cursor=cursor
                        )
                        for child in resp.get("results", []):
                            texts.append(extract_all_text_from_block(child))
                        cursor = resp.get("next_cursor")
                        if not cursor:
                            break
                except Exception:
                    pass

    # 7) рекурсивно обходим детей любого блока с has_children=True
    # (кроме случаев, когда уже обработали особую логику выше)
    if block.get("has_children") and btype not in ("synced_block", "table"):
        try:
            cursor = None
            while True:
                resp = notion.blocks.children.list(
                    block_id=block["id"],
                    start_cursor=cursor
                )
                for child in resp.get("results", []):
                    # child_page — отдельная страница, её анализируется отдельно
                    if child.get("type") == "child_page":
                        continue
                    texts.append(extract_all_text_from_block(child))
                cursor = resp.get("next_cursor")
                if not cursor:
                    break
        except Exception:
            pass

    return " ".join(t for t in texts if t).strip()


def detect_lang(text: str) -> str:
    try:
        return detect(text)
    except Exception:
        return "unknown"


def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))


def analyze_page_lang(page_id: str) -> tuple[int, int]:
    """Вернёт (ru_words, en_words) для одной страницы."""
    ru = 0
    en = 0

    cursor = None
    while True:
        try:
            resp = notion.blocks.children.list(
                block_id=page_id,
                start_cursor=cursor
            )
        except Exception as e:
            print(f"Can't read blocks for page {page_id}: {e}")
            break

        for block in resp.get("results", []):
            # child_page — отдельная страница, не считаем её текст в родителя
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

        cursor = resp.get("next_cursor")
        if not cursor:
            break

    return ru, en


# ==========================
# Main
# ==========================

def main():
    start = time.time()
    print(f"Root page: {ROOT_PAGE_ID}")

    pages = get_all_pages_from_root(ROOT_PAGE_ID)
    print(f"Total pages under root (including root): {len(pages)}")

    results = []
    for p in pages:
        pid = p["id"]
        title = p["title"]
        url = p["url"]
        author = p["author"]

        ru_words, en_words = analyze_page_lang(pid)
        total = ru_words + en_words
        ru_pct = (ru_words / total * 100) if total else 0.0
        en_pct = (en_words / total * 100) if total else 0.0

        results.append({
            "Page Title": title,
            "Page URL": url,
            "Author": author,
            "% Russian": round(ru_pct, 2),
            "% English": round(en_pct, 2),
        })

    # сортировка: сначала по % English, потом по % Russian (обе по убыванию)
    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Page Title", "Page URL", "Author", "% Russian", "% English"],
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved {len(results)} rows to {fname}")
    print(f"Done in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
