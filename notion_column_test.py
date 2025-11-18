import os
import re
from notion_client import Client
from langdetect import detect

# Берём токен и ID страницы из переменных окружения
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
PAGE_ID = os.getenv("TEST_PAGE_ID") or os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not PAGE_ID:
    raise ValueError("Environment vars NOTION_TOKEN and TEST_PAGE_ID or ROOT_PAGE_ID are required")

def normalize_id(raw_id: str) -> str:
    """Нормализуем Notion ID или URL к 32-символьному hex."""
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip()
    m = re.search(r"([0-9a-fA-F]{32})", s.replace("-", ""))
    if m:
        return m.group(1)
    return s.replace("-", "")

client = Client(auth=NOTION_TOKEN)
PAGE_ID = normalize_id(PAGE_ID)

def get_children(block_id: str):
    """Забираем всех детей блока (с пагинацией)."""
    all_blocks = []
    cursor = None
    while True:
        resp = client.blocks.children.list(block_id=block_id, start_cursor=cursor)
        all_blocks.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return all_blocks

def extract_all_text_from_block(block: dict) -> str:
    """
    Рекурсивно достаём ВСЁ читаемое человеком:
    - rich_text
    - caption'ы
    - параграфы, заголовки, списки, callout'ы, toggles
    - детей, включая column_list / column
    """
    texts = []
    btype = block.get("type")
    content = block.get(btype, {}) if btype else {}

    # rich_text внутри контента блока
    if isinstance(content, dict):
        if "rich_text" in content:
            rt = content.get("rich_text") or []
            texts.append(" ".join(t.get("plain_text", "") for t in rt if t.get("plain_text")))

        # caption (картинки, bookmark и т.п.)
        if "caption" in content:
            cap = content.get("caption") or []
            texts.append(" ".join(t.get("plain_text", "") for t in cap if t.get("plain_text")))

    # equation
    if btype == "equation":
        eq = content.get("expression")
        if eq:
            texts.append(eq)

    # ВАЖНО: для column_list / column принудительно лезем в детей
    force_children = btype in ("column_list", "column")

    try:
        if block.get("has_children") or force_children:
            children = get_children(block["id"])
            for child in children:
                child_text = extract_all_text_from_block(child)
                if child_text:
                    texts.append(child_text)
    except Exception as e:
        print(f"Error fetching children for block {block.get('id')}: {e}")

    return " ".join(t for t in texts if t).strip()

def detect_lang_safe(text: str) -> str:
    text = text.strip()
    if not text:
        return "unknown"
    try:
        return detect(text)
    except Exception:
        return "unknown"

def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))

def main():
    print(f"Testing page: {PAGE_ID}")
    blocks = get_children(PAGE_ID)
    print(f"Top-level blocks: {len(blocks)}")

    total_ru = 0
    total_en = 0

    for block in blocks:
        btype = block.get("type")

        # сабстраницы сейчас не трогаем
        if btype == "child_page":
            continue

        text = extract_all_text_from_block(block)
        if not text:
            continue

        lang = detect_lang_safe(text)
        words = count_words(text)

        print("-" * 40)
        print(f"Block {block.get('id')} type={btype}")
        print(f"Detected lang: {lang}, words: {words}")
        print(text[:300] + ("..." if len(text) > 300 else ""))

        if lang == "ru":
            total_ru += words
        elif lang == "en":
            total_en += words

    total = total_ru + total_en
    if total == 0:
        ru_pct = en_pct = 0.0
    else:
        ru_pct = total_ru / total * 100
        en_pct = total_en / total * 100

    print("=" * 60)
    print(f"Total RU words: {total_ru}")
    print(f"Total EN words: {total_en}")
    print(f"RU %: {ru_pct:.2f}")
    print(f"EN %: {en_pct:.2f}")

if __name__ == "__main__":
    main()
