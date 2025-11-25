from notion_client import Client
from langdetect import detect
import os
import csv
import time
import re

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

notion = Client(auth=NOTION_TOKEN)

# ============================
# Helpers
# ============================
def get_page_title(page_id):
    try:
        page = notion.pages.retrieve(page_id=page_id)
        props = page.get("properties", {})
        for prop in props.values():
            if prop.get("type") == "title":
                return "".join(
                    [t["plain_text"] for t in prop[prop["type"]]["title"]]
                )
    except Exception:
        pass
    return "(Без названия)"


def get_page_blocks(page_id):
    blocks = []
    next_cursor = None
    while True:
        response = notion.blocks.children.list(
            block_id=page_id, start_cursor=next_cursor
        )
        blocks.extend(response["results"])
        if not response.get("has_more"):
            break
        next_cursor = response.get("next_cursor")
    return blocks


def extract_text_from_block(block):
    text = ""
    block_type = block.get("type")

    if block_type and isinstance(block.get(block_type), dict):
        rt = block[block_type].get("rich_text", [])
        text = "".join([t.get("plain_text", "") for t in rt])

    return text


def detect_language(text):
    try:
        return detect(text)
    except:
        return "unknown"


def count_words(text):
    return len(re.findall(r"\b\w+\b", text))


def is_effectively_empty(blocks):
    """если кроме title нет контента — считаем пустой"""
    total_text = ""

    for block in blocks:
        t = extract_text_from_block(block).strip()
        if t:
            total_text += t

    return len(total_text.strip()) == 0


# ============================
# Main recursive processor
# ============================
def process_page(page_id, results):
    title = get_page_title(page_id)
    blocks = get_page_blocks(page_id)

    # Пропуск пустых страниц (только title)
    if is_effectively_empty(blocks):
        print(f"⚪ Skip empty page: {title}")
        return

    ru_words = 0
    en_words = 0

    for block in blocks:
        text = extract_text_from_block(block)

        if text.strip():
            lang = detect_language(text)
            words = count_words(text)

            if lang == "ru":
                ru_words += words
            elif lang == "en":
                en_words += words

        # Рекурсивно обрабатываем подстраницы
        if block["type"] == "child_page":
            subpage_id = block["id"]
            process_page(subpage_id, results)

    total = ru_words + en_words
    ru_percent = (ru_words / total * 100) if total else 0
    en_percent = (en_words / total * 100) if total else 0

    results.append({
        "Page Title": title,
        "Page ID": page_id,
        "% Russian": round(ru_percent, 2),
        "% English": round(en_percent, 2),
    })


def export_to_csv(results, filename="notion_language_stats.csv"):
    if not results:
        print("⚠ Нет страниц для сохранения.")
        return

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"✅ CSV сохранён: {filename}")


# ============================
# Entry point
# ============================
if __name__ == "__main__":
    start = time.time()
    results = []

    process_page(ROOT_PAGE_ID, results)

    export_to_csv(results)

    print(f"⏱ Готово за {time.time() - start:.1f} сек.")
