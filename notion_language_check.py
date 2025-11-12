import os
import csv
import re
import time
from notion_client import Client
from langdetect import detect

# --- создаём requirements.txt если нет ---
if not os.path.exists("requirements.txt"):
    with open("requirements.txt", "w", encoding="utf-8") as req:
        req.write("notion-client\nlangdetect\n")

# --- настройки ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("SLACK_WEBHOOK_URL")  # используем переменную Slack для ID стартовой страницы

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("❌ Не заданы переменные окружения NOTION_TOKEN или SLACK_WEBHOOK_URL")

notion = Client(auth=NOTION_TOKEN)

# --- функции ---
def get_page_title(page_id, fallback="(Без названия)"):
    try:
        page = notion.pages.retrieve(page_id=page_id)
        props = page.get("properties", {})
        for prop in props.values():
            if prop.get("type") == "title":
                title_parts = [t["plain_text"] for t in prop["title"]]
                if title_parts:
                    return "".join(title_parts)
        # fallback для child_page
        block = notion.blocks.retrieve(block_id=page_id)
        if block.get("type") == "child_page":
            return block["child_page"].get("title", fallback)
    except Exception:
        return fallback
    return fallback

def get_page_url(page_id):
    clean_id = page_id.replace("-", "")
    return f"https://www.notion.so/{clean_id}"

def get_block_children(block_id):
    children = []
    next_cursor = None
    while True:
        response = notion.blocks.children.list(block_id=block_id, start_cursor=next_cursor)
        children.extend(response["results"])
        if not response.get("has_more"):
            break
        next_cursor = response.get("next_cursor")
    return children

def extract_text(block):
    text = ""
    btype = block.get("type")
    if btype and isinstance(block.get(btype), dict):
        rich_text = block[btype].get("rich_text", [])
        text = "".join([t.get("plain_text", "") for t in rich_text])
    return text.strip()

def detect_language(text):
    try:
        return detect(text)
    except:
        return "unknown"

def count_words(text):
    return len(re.findall(r'\b\w+\b', text))

def process_page(page_id, results, visited=None):
    if visited is None:
        visited = set()
    if page_id in visited:
        return
    visited.add(page_id)

    title = get_page_title(page_id)
    url = get_page_url(page_id)
    blocks = get_block_children(page_id)

    ru_words = 0
    en_words = 0

    for block in blocks:
        text = extract_text(block)
        if text:
            lang = detect_language(text)
            words = count_words(text)
            if lang == "ru":
                ru_words += words
            elif lang == "en":
                en_words += words

        # рекурсивно обрабатываем подстраницы
        if block["type"] == "child_page":
            process_page(block["id"], results, visited)

    total = ru_words + en_words
    ru_percent = (ru_words / total * 100) if total else 0
    en_percent = (en_words / total * 100) if total else 0

    results.append({
        "Page Title": title,
        "Page URL": url,
        "% Russian": round(ru_percent, 2),
        "% English": round(en_percent, 2)
    })

def export_to_csv(results, filename="notion_language_percentages.csv"):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Page Title", "Page URL", "% Russian", "% English"])
        writer.writeheader()
        writer.writerows(results)
    print(f"✅ CSV сохранён: {filename}")

# --- выполнение ---
if __name__ == "__main__":
    start = time.time()
    results = []
    process_page(ROOT_PAGE_ID, results)
    export_to_csv(results)
    print(f"⏱ Выполнено за {time.time() - start:.1f} сек. ({len(results)} страниц)")
