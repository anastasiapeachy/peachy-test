import os
import csv
import re
import time
from notion_client import Client
from langdetect import detect

# --- создаём requirements.txt, если его нет ---
if not os.path.exists("requirements.txt"):
    with open("requirements.txt", "w", encoding="utf-8") as req:
        req.write("notion-client\nlangdetect\n")

# --- настройки ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("SLACK_WEBHOOK_URL")  # используем Slack переменную для ID страницы

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("❌ Не заданы переменные окружения NOTION_TOKEN или SLACK_WEBHOOK_URL")

notion = Client(auth=NOTION_TOKEN)

# --- функции ---
def get_page_title(page_id, fallback_name="(Без названия)"):
    """Получаем корректный заголовок страницы"""
    try:
        page = notion.pages.retrieve(page_id=page_id)
        props = page.get("properties", {})
        for prop in props.values():
            if prop.get("type") == "title":
                title_parts = [t["plain_text"] for t in prop["title"]]
                if title_parts:
                    return "".join(title_parts)
        # если нет title property — возможно, это child_page
        block = notion.blocks.retrieve(block_id=page_id)
        if block.get("type") == "child_page":
            return block["child_page"].get("title", fallback_name)
    except Exception:
        return fallback_name
    return fallback_name

def get_page_url(page_id):
    """Ссылка на страницу"""
    clean_id = page_id.replace("-", "")
    return f"https://www.notion.so/{clean_id}"

def get_block_children(block_id):
    """Получаем всех детей блока"""
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
    """Извлекаем текст из блока"""
    text = ""
    btype = block.get("type")
    if btype and isinstance(block.get(btype), dict):
        rich_text = block[btype].get("rich_text", [])
        text = "".join([t.get("plain_text", "") for t in rich_text])
    return text.strip()

def detect_language(text):
    """Определяем язык текста"""
    try:
        return detect(text)
    except:
        return "unknown"

def count_words(text):
    """Подсчет количества слов"""
    return len(re.findall(r'\b\w+\b', text))

def process_page(page_id, results, visited=None):
    """Рекурсивный обход всех страниц и баз данных"""
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

        # рекурсивный обход подстраниц и баз данных
        if block["type"] == "child_page":
            process_page(block["id"], results, visited)
        elif block["type"] == "child_database":
            query_and_process_database(block["id"], results, visited)

    total = ru_words + en_words
    ru_percent = (ru_words / total * 100) if total else 0
    en_percent = (en_words / total * 100) if total else 0

    results.append({
        "Page Title": title,
        "Page URL": url,
        "% Russian": round(ru_percent, 2),
        "% English": round(en_percent, 2)
    })

def query_and_process_database(database_id, results, visited):
    """Обрабатывает все страницы внутри базы данных"""
    next_cursor = None
    while True:
        response = notion.databases.query(database_id=database_id, start_cursor=next_cursor)
        for page in response["results"]:
            process_page(page["id"], results, visited)
        if not response.get("has_more"):
            break
        next_cursor = response.get("next_cursor")

def export_to_csv(results, filename="notion_language_percentages.csv"):
    """Сохраняем результаты в CSV"""
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
