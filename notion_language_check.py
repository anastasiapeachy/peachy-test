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
ROOT_PAGE_ID = os.getenv("SLACK_WEBHOOK_URL")  # используем как ID стартовой страницы

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("❌ Не заданы переменные окружения NOTION_TOKEN или SLACK_WEBHOOK_URL")

notion = Client(auth=NOTION_TOKEN)

# --- функции ---
def get_page_title(page):
    """Получаем заголовок страницы"""
    props = page.get("properties", {})
    for prop in props.values():
        if prop.get("type") == "title":
            title_parts = [t["plain_text"] for t in prop["title"]]
            if title_parts:
                return "".join(title_parts)
    return "(Без названия)"

def get_page_url(page_id):
    clean_id = page_id.replace("-", "")
    return f"https://www.notion.so/{clean_id}"

def get_block_children(block_id):
    """Получаем все блоки страницы"""
    children = []
    next_cursor = None
    while True:
        try:
            response = notion.blocks.children.list(block_id=block_id, start_cursor=next_cursor)
            children.extend(response["results"])
            if not response.get("has_more"):
                break
            next_cursor = response.get("next_cursor")
        except Exception as e:
            print(f"⚠️ Ошибка при получении блоков {block_id}: {e}")
            break
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
    try:
        return detect(text)
    except:
        return "unknown"

def count_words(text):
    return len(re.findall(r'\b\w+\b', text))

def process_page(page_id):
    """Обрабатывает одну страницу и возвращает проценты"""
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
        # рекурсивно подстраницы внутри child_page
        if block.get("type") == "child_page":
            sub_id = block["id"]
            sub_ru, sub_en = process_page(sub_id)
            ru_words += sub_ru
            en_words += sub_en

    return ru_words, en_words

def search_pages(root_id):
    """Ищем все страницы под корнем"""
    pages = []
    next_cursor = None
    while True:
        try:
            response = notion.search(
                query="",
                filter={"value": "page", "property": "object"},
                start_cursor=next_cursor
            )
        except Exception as e:
            print(f"⚠️ Ошибка поиска: {e}")
            break

        for result in response.get("results", []):
            parent = result.get("parent", {})
            # Фильтруем по parent_id
            if parent.get("type") == "page_id" and parent.get("page_id") == root_id:
                pages.append(result)
        if not response.get("has_more"):
            break
        next_cursor = response.get("next_cursor")
    return pages

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

    pages = search_pages(ROOT_PAGE_ID)
    print(f"Найдено {len(pages)} страниц под корнем {ROOT_PAGE_ID}")

    for page in pages:
        page_id = page["id"]
        title = get_page_title(page)
        url = get_page_url(page_id)
        ru_words, en_words = process_page(page_id)
        total = ru_words + en_words
        ru_percent = (ru_words / total * 100) if total else 0
        en_percent = (en_words / total * 100) if total else 0
        results.append({
            "Page Title": title,
            "Page URL": url,
            "% Russian": round(ru_percent, 2),
            "% English": round(en_percent, 2)
        })

    export_to_csv(results)
    print(f"⏱ Выполнено за {time.time() - start:.1f} сек. ({len(results)} страниц)")
