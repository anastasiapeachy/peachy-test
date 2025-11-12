"""
Полный анализ всех страниц и подстраниц в Notion, используя Search API.
CSV с колонками: Page Title, Page URL, % Russian, % English
"""

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
