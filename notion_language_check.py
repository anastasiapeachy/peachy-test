"""
Анализирует все страницы и подстраницы в Notion (включая базы данных)
и создает CSV-файл с процентом текста на русском и английском языках.
Выводятся только: название страницы, ссылка на неё и проценты.
"""

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
ROOT_PAGE_ID = os.getenv("SLACK_WEBHOOK_URL")  # Используем Slack-переменную для ID страницы

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("❌ Не заданы переменные окружения NOTION_TOKEN или SLACK_WEBHOOK_URL")

notion = Client(auth=NOTION_TOKEN)

# --- утилиты ---
def get_page_title(page_id, fallback_name="(Без названия)"):
    """Получаем заголовок страницы"""
    try:
        page = notion.pages.retrieve(page_id=page_id)
        props = page.get("properties", {})
        for prop in props.values():
            if prop.get("type") == "title":
                title_parts = [t["plain_text"] for t in prop["title"]]
                if title_parts:
                    return "".join(title_parts)
        # если страница не имеет title property — возможно, это child_page
        block = no
