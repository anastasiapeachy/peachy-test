from notion_client import Client
import json
import os
import time
import requests

# === НАСТРОЙКИ ===
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
STORAGE_FILE = "known_pages.json"

notion = Client(auth=NOTION_TOKEN)


def notion_url(page_id: str) -> str:
    """Генерирует ссылку на страницу."""
    clean_id = page_id.replace("-", "")
    return f"https://www.notion.so/{clean_id}"


def get_page_info(page_id):
    """Получает информацию о странице (название, автор, ссылка)."""
    page = notion.pages.retrieve(page_id=page_id)
    title = None

    # Пробуем извлечь заголовок
    if "properties" in page:
        for prop in page["properties"].values():
            if prop["type"] == "title" and prop["title"]:
                title = prop["title"][0]["plain_text"]
                break

    if not title:
        title = "Без названия"

    # Получаем имя автора
    author_info = page.get("created_by", {})
    author_name = "Неизвестен"
    if author_info and "id" in author_info:
        try:
            user_data = notion.users.retrieve(user_id=author_info["id"])
            author_name = user_data.get("name") or "Неизвестен"
        except Exception:
            pass

    return {
        "id": page_id,
        "title": title,
        "author": author_name,
        "url": notion_url(page_id),
    }


def get_all_pages_recursively(block_id):
    """Рекурсивно получает все страницы (включая вложенные)."""
    pages = []
    response = notion.blocks.children.list(block_id=block_id)

    while True:
        for block in response["results"]:
            if block["type"] == "child_page":
                page_id = block["id"]
                title = block["child_page"]["title"]
                try:
                    info = get_page_info(page_id)
                except Exception:
                    info = {"id": page_id, "title": title, "author": "?", "url": notion_url(page_id)}
                pages.append(info)
                # Проверяем вложенные страницы
                pages.extend(get_all_pages_recursively(page_id))

        if not response.get("has_more"):
            break
        response = notion.blocks.children.list(block_id=block_id, start_cursor=response["next_cursor"])
        time.sleep(0.3)

    return pages


def load_known_pages():
    if os.path.exists(STORAGE_FILE):
        with open(STORAGE_FILE, "r") as f:
            return json.load(f)
    return []


def save_known_pages(pages):
    with open(STORAGE_FILE, "w") as f:
        json.dump(pages, f, indent=2, ensure_ascii=False)


def send_to_slack(message: str):
    """Отправляет сообщение в Slack."""
    if not SLACK_WEBHOOK_URL:
        print("⚠️ Slack webhook не задан — пропускаю отправку.")
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": message})
    except Exception as e:
        print(f"Ошибка при отправке в Slack: {e}")


def main():
    print("🔍 Проверяю обновления в Notion...")

    known = load_known_pages()
    current = get_all_pages_recursively(ROOT_PAGE_ID)

    known_ids = {p["id"] for p in known}
    new_pages = [p for p in current if p["id"] not in known_ids]

    if new_pages:
        message = "🆕 *New articles in Notion this week:*\n"
        for p in new_pages:
            message += f"\n📘 *{p['title']}*\n🔗 {p['url']}\n✍️ {p['author']}\n"
        print(message)
        send_to_slack(message)
        save_known_pages(current)
    else:
        print("✅ Новых статей нет.")
        send_to_slack("✅ Новых статей в Notion нет.")


if __name__ == "__main__":
    main()
