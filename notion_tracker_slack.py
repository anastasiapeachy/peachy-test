from notion_client import Client
import json
import os
import time
import requests
from datetime import datetime, timezone, timedelta

# === SETTINGS ===
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

STORAGE_FILE = "notion_tracker_data/known_pages.json"
DELAY_DAYS = 7   # задержка перед публикацией

notion = Client(auth=NOTION_TOKEN)


def notion_url(page_id: str) -> str:
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_page_info(page_id):
    """Получает title, author, created_time, is_public."""
    page = notion.pages.retrieve(page_id=page_id)

    # ------------------- TITLE -------------------
    title = "Untitled"
    for prop in page.get("properties", {}).values():
        if prop["type"] == "title" and prop.get("title"):
            title = prop["title"][0]["plain_text"]
            break

    # ------------------- AUTHOR -------------------
    author_info = page.get("created_by", {})
    author_name = author_info.get("name") or author_info.get("id", "Unknown")

    if author_name == author_info.get("id"):
        try:
            u = notion.users.retrieve(user_id=author_info["id"])
            author_name = u.get("name") or author_name
        except:
            pass

    # ------------------- TIMES -------------------
    created_time = page.get("created_time", "")
    last_edited = page.get("last_edited_time", "")

    # ------------------- PUBLIC SHARE -------------------
    is_public = bool(page.get("public_url"))

    return {
        "id": page_id,
        "title": title,
        "author": author_name,
        "url": notion_url(page_id),
        "created_time": created_time,
        "last_edited_time": last_edited,
        "is_public": is_public,
    }


def get_all_pages_recursively(block_id):
    """Рекурсивно собирает child_page и вложенные."""
    pages = []
    resp = notion.blocks.children.list(block_id=block_id)

    while True:
        for block in resp.get("results", []):
            if block["type"] == "child_page":
                pid = block["id"]
                title = block["child_page"]["title"]
                try:
                    info = get_page_info(pid)
                except Exception as e:
                    print(f"⚠️ Error reading {title} ({pid}): {e}")
                    continue

                pages.append(info)
                # Рекурсивно идём внутрь
                pages.extend(get_all_pages_recursively(pid))

        if not resp.get("has_more"):
            break

        resp = notion.blocks.children.list(
            block_id=block_id,
            start_cursor=resp.get("next_cursor")
        )
        time.sleep(0.2)

    return pages


def load_known_pages():
    os.makedirs(os.path.dirname(STORAGE_FILE), exist_ok=True)
    if os.path.exists(STORAGE_FILE):
        with open(STORAGE_FILE, "r") as f:
            data = json.load(f)
        print(f"Loaded known pages: {len(data)}")
        return data
    return []


def save_known_pages(pages):
    os.makedirs(os.path.dirname(STORAGE_FILE), exist_ok=True)
    with open(STORAGE_FILE, "w") as f:
        json.dump(pages, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(pages)} pages to cache")


def send_to_slack(message):
    if not SLACK_WEBHOOK_URL:
        print("⚠️ SLACK_WEBHOOK_URL not set")
        return
    requests.post(SLACK_WEBHOOK_URL, json={"text": message})


def main():
    known = load_known_pages()
    known_ids = {p["id"] for p in known}

    # === Сканируем весь раздел ===
    current = get_all_pages_recursively(ROOT_PAGE_ID)
    print(f"Found total pages BEFORE filtering: {len(current)}")  # <===== ✔️ ЛОГ

    # Новые страницы относительно кэша
    new_pages = [p for p in current if p["id"] not in known_ids]

    # Фильтр — только public + старше 7 дней
    now = datetime.now(timezone.utc)
    eligible = []

    for p in new_pages:
        if not p["is_public"]:
            continue
        if not p["created_time"]:
            continue

        try:
            created_dt = datetime.fromisoformat(p["created_time"].replace("Z", "+00:00"))
        except:
            continue

        if (now - created_dt).days >= DELAY_DAYS:
            eligible.append(p)

    # === Отправляем в Slack ===
    if eligible:
        lines = ["🆕 *New public Notion articles (older than 7 days):*", ""]
        for p in eligible:
            lines.append(
                f":blue_book: *{p['title']}*\n"
                f":link: {p['url']}\n"
                f":writing_hand: {p['author']}\n"
            )
        send_to_slack("\n".join(lines))
    else:
        print("No eligible pages to send.")

    save_known_pages(current)


if __name__ == "__main__":
    main()
