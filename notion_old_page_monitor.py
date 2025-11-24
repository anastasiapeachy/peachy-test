from notion_client import Client
from notion_client.errors import APIResponseError
import os
import time
import requests
from datetime import datetime, timezone, timedelta
import csv
import json
import argparse

# ===== Args =====
parser = argparse.ArgumentParser()
parser.add_argument("--artifact-url", default=None)
args = parser.parse_args()

ARTIFACT_URL = args.artifact_url

# ===== Environment =====
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# NOTION инициализируем ТОЛЬКО в фазе сканирования
if ARTIFACT_URL is None:
    if not NOTION_TOKEN:
        raise ValueError("NOTION_TOKEN is not set")
    if not ROOT_PAGE_ID:
        raise ValueError("ROOT_PAGE_ID is not set")
    notion = Client(auth=NOTION_TOKEN)
    ONE_YEAR_AGO = datetime.now(timezone.utc) - timedelta(days=365)
else:
    notion = None
    ONE_YEAR_AGO = None


# ======================================================
# Helpers
# ======================================================

def notion_url(page_id: str) -> str:
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def safe_request(func, *args, **kwargs):
    """
    Безопасный вызов Notion API:
    - retry на 429 (rate limit)
    - retry на 5xx (временные ошибки Notion)
    - экспоненциальный backoff
    """
    max_retries = 10
    base_delay = 0.25
    backoff = 1

    for attempt in range(max_retries):
        try:
            time.sleep(base_delay)
            return func(*args, **kwargs)
        except APIResponseError as e:
            status = e.status
            # 429 — слишком много запросов
            if status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limit. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            # 5xx — глюки Notion
            if 500 <= status <= 599:
                print(f"[{status}] Notion internal error. Retry in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            # Остальное — фатально
            raise
    raise RuntimeError("Too many retries — Notion API not responding.")


def get_page_info(page_id):
    page = safe_request(notion.pages.retrieve, page_id)
    title = "Untitled"

    if "properties" in page:
        for prop in page["properties"].values():
            if prop["type"] == "title" and prop.get("title"):
                title = prop["title"][0]["plain_text"]
                break

    last_raw = page.get("last_edited_time", "")
    last_dt = datetime.fromisoformat(last_raw.replace("Z", "+00:00")).astimezone(timezone.utc)

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "last_edited": last_dt,
    }


def is_page_empty(page_id: str) -> bool:
    """
    Считаем страницу "пустой", если у неё нет ни одного блока-контента.
    (Т.е. страницы из database только с заголовком, но без текста, будут отфильтрованы.)
    """
    resp = safe_request(
        notion.blocks.children.list,
        block_id=page_id,
        page_size=1  # достаточно проверить, что первый блок вообще существует
    )
    results = resp.get("results", [])
    return len(results) == 0


# ======================================================
# Глубокий обход всех потомков ROOT_PAGE_ID
# ======================================================

def get_all_pages(block_id):
    pages = []
    cursor = None

    while True:
        resp = safe_request(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor
        )

        for block in resp["results"]:
            btype = block["type"]

            # 1) child_page → полноценная страница
            if btype == "child_page":
                pid = block["id"]
                try:
                    info = get_page_info(pid)
                    pages.append(info)
                    # рекурсивно уходим внутрь
                    pages.extend(get_all_pages(pid))
                except Exception as e:
                    print(f"Skipping page {pid}: {e}")

            # 2) Любой блок с has_children → сканируем детей
            if block.get("has_children", False):
                try:
                    pages.extend(get_all_pages(block["id"]))
                except Exception as e:
                    print(f"Skipping children of {block['id']}: {e}")

            # 3) Дополнительно углубляемся по типам, которые часто прячут вложенные страницы
            if btype in [
                "column", "column_list",
                "bulleted_list_item", "numbered_list_item",
                "toggle", "to_do", "synced_block",
                "paragraph", "quote", "callout"
            ]:
                try:
                    pages.extend(get_all_pages(block["id"]))
                except Exception as e:
                    print(f"Deep scan skip for {block['id']}: {e}")

        cursor = resp.get("next_cursor")
        if not cursor:
            break

        time.sleep(0.15)

    return pages


# ======================================================
# Phase 1 — scan Notion & generate CSV
# ======================================================

def generate_csv_and_count():
    print("Scanning Notion deeply...")
    pages = get_all_pages(ROOT_PAGE_ID)
    print(f"Total discovered pages (including empty): {len(pages)}")

    old_pages = []

    for p in pages:
        # игнорируем пустые страницы (только заголовок, нет контента)
        try:
            if is_page_empty(p["id"]):
                continue
        except Exception as e:
            print(f"Failed emptiness check for {p['id']}: {e}")

        if p["last_edited"] < ONE_YEAR_AGO:
            old_pages.append({
                "title": p["title"],
                "last_edited": p["last_edited"].isoformat(),
                "url": p["url"]
            })

    # сортировка: самые старые наверху
    old_pages.sort(key=lambda x: x["last_edited"])
    print(f"Old non-empty pages found: {len(old_pages)}")

    # CSV только со старыми непустыми страницами
    with open("notion_old_pages.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["title", "last_edited", "url"])
        for p in old_pages:
            w.writerow([p["title"], p["last_edited"], p["url"]])

    # Сохраняем количество для второй фазы
    with open("notion_old_pages_count.json", "w", encoding="utf-8") as f:
        json.dump({"count": len(old_pages)}, f, ensure_ascii=False)

    print("CSV saved")


# ======================================================
# Phase 2 — Slack notification (через Webhook)
# ======================================================

def notify_slack(artifact_url: str):
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL missing, skip Slack.")
        return

    # читаем количество страниц
    try:
        with open("notion_old_pages_count.json", "r", encoding="utf-8") as f:
            total = json.load(f)["count"]
    except Exception as e:
        print(f"Cannot read notion_old_pages_count.json: {e}")
        return

    # условие: если 0 → не слать вообще
    if total == 0:
        print("No old pages found — skipping Slack.")
        return

    text = f"📄 Найдено *{total}* страниц, которые не редактировались больше года."

    # "человеческий" блок с кнопкой Download CSV
    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": text}
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Download CSV"},
                    "url": artifact_url,
                    "style": "primary"
                }
            ]
        }
    ]

    payload = {"blocks": blocks}

    print("Sending Slack message...")
    resp = requests.post(SLACK_WEBHOOK_URL, json=payload)
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}

    print(f"Slack status: {resp.status_code}")
    print(f"Slack response: {data}")


# ======================================================
# MAIN
# ======================================================

if ARTIFACT_URL:
    # Phase 2 — только Slack, без Notion
    notify_slack(ARTIFACT_URL)
else:
    # Phase 1 — только Notion + CSV + count.json
    generate_csv_and_count()
