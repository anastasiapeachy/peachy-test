from notion_client import Client
from notion_client.errors import APIResponseError
import os
import time
import requests
from datetime import datetime, timezone, timedelta
import csv
import json
import argparse

# ===== Args for phase 2 (Slack run) =====
parser = argparse.ArgumentParser()
parser.add_argument("--artifact-url", default=None)
args = parser.parse_args()

ARTIFACT_URL = args.artifact_url

# ===== Environment =====
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN is not set")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID is not set")

notion = Client(auth=NOTION_TOKEN)
ONE_YEAR_AGO = datetime.now(timezone.utc) - timedelta(days=365)


# ======================================================
# Helpers
# ======================================================

def safe_request(func, *args, **kwargs):
    """
    Безопасный вызов Notion API:
    - ретраи при 429 (rate limit)
    - ретраи при 5xx
    - экспоненциальный backoff
    """
    max_retries = 8
    base_delay = 0.3
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
                print(f"[429] Rate limit exceeded. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            # 5xx — проблемы на стороне Notion
            if 500 <= status <= 599:
                print(f"[{status}] Notion API error. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            # другие ошибки — пробрасываем
            raise
    raise RuntimeError("Too many retries — Notion API not responding.")


def notion_url(page_id: str) -> str:
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_page_info(page_id: str):
    """Инфо о странице: title, url, last_edited."""
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    title = "Untitled"
    if "properties" in page:
        for prop in page["properties"].values():
            if prop["type"] == "title" and prop.get("title"):
                title = prop["title"][0]["plain_text"]
                break

    last_raw = page.get("last_edited_time", "")
    last_dt = datetime.fromisoformat(last_raw.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "last_edited": last_dt,
    }


def get_block_children(block_id: str):
    """Все дочерние блоки (с пагинацией)."""
    blocks = []
    cursor = None

    while True:
        resp = safe_request(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor,
        )
        blocks.extend(resp.get("results", []))
        cursor = resp.get("next_cursor")
        if not cursor:
            break
        time.sleep(0.1)

    return blocks


def get_database_pages(database_id: str):
    """Все страницы внутри базы (database)."""
    pages = []
    cursor = None

    while True:
        resp = safe_request(
            notion.databases.query,
            database_id=database_id,
            start_cursor=cursor,
        )
        pages.extend(resp.get("results", []))
        cursor = resp.get("next_cursor")
        if not cursor:
            break
        time.sleep(0.1)

    return pages


# ======================================================
# ⭐⭐⭐ РЕАЛЬНАЯ РАБОЧАЯ РЕКУРСИЯ — полный обход всех потомков ROOT_PAGE_ID
# ======================================================

def get_all_pages(block_id: str):
    """
    Рекурсивно обходим ВСЕ потомки:
    - child_page (страницы)
    - child_database + все страницы в базе
    - любые блоки с has_children=True (columns, toggles и т.п.)
    """
    pages = []
    children = get_block_children(block_id)

    for block in children:
        btype = block["type"]

        # 1) child_page → обычная страница
        if btype == "child_page":
            pid = block["id"]
            try:
                info = get_page_info(pid)
                pages.append(info)
                # рекурсивно заходим внутрь страницы
                pages.extend(get_all_pages(pid))
            except Exception as e:
                print(f"Skip child_page {pid}: {e}")

        # 2) child_database → база со строками-страницами
        elif btype == "child_database":
            db_id = block["id"]
            try:
                db_pages = get_database_pages(db_id)
                for db_page in db_pages:
                    pid = db_page["id"]
                    try:
                        info = get_page_info(pid)
                        pages.append(info)
                        # на всякий случай заходим внутрь каждой db-страницы
                        pages.extend(get_all_pages(pid))
                    except Exception as e:
                        print(f"Skip db page {pid}: {e}")
            except Exception as e:
                print(f"Skip child_database {db_id}: {e}")

        # 3) Любой блок с потомками (columns, lists, toggles и т.п.)
        if block.get("has_children") and btype not in ("child_page", "child_database"):
            try:
                pages.extend(get_all_pages(block["id"]))
            except Exception as e:
                print(f"Skip nested block {block['id']}: {e}")

    return pages


# ======================================================
# Slack через Webhook
# ======================================================

def send_slack_webhook(total: int, artifact_url: str):
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL missing — Slack notification skipped.")
        return

    text = (
        f"📄 Найдено *{total}* страниц, которые не редактировались больше года.\n"
        f"CSV отчёт доступен в GitHub Actions: {artifact_url}"
    )

    payload = {"text": text}

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code != 200:
            print(
                f"Failed to send Slack message: {resp.status_code} {resp.text}"
            )
        else:
            print("Slack notification sent successfully.")
    except Exception as e:
        print(f"Error sending Slack webhook: {e}")


# ======================================================
# Phase 1 — scan Notion & generate CSV
# ======================================================

def generate_csv_and_count():
    print("Scanning Notion deeply...")
    pages = get_all_pages(ROOT_PAGE_ID)
    print(f"Total discovered pages: {len(pages)}")

    old_pages = [
        {
            "title": p["title"],
            "last_edited": p["last_edited"].isoformat(),
            "url": p["url"],
        }
        for p in pages
        if p["last_edited"] < ONE_YEAR_AGO
    ]

    # сортируем: самые старые — сверху
    old_pages.sort(key=lambda x: x["last_edited"])
    print(f"Old pages found: {len(old_pages)}")

    # CSV только со старыми страницами
    with open("notion_old_pages.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["title", "last_edited", "url"])
        for p in old_pages:
            w.writerow([p["title"], p["last_edited"], p["url"]])

    # отдельный файл с числом старых страниц
    with open("notion_old_pages_count.json", "w", encoding="utf-8") as f:
        json.dump({"count": len(old_pages)}, f, ensure_ascii=False)

    print("CSV saved")


# ======================================================
# Phase 2 — Slack notification (с ссылкой на artifact)
# ======================================================

def notify_slack():
    # читаем число старых страниц
    try:
        with open("notion_old_pages_count.json", "r", encoding="utf-8") as f:
            total = json.load(f)["count"]
    except FileNotFoundError:
        print("notion_old_pages_count.json not found, Slack step skipped.")
        return

    send_slack_webhook(total, ARTIFACT_URL)


# ======================================================
# MAIN
# ======================================================

if ARTIFACT_URL:
    # Phase 2 — Slack
    notify_slack()
else:
    # Phase 1 — сканирование и CSV
    generate_csv_and_count()
