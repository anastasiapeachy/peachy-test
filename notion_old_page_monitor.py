from notion_client import Client
import os
import time
import requests
from datetime import datetime, timezone, timedelta
import csv
import json
import argparse

# аргумент для второго запуска (уведомление)
parser = argparse.ArgumentParser()
parser.add_argument("--artifact-url", default=None)
args = parser.parse_args()

ARTIFACT_URL = args.artifact_url

# env
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

notion = Client(auth=NOTION_TOKEN)
ONE_YEAR_AGO = datetime.now(timezone.utc) - timedelta(days=365)


# ============================================
# Very stable working get_page_info (тот самый)
# ============================================

def notion_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_page_info(page_id):
    page = notion.pages.retrieve(page_id=page_id)

    # Extract title
    title = "Untitled"
    if "properties" in page:
        for prop in page["properties"].values():
            if prop["type"] == "title" and prop.get("title"):
                title = prop["title"][0]["plain_text"]
                break

    # Last edited
    last_raw = page.get("last_edited_time", "")
    last_dt = datetime.fromisoformat(last_raw.replace("Z", "+00:00")).astimezone(timezone.utc)

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "last_edited": last_dt
    }


# ============================================================
# ⭐⭐ Тот самый рабочий рекурсивный обход, который находил ВСЕ
# ============================================================

def get_all_pages(block_id):
    pages = []
    cursor = None

    while True:
        response = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)

        for block in response["results"]:
            btype = block["type"]

            # 1. child_page → добавляем
            if btype == "child_page":
                pid = block["id"]
                try:
                    info = get_page_info(pid)
                    pages.append(info)
                    # рекурсия
                    pages.extend(get_all_pages(pid))
                except Exception as e:
                    print(f"Skipping page {pid}: {e}")

            # 2. любой блок с has_children → обходим (это критично!)
            if block.get("has_children"):
                try:
                    pages.extend(get_all_pages(block["id"]))
                except Exception:
                    pass

        # Pagination
        cursor = response.get("next_cursor")
        if not cursor:
            break

        time.sleep(0.15)

    return pages


# ============================================
# Slack message
# ============================================

def send_slack_message(total, artifact_url):
    payload = {
        "text": (
            f"📄 Найдено *{total}* страниц, которые не редактировались больше года.\n"
            f"📎 Скачать CSV: {artifact_url}"
        )
    }

    print("Sending Slack message...")
    r = requests.post(SLACK_WEBHOOK_URL, json=payload)
    print("Slack status:", r.status_code)
    print(r.text)
    r.raise_for_status()


# ============================================
# Phase 1 — generate CSV & count
# ============================================

def generate_csv_and_count():
    print("Fetching pages recursively...")
    pages = get_all_pages(ROOT_PAGE_ID)
    print(f"Total found: {len(pages)}")

    old_pages = [
        {
            "title": p["title"],
            "last_edited": p["last_edited"].isoformat(),
            "url": p["url"]
        }
        for p in pages
        if p["last_edited"] < ONE_YEAR_AGO
    ]

    old_pages.sort(key=lambda x: x["last_edited"])
    print(f"Old pages: {len(old_pages)}")

    # save CSV
    with open("notion_old_pages.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["title", "last_edited", "url"])
        for p in old_pages:
            w.writerow([p["title"], p["last_edited"], p["url"]])

    print("CSV saved")

    # save count
    with open("notion_old_pages_count.json", "w") as f:
        json.dump({"count": len(old_pages)}, f, ensure_ascii=False)


# ============================================
# Phase 2 — notify Slack
# ============================================

def notify_slack():
    with open("notion_old_pages_count.json", "r") as f:
        total = json.load(f)["count"]

    send_slack_message(total, ARTIFACT_URL)


# ============================================
# MAIN switch
# ============================================

if ARTIFACT_URL:
    notify_slack()
else:
    generate_csv_and_count()
