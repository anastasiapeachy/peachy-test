from notion_client import Client
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

# ===== Environment Vars =====
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

notion = Client(auth=NOTION_TOKEN)
ONE_YEAR_AGO = datetime.now(timezone.utc) - timedelta(days=365)

# ============= NEW: защита от циклической рекурсии ============
visited_blocks = set()

# ======================================================
# Helpers
# ======================================================

def notion_url(page_id):
    return f"https://www.notion.so/{page_id.replace('-', '')}"


def get_page_info(page_id):
    page = notion.pages.retrieve(page_id=page_id)

    # Title extraction
    title = "Untitled"
    if "properties" in page:
        for prop in page["properties"].values():
            if prop["type"] == "title" and prop.get("title"):
                title = prop["title"][0]["plain_text"]
                break

    # Last edited timestamp
    last_raw = page.get("last_edited_time", "")
    last_dt = datetime.fromisoformat(last_raw.replace("Z", "+00:00")).astimezone(timezone.utc)

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "last_edited": last_dt
    }


# ======================================================
# ⭐⭐⭐ Полный глубокий обход всех подстраниц — с защитой visited
# ======================================================

def get_all_pages(block_id):
    # ---- защита от бесконечной рекурсии ----
    if block_id in visited_blocks:
        return []
    visited_blocks.add(block_id)

    pages = []
    cursor = None

    while True:
        resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)

        for block in resp["results"]:
            btype = block["type"]

            # 1) child_page → нормальная страница
            if btype == "child_page":
                pid = block["id"]
                try:
                    info = get_page_info(pid)
                    pages.append(info)
                    pages.extend(get_all_pages(pid))
                except Exception:
                    pass

            # 2) обычная рекурсия в блоки с детьми
            if block.get("has_children", False):
                try:
                    pages.extend(get_all_pages(block["id"]))
                except Exception:
                    pass

            # 3) глубокий обход: колонок, списков, toggles, synced_block, paragraph etc.
            if btype in [
                "column", "column_list",
                "bulleted_list_item", "numbered_list_item",
                "toggle", "to_do", "synced_block",
                "paragraph", "quote", "callout"
            ]:
                try:
                    pages.extend(get_all_pages(block["id"]))
                except Exception:
                    pass

        cursor = resp.get("next_cursor")
        if not cursor:
            break

        time.sleep(0.12)

    return pages


# ======================================================
# Slack: только сообщение с количеством + ссылка
# ======================================================

def upload_file_to_slack(filepath, message):
    print("Slack direct file upload disabled — using link-only mode.")


def notify_slack():
    with open("notion_old_pages_count.json", "r") as f:
        total = json.load(f)["count"]

    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL missing")
        return

    message = {
        "text": (
            f"📄 Найдено *{total}* страниц, которые не редактировали больше года.\n\n"
            f"📎 Скачать CSV файл: {ARTIFACT_URL}"
        )
    }

    r = requests.post(SLACK_WEBHOOK_URL, json=message)
    print("Slack response:", r.status_code, r.text)
    r.raise_for_status()


# ======================================================
# Phase 1 — scan Notion & generate CSV
# ======================================================

def generate_csv_and_count():
    print("Scanning Notion deeply...")
    pages = get_all_pages(ROOT_PAGE_ID)
    print(f"Total discovered pages: {len(pages)}")

    # Filter older than 1 year
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
    print(f"Old pages found: {len(old_pages)}")

    # Save CSV
    with open("notion_old_pages.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["title", "last_edited", "url"])
        for p in old_pages:
            w.writerow([p["title"], p["last_edited"], p["url"]])

    # Save count
    with open("notion_old_pages_count.json", "w") as f:
        json.dump({"count": len(old_pages)}, f, ensure_ascii=False)

    print("CSV saved")


# ======================================================
# MAIN
# ======================================================

if ARTIFACT_URL:
    notify_slack()      # Phase 2
else:
    generate_csv_and_count()   # Phase 1
