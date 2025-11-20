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

ARTIFACT_URL = args.artifact_url  # not used for upload but kept for consistency

# ===== Environment =====
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")

notion = Client(auth=NOTION_TOKEN)
ONE_YEAR_AGO = datetime.now(timezone.utc) - timedelta(days=365)


# ======================================================
# Helpers
# ======================================================

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


# ======================================================
# ⭐⭐⭐ ТВОЯ РАБОЧАЯ РЕКУРСИЯ (full deep scan)
# ======================================================

def get_all_pages(block_id):
    pages = []
    cursor = None

    while True:
        resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)

        for block in resp["results"]:
            btype = block["type"]

            # 1) child_page → это страница
            if btype == "child_page":
                pid = block["id"]
                try:
                    info = get_page_info(pid)
                    pages.append(info)
                    pages.extend(get_all_pages(pid))
                except Exception:
                    pass

            # 2) Notion иногда скрывает подстраницы внутри:
            #    column, lists, toggle, synced_block...
            #    поэтому рекурсивно заходим ВСЕГДА
            if block.get("has_children", False):
                try:
                    pages.extend(get_all_pages(block["id"]))
                except Exception:
                    pass

            # 3) 💥 КРИТИЧЕСКОЕ ДОПОЛНЕНИЕ:
            #    даже если has_children=False — вложенные страницы МОГУТ быть внутри
            if btype in [
                "column", "column_list",
                "bulleted_list_item", "numbered_list_item",
                "toggle", "to_do",
                "synced_block", "paragraph",
                "quote", "callout"
            ]:
                try:
                    pages.extend(get_all_pages(block["id"]))
                except Exception:
                    pass

        cursor = resp.get("next_cursor")
        if not cursor:
            break

        time.sleep(0.15)

    return pages


# ======================================================
# Slack file upload (file + comment)
# ======================================================

def upload_file_to_slack(filepath, message):
    if not SLACK_BOT_TOKEN or not SLACK_CHANNEL:
        print("Slack bot token or channel missing.")
        return

    print("Uploading CSV to Slack...")

    with open(filepath, "rb") as f:
        response = requests.post(
            "https://slack.com/api/files.upload",
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
            data={"channels": SLACK_CHANNEL, "initial_comment": message},
            files={"file": f}
        )

    print("Slack upload status:", response.status_code)
    print("Slack response:", response.text)

    response.raise_for_status()

    data = response.json()
    if not data.get("ok"):
        raise Exception(f"Slack error: {data.get('error')}")


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

    with open("notion_old_pages_count.json", "w") as f:
        json.dump({"count": len(old_pages)}, f, ensure_ascii=False)

    print("CSV saved")


# ======================================================
# Phase 2 — Slack notification with file
# ======================================================

def notify_slack():
    with open("notion_old_pages_count.json", "r") as f:
        total = json.load(f)["count"]

    message = f"📄 Найдено *{total}* страниц, которые не редактировались больше года."

    upload_file_to_slack("notion_old_pages.csv", message)


# ======================================================
# MAIN
# ======================================================

if ARTIFACT_URL:
    notify_slack()
else:
    generate_csv_and_count()
