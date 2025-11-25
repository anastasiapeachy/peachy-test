from notion_client import Client
import os
import time
import requests
from datetime import datetime, timezone, timedelta
import csv
import json
import argparse

# -------- Parse arguments (only for Slack phase) ----------
parser = argparse.ArgumentParser()
parser.add_argument("--artifact-url", default=None)
args = parser.parse_args()

ARTIFACT_URL = args.artifact_url

# -------- Environment -------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN is not set")

notion = Client(auth=NOTION_TOKEN)
ONE_YEAR_AGO = datetime.now(timezone.utc) - timedelta(days=365)


# -------- Helpers -------------

def notion_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_page_info(page_id):
    page = notion.pages.retrieve(page_id=page_id)

    # Title
    title = "Untitled"
    if "properties" in page:
        for prop in page["properties"].values():
            if prop["type"] == "title" and prop.get("title"):
                title = prop["title"][0]["plain_text"]
                break

    # Last edited
    last_raw = page.get("last_edited_time", "")
    last_dt = datetime.fromisoformat(last_raw.replace("Z", "+00:00")).astimezone(timezone.utc)

    # Detect empty database pages
    props = page.get("properties", {})
    db_props = [p for p in props.values() if p["type"] != "title"]
    is_empty = all(
        p.get(p["type"]) in (None, [], "", {}) or p.get("type") == "formula"
        for p in props.values()
    )

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "last_edited": last_dt,
        "is_empty": is_empty
    }


# -------- Deep recursive scanner (original working version) -------------

def get_all_pages(block_id):
    pages = []
    cursor = None

    while True:
        resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)

        for block in resp["results"]:
            btype = block["type"]

            # 1) Normal child pages
            if btype == "child_page":
                pid = block["id"]
                try:
                    info = get_page_info(pid)
                    pages.append(info)
                    pages.extend(get_all_pages(pid))
                except Exception:
                    pass

            # 2) Blocks with children → scan
            if block.get("has_children", False):
                try:
                    pages.extend(get_all_pages(block["id"]))
                except Exception:
                    pass

            # 3) Forced scanning inside content blocks
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

        time.sleep(0.15)

    return pages


# -------- Slack message sender (webhook + button) -------------

def send_slack_message(total_count, csv_url):
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL missing")
        return

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"📄 Найдено *{total_count}* страниц, которые не редактировались больше года."
            }
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "⬇️ Download CSV"},
                    "url": csv_url
                }
            ]
        }
    ]

    payload = {"blocks": blocks}

    resp = requests.post(SLACK_WEBHOOK_URL, json=payload)
    print("Slack response:", resp.status_code, resp.text)


# -------- Phase 1: Generate CSV -----------

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
        if not p["is_empty"] and p["last_edited"] < ONE_YEAR_AGO
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


# -------- Phase 2: Slack -----------

def notify_slack():
    with open("notion_old_pages_count.json", "r") as f:
        total = json.load(f)["count"]

    if total == 0:
        print("No outdated pages — message not sent")
        return

    send_slack_message(total, ARTIFACT_URL)


# -------- MAIN -----------

if ARTIFACT_URL:
    notify_slack()
else:
    generate_csv_and_count()
