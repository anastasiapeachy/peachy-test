from notion_client import Client
import os
import time
import requests
from datetime import datetime, timezone, timedelta
import csv
import json

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
ARTIFACT_URL = os.getenv("ARTIFACT_URL")  # появляется при втором запуске

notion = Client(auth=NOTION_TOKEN)
ONE_YEAR_AGO = datetime.now(timezone.utc) - timedelta(days=365)


# ============================================
# Helpers
# ============================================

def notion_url(page_id):
    clean_id = page_id.replace("-", "")
    return f"https://www.notion.so/{clean_id}"


def get_page_info(page_id):
    page = notion.pages.retrieve(page_id=page_id)

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


def get_all_pages(block_id):
    pages = []
    response = notion.blocks.children.list(block_id=block_id)

    while True:
        for block in response["results"]:
            if block["type"] == "child_page":
                page_id = block["id"]
                try:
                    info = get_page_info(page_id)
                    pages.append(info)
                    pages.extend(get_all_pages(page_id))
                except Exception as e:
                    print(f"Skipping page {page_id}: {e}")

            if block.get("has_children") and block["type"] != "child_page":
                try:
                    pages.extend(get_all_pages(block["id"]))
                except Exception:
                    pass

        if not response.get("has_more"):
            break

        response = notion.blocks.children.list(
            block_id=block_id,
            start_cursor=response["next_cursor"]
        )
        time.sleep(0.2)

    return pages


# ============================================
# Slack message
# ============================================

def send_slack_message(total_count, artifact_url):
    if not SLACK_WEBHOOK_URL:
        print("No Slack webhook configured.")
        return

    payload = {
        "text": (
            f"📄 Найдено *{total_count}* страниц, которые не редактировались больше года.\n"
            f"📎 Скачать CSV: {artifact_url}"
        )
    }

    print("Sending Slack message...")
    r = requests.post(SLACK_WEBHOOK_URL, json=payload)
    print("Slack status:", r.status_code)
    print(r.text)
    r.raise_for_status()


# ============================================
# MAIN mode 1: generate CSV
# MAIN mode 2: post Slack message (if ARTIFACT_URL is set)
# ============================================

def main():
    # Mode 2 — send Slack message
    if ARTIFACT_URL:
        with open("notion_old_pages_count.json", "r") as f:
            total = json.load(f)["count"]

        send_slack_message(total, ARTIFACT_URL)
        return

    # Mode 1 — generate CSV + save count
    print("Fetching pages recursively...")
    pages = get_all_pages(ROOT_PAGE_ID)
    print(f"Total found: {len(pages)}")

    old_pages = [
        {
            "title": p["title"],
            "last_edited": p["last_edited"].isoformat(),
            "url": p["url"],
        }
        for p in pages
        if p["last_edited"] < ONE_YEAR_AGO
    ]

    old_pages.sort(key=lambda x: x["last_edited"])
    print(f"Old pages: {len(old_pages)}")

    # Save CSV
    csv_path = "notion_old_pages.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["title", "last_edited", "url"])
        for p in old_pages:
            w.writerow([p["title"], p["last_edited"], p["url"]])

    print("CSV saved")

    # save count for phase 2
    with open("notion_old_pages_count.json", "w") as f:
        json.dump({"count": len(old_pages)}, f)


if __name__ == "__main__":
    main()
