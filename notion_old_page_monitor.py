from notion_client import Client
import json
import os
import time
import requests
from datetime import datetime, timezone, timedelta
import csv


NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

notion = Client(auth=NOTION_TOKEN)

ONE_YEAR_AGO = datetime.now(timezone.utc) - timedelta(days=365)


# ============================================
# Helpers
# ============================================

def notion_url(page_id):
    clean_id = page_id.replace("-", "")
    return f"https://www.notion.so/{clean_id}"


def get_page_info(page_id):
    """Return full details of a Notion page."""
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
    if last_raw:
        last_dt = datetime.fromisoformat(last_raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    else:
        last_dt = None

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "last_edited": last_dt,
    }


def get_all_pages(block_id):
    """
    Recursively fetch all child pages.
    EXACT same recursion style as your sample code.
    """
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

            # recursively fetch children of any block with children
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

def send_to_slack(old_pages):
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL is empty! Check GitHub Secrets.")
        return

    total = len(old_pages)

    if total == 0:
        payload = {
            "text": "🎉 Нет страниц, которые не редактировались больше года!"
        }
    else:
        top_pages = old_pages[:20]

        rows = "\n".join(
            f"- *<{p['url']}|{p['title']}>* — `{p['last_edited']}`"
            for p in top_pages
        )

        if total > 20:
            rows += f"\n… и ещё *{total - 20}* страниц"

        payload = {
            "text": (
                f"📄 *Отчёт Notion*\n"
                f"Найдено *{total}* страниц, которые не редактировались больше года.\n\n"
                f"{rows}"
            )
        }

    print("\n=== Sending Slack message ===")
    print("SLACK_WEBHOOK_URL:", SLACK_WEBHOOK_URL)
    print("Payload to Slack:")
    print(json.dumps(payload, indent=2, ensure_ascii=False))

    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        print("Slack status:", response.status_code)
        print("Slack response:", response.text)

        response.raise_for_status()

        print("Slack message sent successfully ✔")
    except Exception as e:
        print(f"Failed to send Slack message: {e}")

    # show max 20 items
    top_pages = old_pages[:20]

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"📄 *Найдено {total} страниц, которые не редактировались больше года*"
            }
        },
        {"type": "divider"}
    ]

    for page in top_pages:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*<{page['url']}|{page['title']}>*\n🕒 Последняя правка: `{page['last_edited']}`"
            }
        })

    if total > 20:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"… и ещё *{total - 20}* страниц"}
        })

    payload = {"blocks": blocks}

    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        response.raise_for_status()
        print("Posted report to Slack")
    except Exception as e:
        print(f"Failed to post to Slack: {e}")


# ============================================
# MAIN
# ============================================

def main():
    print("Fetching pages recursively...")
    pages = get_all_pages(ROOT_PAGE_ID)

    print(f"Total found: {len(pages)}")

    # Filter old pages
    old_pages = []
    for p in pages:
        if not p["last_edited"]:
            continue
        if p["last_edited"] < ONE_YEAR_AGO:
            p["last_edited"] = p["last_edited"].isoformat()
            old_pages.append(p)

    # Sort oldest first
    old_pages.sort(key=lambda x: x["last_edited"])

    print(f"Old pages: {len(old_pages)}")

    # Save CSV (artifact)
    with open("notion_old_pages.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["title", "last_edited", "url"])
        for p in old_pages:
            w.writerow([p["title"], p["last_edited"], p["url"]])

    print("CSV saved → notion_old_pages.csv")

    # Slack report
    send_to_slack(old_pages)


if __name__ == "__main__":
    main()
