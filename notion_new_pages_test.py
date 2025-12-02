from notion_client import Client
from notion_client.errors import APIResponseError
import os
import time
import requests
from datetime import datetime, timezone, timedelta

# --- ENV ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

notion = Client(auth=NOTION_TOKEN)

# === Safe request: retry on 429 and 5xx ===
def safe_request(func, *args, **kwargs):
    retries = 7
    delay = 0.25
    backoff = 1

    for attempt in range(retries):
        try:
            time.sleep(delay)
            return func(*args, **kwargs)

        except APIResponseError as e:
            status = e.status

            if status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limited → waiting {retry_after}s…")
                time.sleep(retry_after)
                continue

            if 500 <= status <= 599:
                print(f"[{status}] Server error → retry in {backoff}s…")
                time.sleep(backoff)
                backoff = min(backoff * 2, 20)
                continue

            raise

    raise RuntimeError("Notion not responding")


# === Helpers ===
def notion_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_page_info(page_id):
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    title = "Untitled"
    for prop in page.get("properties", {}).values():
        if prop["type"] == "title" and prop.get("title"):
            title = prop["title"][0]["plain_text"]
            break

    # created_time
    created_raw = page.get("created_time", "")
    created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00")).astimezone(timezone.utc)

    return {
        "id": page_id,
        "title": title,
        "created": created_dt,
        "url": notion_url(page_id),
    }


# === Deep recursive page scanner ===
def get_block_children(block_id):
    blocks = []
    cursor = None

    while True:
        resp = safe_request(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor
        )
        blocks.extend(resp.get("results", []))
        cursor = resp.get("next_cursor")
        if not cursor:
            break

        time.sleep(0.1)

    return blocks


def get_all_pages(block_id):
    pages = []
    children = get_block_children(block_id)

    for block in children:
        btype = block["type"]

        if btype == "child_page":
            pid = block["id"]
            try:
                info = get_page_info(pid)
                pages.append(info)
                pages.extend(get_all_pages(pid))
            except Exception as e:
                print(f"Skip child_page {pid}: {e}")

        # deep nested blocks
        if block.get("has_children", False):
            try:
                pages.extend(get_all_pages(block["id"]))
            except Exception:
                pass

    return pages


# === Slack ===
def send_slack(text):
    if not SLACK_WEBHOOK_URL:
        print("Slack webhook missing")
        return

    resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text})
    print("Slack:", resp.status_code, resp.text)


# === MAIN (TEST MODE) ===
def main():
    print("Scanning all pages deeply…")
    pages = get_all_pages(ROOT_PAGE_ID)
    print(f"Total pages found: {len(pages)}")

    one_day_ago = datetime.now(timezone.utc) - timedelta(days=1)

    new_pages = [
        p for p in pages
        if p["created"] > one_day_ago
    ]

    print(f"Pages created in last 24 hours: {len(new_pages)}")

    if not new_pages:
        send_slack("❗ Тест: нет новых страниц за последние 24 часа.")
        return

    msg = ["🆕 *ТЕСТ: страницы, созданные за последние 24 часа:*", ""]
    for p in new_pages:
        msg.append(f"📘 *{p['title']}*\n🔗 {p['url']}")

    send_slack("\n".join(msg))


if __name__ == "__main__":
    main()
