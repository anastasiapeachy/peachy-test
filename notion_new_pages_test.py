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


# === Safe request: retry on 429 & 5xx ===
def safe_request(func, *args, **kwargs):
    retries = 8
    delay = 0.25
    backoff = 1

    for attempt in range(retries):
        try:
            time.sleep(delay)
            return func(*args, **kwargs)
        except APIResponseError as e:
            status = e.status

            if status == 429:
                retry = int(e.headers.get("Retry-After", 1))
                print(f"[429] Rate limited → wait {retry}s")
                time.sleep(retry)
                continue

            if 500 <= status <= 599:
                print(f"[{status}] Server error → retry in {backoff}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue

            raise

    raise RuntimeError("Notion not responding")


# === Helpers ===
def notion_url(page_id):
    return f"https://www.notion.so/{page_id.replace('-', '')}"


def get_page_info(page_id):
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    # title
    title = "Untitled"
    for prop in page.get("properties", {}).values():
        if prop["type"] == "title" and prop.get("title"):
            title = prop["title"][0]["plain_text"]
            break

    # created time
    created_raw = page.get("created_time", "")
    created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00")).astimezone(timezone.utc)

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "created": created_dt,
    }


# === Optimized recursive page scanner ===
def get_block_children(block_id):
    blocks = []
    cursor = None

    while True:
        resp = safe_request(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor
        )

        blocks.extend(resp["results"])
        cursor = resp.get("next_cursor")
        if not cursor:
            break

    return blocks


def get_all_pages_fast(block_id):
    """
    Optimized traversal:
    ✔ child_page
    ✔ child_database
    ✔ ignore text blocks (paragraph, callout, toggle, etc.)
    """
    pages = []
    children = get_block_children(block_id)

    for block in children:
        btype = block["type"]

        # --- child_page ---
        if btype == "child_page":
            pid = block["id"]
            try:
                info = get_page_info(pid)
                pages.append(info)
                pages.extend(get_all_pages_fast(pid))  # go deeper
            except Exception as e:
                print(f"Skip page {pid}: {e}")

        # --- child_database ---
        elif btype == "child_database":
            db_id = block["id"]
            try:
                db_pages = safe_request(notion.databases.query, database_id=db_id)["results"]
                for row in db_pages:
                    pid = row["id"]
                    try:
                        info = get_page_info(pid)
                        pages.append(info)
                        pages.extend(get_all_pages_fast(pid))
                    except:
                        pass
            except Exception as e:
                print(f"Skip database {db_id}: {e}")

        # NO scanning of normal blocks → prevents 6-hour timeout

    return pages


# === Slack ===
def send_slack(text):
    if not SLACK_WEBHOOK_URL:
        print("Slack webhook missing")
        return

    resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text})
    print("Slack:", resp.status_code, resp.text)


# === MAIN TEST: pages created in last 24 hours ===
def main():
    print("Scanning pages (optimized)…")
    pages = get_all_pages_fast(ROOT_PAGE_ID)
    print(f"Total pages found: {len(pages)}")

    one_day_ago = datetime.now(timezone.utc) - timedelta(days=1)

    new_pages = [p for p in pages if p["created"] > one_day_ago]

    print(f"Pages created in last 24h: {len(new_pages)}")

    if not new_pages:
        send_slack("❗ Тест: нет новых страниц за последние 24 часа.")
        return

    lines = ["🆕 *ТЕСТ: новые страницы за последние 24 часа:*", ""]
    for p in new_pages:
        lines.append(f"📘 *{p['title']}*\n🔗 {p['url']}\n")

    send_slack("\n".join(lines))


if __name__ == "__main__":
    main()
