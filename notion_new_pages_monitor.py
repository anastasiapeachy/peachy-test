from notion_client import Client
from notion_client.errors import APIResponseError
import os
import time
import requests
from datetime import datetime, timezone, timedelta

# ------------------------
# ENVIRONMENT
# ------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN is not set")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID is not set")

notion = Client(auth=NOTION_TOKEN)

# за последние 24 часа
ONE_DAY_AGO = datetime.now(timezone.utc) - timedelta(days=1)

# ------------------------
# SAFE REQUEST (retry)
# ------------------------
def safe_request(func, *args, **kwargs):
    max_retries = 8
    delay = 0.25
    backoff = 1

    for attempt in range(max_retries):
        try:
            time.sleep(delay)
            return func(*args, **kwargs)
        except APIResponseError as e:
            status = e.status

            if status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limit → wait {retry_after}s")
                time.sleep(retry_after)
                continue

            if 500 <= status <= 599:
                print(f"[{status}] Server error → retry in {backoff}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue

            raise

    raise RuntimeError("Notion is not responding after retries")

# ------------------------
# HELPERS
# ------------------------
def notion_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"

def get_page_info(page_id):
    """Extracts title, url, author, created."""
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    # Title
    title = "Untitled"
    for prop in page.get("properties", {}).values():
        if prop["type"] == "title" and prop.get("title"):
            title = prop["title"][0]["plain_text"]
            break

    # Author
    created_by = page.get("created_by", {})
    author = created_by.get("name") or created_by.get("id", "Unknown")

    # Fix missing name
    if author == created_by.get("id"):
        try:
            user = safe_request(notion.users.retrieve, user_id=created_by["id"])
            author = user.get("name") or author
        except:
            pass

    # Created time
    created_raw = page.get("created_time", "")
    created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00")).astimezone(timezone.utc)

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "author": author,
        "created": created_dt
    }

# ------------------------
# BLOCK CHILDREN
# ------------------------
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

# ------------------------
# FULL SCAN (this one works properly!)
# ------------------------
def get_all_pages(block_id):
    pages = []
    children = get_block_children(block_id)

    for block in children:
        btype = block["type"]

        # 1️⃣ Child page
        if btype == "child_page":
            pid = block["id"]
            try:
                info = get_page_info(pid)
                pages.append(info)
                pages.extend(get_all_pages(pid))
            except Exception as e:
                print(f"Skip child_page {pid}: {e}")

        # 2️⃣ Child database
        elif btype == "child_database":
            db_id = block["id"]

            try:
                db_pages = safe_request(notion.databases.query, database_id=db_id)
                for db_page in db_pages["results"]:
                    pid = db_page["id"]

                    try:
                        info = get_page_info(pid)
                        pages.append(info)
                        pages.extend(get_all_pages(pid))
                    except Exception as e:
                        print(f"Skip db row {pid}: {e}")

            except Exception as e:
                print(f"Skip database {db_id}: {e}")

        # 3️⃣ Columns, toggles, nested blocks
        if block.get("has_children", False):
            try:
                pages.extend(get_all_pages(block["id"]))
            except:
                pass

    return pages

# ------------------------
# SLACK
# ------------------------
def send_slack(text):
    if not SLACK_WEBHOOK_URL:
        print("Slack webhook missing")
        return

    resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text})
    print("Slack:", resp.status_code, resp.text)

# ------------------------
# MAIN
# ------------------------
def main():
    print("Scanning Notion deeply…")
    pages = get_all_pages(ROOT_PAGE_ID)
    print(f"Total discovered pages: {len(pages)}")

    new_pages = [p for p in pages if p["created"] > ONE_DAY_AGO]
    print(f"Pages created in last 24h: {len(new_pages)}")

    if not new_pages:
        send_slack("❗ Тест: новых страниц за последние 24 часа не найдено.")
        return

    msg = ["🆕 *Тест: новые страницы (созданы за последние 24 часа):*", ""]
    for p in new_pages:
        msg.append(
            f"📘 *{p['title']}*\n"
            f"🔗 {p['url']}\n"
            f"✍️ {p['author']}\n"
        )

    send_slack("\n".join(msg))

if __name__ == "__main__":
    main()
