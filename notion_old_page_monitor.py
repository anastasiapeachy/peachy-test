from notion_client import Client
from notion_client.errors import APIResponseError
import os
import time
import requests
from datetime import datetime, timezone, timedelta
import csv
import json
import argparse

# ======================================================
# PHASE 2 argument: --artifact-url
# ======================================================
parser = argparse.ArgumentParser()
parser.add_argument("--artifact-url", default=None)
args = parser.parse_args()

ARTIFACT_URL = args.artifact_url

# ======================================================
# ENVIRONMENT
# ======================================================
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
# SAFE REQUEST (429, 5xx retry)
# ======================================================
def safe_request(func, *args, **kwargs):
    max_retries = 8
    delay = 0.3
    backoff = 1

    for attempt in range(max_retries):
        try:
            time.sleep(delay)
            return func(*args, **kwargs)
        except APIResponseError as e:
            status = e.status

            # Rate limit
            if status == 429:
                retry_after = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limit exceeded. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue

            # Server errors
            if 500 <= status <= 599:
                print(f"[{status}] Server error. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue

            raise

    raise RuntimeError("Notion API not responding after retries")

# ======================================================
# HELPERS
# ======================================================
def notion_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"

def get_page_info(page_id):
    page = safe_request(notion.pages.retrieve, page_id=page_id)

    # Extract title
    title = "Untitled"
    if "properties" in page:
        for prop in page["properties"].values():
            if prop["type"] == "title" and prop.get("title"):
                title = prop["title"][0]["plain_text"]
                break

    # ISO date -> datetime
    last_raw = page.get("last_edited_time", "")
    last_dt = datetime.fromisoformat(last_raw.replace("Z", "+00:00")).astimezone(timezone.utc)

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "last_edited": last_dt,
    }

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

def get_database_pages(database_id):
    pages = []
    cursor = None

    while True:
        resp = safe_request(
            notion.databases.query,
            database_id=database_id,
            start_cursor=cursor
        )
        pages.extend(resp.get("results", []))
        cursor = resp.get("next_cursor")
        if not cursor:
            break
        time.sleep(0.1)

    return pages

# ======================================================
# EMPTY PAGE CHECK (for database rows)
# ======================================================
def is_empty_page(page_id):
    """
    Страница считается пустой, если нет ни одного блока контента.
    Наличие title НЕ делает страницу непустой.
    """
    try:
        children = get_block_children(page_id)
        if len(children) == 0:
            return True
        return False
    except Exception:
        return False

# ======================================================
# FULL RECURSIVE SCAN
# ======================================================
def get_all_pages(block_id):
    pages = []
    children = get_block_children(block_id)

    for block in children:
        btype = block["type"]

        # -------------------------
        # child_page
        # -------------------------
        if btype == "child_page":
            pid = block["id"]
            try:
                info = get_page_info(pid)
                pages.append(info)
                pages.extend(get_all_pages(pid))
            except Exception as e:
                print(f"Skip child_page {pid}: {e}")

        # -------------------------
        # child_database
        # -------------------------
        elif btype == "child_database":
            db_id = block["id"]
            try:
                db_pages = get_database_pages(db_id)
                for db_page in db_pages:
                    pid = db_page["id"]

                    # ⛔ PRO SKIP: empty database pages
                    try:
                        if is_empty_page(pid):
                            print(f"Skip empty database page: {pid}")
                            continue
                    except Exception:
                        pass

                    try:
                        info = get_page_info(pid)
                        pages.append(info)
                        pages.extend(get_all_pages(pid))
                    except Exception as e:
                        print(f"Skip db page {pid}: {e}")
            except Exception as e:
                print(f"Skip child_database {db_id}: {e}")

        # -------------------------
        # ANY nested block with children
        # -------------------------
        if block.get("has_children") and btype not in ("child_page", "child_database"):
            try:
                pages.extend(get_all_pages(block["id"]))
            except Exception as e:
                print(f"Skip nested block {block['id']}: {e}")

    return pages

# ======================================================
# SLACK WEBHOOK
# ======================================================
def send_slack_webhook(total, artifact_url):
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL missing, skipping Slack.")
        return

    text = (
        f"📄 Найдено *{total}* страниц, которые не редактировались больше года.\n"
        f"📎 CSV отчёт: {artifact_url}"
    )

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
        if resp.status_code != 200:
            print(f"Slack error: {resp.status_code} {resp.text}")
        else:
            print("Slack message sent.")
    except Exception as e:
        print(f"Slack webhook error: {e}")

# ======================================================
# PHASE 1 — SCAN & CSV
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

    old_pages.sort(key=lambda x: x["last_edited"])
    print(f"Old pages found: {len(old_pages)}")

    with open("notion_old_pages.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["title", "last_edited", "url"])
        for p in old_pages:
            w.writerow([p["title"], p["last_edited"], p["url"]])

    with open("notion_old_pages_count.json", "w", encoding="utf-8") as f:
        json.dump({"count": len(old_pages)}, f, ensure_ascii=False)

    print("CSV saved.")

# ======================================================
# PHASE 2 — SLACK
# ======================================================
def notify_slack():
    try:
        with open("notion_old_pages_count.json", "r", encoding="utf-8") as f:
            total = json.load(f)["count"]
    except FileNotFoundError:
        print("no CSV count file. Skip Slack.")
        return

    send_slack_webhook(total, ARTIFACT_URL)

# ======================================================
# MAIN
# ======================================================
if ARTIFACT_URL:
    notify_slack()
else:
    generate_csv_and_count()
