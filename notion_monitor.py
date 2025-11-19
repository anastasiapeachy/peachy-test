import os
import csv
import time
import requests
from datetime import datetime, timedelta, timezone
from notion_client import Client
from notion_client.errors import APIResponseError, HTTPResponseError

# ========================
# ENV
# ========================

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")  # если хочешь использовать — webhook может игнорировать его

if not all([NOTION_TOKEN, ROOT_PAGE_ID, SLACK_WEBHOOK_URL]):
    raise ValueError("Missing required environment variables.")

notion = Client(auth=NOTION_TOKEN)

NOW = datetime.now(timezone.utc)
ONE_YEAR_AGO = NOW - timedelta(days=365)


# ========================
# HELPERS
# ========================

def normalize_id(raw: str):
    raw = raw.strip()
    if "/" in raw:
        raw = raw.split("/")[-1]
    return raw.replace("-", "")

ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)


def safe_request(func, *args, **kwargs):
    """
    ЖЕЛЕЗОБЕТОННЫЙ запрос к Notion API.
    Переживает 429/500/502/503/504/timeout/connection reset.
    """
    max_retries = 15
    backoff = 1

    for _ in range(max_retries):
        try:
            return func(*args, **kwargs)

        except (APIResponseError, HTTPResponseError) as e:
            status = getattr(e, "status", None)

            # Rate limit
            if status == 429:
                retry = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Waiting {retry}s…")
                time.sleep(retry)
                continue

            # Server issues
            if status and 500 <= status <= 599:
                print(f"[{status}] Server error — retry in {backoff}s…")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            print(f"[NETWORK] {e} — retry in {backoff}s…")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

        except Exception as e:
            print(f"[UNKNOWN ERROR] {e} — retry in {backoff}s…")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    raise RuntimeError("Too many retries — Notion still failing.")


# ========================
# NOTION RECURSIVE SCAN
# ========================

def get_block_children(block_id: str):
    blocks = []
    cursor = None

    while True:
        response = safe_request(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor
        )
        blocks.extend(response.get("results", []))
        cursor = response.get("next_cursor")
        if not cursor:
            break

    return blocks


def get_page_info(page):
    props = page.get("properties", {})
    title_prop = props.get("title") or props.get("Name")

    if title_prop and title_prop.get("title"):
        title = "".join(t["plain_text"] for t in title_prop["title"])
    else:
        title = "(untitled)"

    last_raw = page["last_edited_time"].replace("Z", "+00:00")
    last_edited = datetime.fromisoformat(last_raw).astimezone(timezone.utc)

    url = f"https://notion.so/{page['id'].replace('-', '')}"

    return title, last_edited, url


def scan(container_id, out_list):
    children = get_block_children(container_id)

    for block in children:
        t = block["type"]

        # Child page
        if t == "child_page":
            p = safe_request(notion.pages.retrieve, block["id"])
            out_list.append(p)
            scan(block["id"], out_list)

        # Any container block
        if block.get("has_children") and t != "child_page":
            scan(block["id"], out_list)


# ========================
# SLACK WEBHOOK MESSAGE
# ========================

def send_slack_message(old_pages):
    total = len(old_pages)

    if total == 0:
        text = (
            f"🎉 *Отчёт Notion*\n"
            f"Нет страниц, которые не редактировались больше года! Все страницы свежие 🌱"
        )
    else:
        # Показываем только первые 20, чтобы Slack не обрезал сообщение
        top = old_pages[:20]

        rows = "\n".join(
            f"- *{p['title']}* — `{p['last_edited']}`" for p in top
        )

        if total > 20:
            rows += f"\n… и ещё *{total - 20}* страниц"

        text = (
            f"📄 *Отчёт Notion*\n"
            f"Найдено *{total}* страниц, которые не редактировали больше года.\n\n"
            f"*Старейшие страницы:*\n{rows}"
        )

    payload = {"text": text}

    resp = requests.post(SLACK_WEBHOOK_URL, json=payload)
    if resp.status_code != 200:
        raise RuntimeError(f"Slack webhook failed: {resp.text}")

    print("Slack message sent ✔")


# ========================
# MAIN
# ========================

def main():
    print("Scanning Notion recursively…")

    pages = []
    scan(ROOT_PAGE_ID, pages)

    print(f"Found pages: {len(pages)}")

    old_pages = []

    for p in pages:
        title, last_edit, url = get_page_info(p)

        if last_edit < ONE_YEAR_AGO:
            old_pages.append({
                "title": title,
                "last_edited": last_edit.isoformat(),
                "url": url
            })

        time.sleep(0.05)

    old_pages.sort(key=lambda x: x["last_edited"])

    print(f"Old pages: {len(old_pages)}")

    # Save CSV (GitHub artifact)
    with open("notion_old_pages.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["title", "last_edited", "url"])
        for r in old_pages:
            w.writerow([r["title"], r["last_edited"], r["url"]])

    print("CSV saved (local)")

    send_slack_message(old_pages)


if __name__ == "__main__":
    main()
