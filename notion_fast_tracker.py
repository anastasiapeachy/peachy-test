from notion_client import Client
import os
import requests
from datetime import datetime, timezone, timedelta

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

notion = Client(auth=NOTION_TOKEN)

# ----------------------------------
# Fast search — get ALL pages
# ----------------------------------
def fast_search_all_pages():
    pages = []
    cursor = None

    while True:
        resp = notion.search(
            query="",
            filter={"property": "object", "value": "page"},
            start_cursor=cursor,
            page_size=100
        )
        pages.extend(resp["results"])

        cursor = resp.get("next_cursor")
        if not cursor:
            break

    return pages


# ----------------------------------
# Check if page belongs to subtree
# ----------------------------------
def belongs_to_root(page):
    parent = page.get("parent", {})

    while parent:
        if parent.get("type") == "page_id" and parent["page_id"] == ROOT_PAGE_ID:
            return True
        if parent.get("type") == "workspace":
            return False
        if parent.get("type") == "database_id":
            # lookup DB parent
            db = notion.databases.retrieve(database_id=parent["database_id"])
            parent = db["parent"]
        elif parent.get("type") == "page_id":
            page_data = notion.pages.retrieve(page_id=parent["page_id"])
            parent = page_data["parent"]
        else:
            return False

    return False


# ----------------------------------
# Send Slack message
# ----------------------------------
def send_to_slack(text):
    if not SLACK_WEBHOOK_URL:
        print("No Slack webhook!")
        return
    requests.post(SLACK_WEBHOOK_URL, json={"text": text})


# ----------------------------------
# MAIN
# ----------------------------------
def main():
    all_pages = fast_search_all_pages()
    print(f"Всего страниц найдено в Notion: {len(all_pages)}")

    pages = [p for p in all_pages if belongs_to_root(p)]
    print(f"Страниц в ROOT_SECTION: {len(pages)}")

    # Example: pages older than 7 days
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    eligible = []
    for p in pages:
        created_raw = p.get("created_time")
        created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))

        public = p.get("public_url") is not None

        if public and created_dt < cutoff:
            eligible.append(p)

    print(f"Eligible pages for Slack: {len(eligible)}")

    if not eligible:
        send_to_slack("Нет новых статей.")
        return

    msg = "*Новые статьи (старше 7 дней):*\n\n"
    for p in eligible:
        title = p["properties"]["title"]["title"][0]["plain_text"] if p["properties"]["title"]["title"] else "Без названия"
        url = p["public_url"] or f"https://www.notion.so/{p['id'].replace('-', '')}"
        msg += f"📘 *{title}*\n{url}\n\n"

    send_to_slack(msg)


if __name__ == "__main__":
    main()
