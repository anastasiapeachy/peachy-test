from notion_client import Client
import json
import os
import time
import requests
from datetime import datetime, timezone, timedelta

# === Settings ===
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
STORAGE_FILE = "notion_tracker_data/known_pages.json"

notion = Client(auth=NOTION_TOKEN)


def notion_url(page_id: str) -> str:
    """Generate page URL."""
    clean_id = page_id.replace("-", "")
    return f"https://www.notion.so/{clean_id}"


def get_page_info(page_id):
    """Get page info (title, author, created_time, is_public)."""
    page = notion.pages.retrieve(page_id=page_id)
    title = "Untitled"

    # Try to extract title from properties
    if "properties" in page:
        for prop in page["properties"].values():
            if prop["type"] == "title" and prop["title"]:
                title = prop["title"][0]["plain_text"]
                break

    author_info = page.get("created_by", {})
    author_name = author_info.get("name") or author_info.get("id", "Unknown")

    # Try to get author's name
    if author_name == author_info.get("id"):
        try:
            user_data = notion.users.retrieve(user_id=author_info["id"])
            author_name = user_data.get("name") or user_data.get("id")
        except Exception as e:
            print(f"Can't get author's name {page_id}: {e}")
            author_name = "Unknown"

    created_time = page.get("created_time", "")
    last_edited_time = page.get("last_edited_time", "")

    # Check if page is public
    is_public = False
    try:
        # pages.retrieve may contain public_url if shared
        if page.get("public_url"):
            is_public = True
    except Exception:
        pass

    return {
        "id": page_id,
        "title": title,
        "author": author_name,
        "url": notion_url(page_id),
        "created_time": created_time,
        "last_edited_time": last_edited_time,
        "is_public": is_public,
    }


def get_all_pages_recursively(block_id):
    """Get all pages and subpages recursively."""
    pages_info = []
    response = notion.blocks.children.list(block_id=block_id)

    while True:
        for block in response["results"]:
            if block["type"] == "child_page":
                page_id = block["id"]
                title = block["child_page"]["title"]
                try:
                    info = get_page_info(page_id)
                except Exception as e:
                    print(f"Error while getting a page {title} ({page_id}): {e}")
                    info = {
                        "id": page_id,
                        "title": title,
                        "author": "?",
                        "url": notion_url(page_id),
                        "created_time": "",
                        "last_edited_time": "",
                        "is_public": False,
                    }
                pages_info.append(info)
                # Check subpages
                pages_info.extend(get_all_pages_recursively(page_id))

        if not response.get("has_more"):
            break
        response = notion.blocks.children.list(
            block_id=block_id, start_cursor=response["next_cursor"]
        )
        time.sleep(0.2)

    return pages_info


def load_known_pages():
    """Return known pages."""
    os.makedirs(os.path.dirname(STORAGE_FILE), exist_ok=True)
    if os.path.exists(STORAGE_FILE):
        with open(STORAGE_FILE, "r") as f:
            data = json.load(f)
        print(f"{len(data)} known pages are loaded.")
        return data
    return []


def save_known_pages(pages):
    """Saving a list of known pages."""
    os.makedirs(os.path.dirname(STORAGE_FILE), exist_ok=True)
    with open(STORAGE_FILE, "w") as f:
        json.dump(pages, f, indent=2, ensure_ascii=False)
        print(f"{len(pages)} pages are saved in {STORAGE_FILE}")


def send_to_slack(message):
    """Send a message to Slack."""
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL is not set.")
        return
    requests.post(SLACK_WEBHOOK_URL, json={"text": message})


def main():
    known = load_known_pages()
    current = get_all_pages_recursively(ROOT_PAGE_ID)

    known_ids = {p["id"] for p in known}
    new_pages = [p for p in current if p["id"] not in known_ids]

    # === фильтруем ===
    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    eligible_pages = []

    for p in new_pages:
        if not p.get("created_time"):
            continue
        try:
            created_dt = datetime.fromisoformat(p["created_time"].replace("Z", "+00:00"))
        except Exception:
            continue
        if p.get("is_public") and created_dt < week_ago:
            eligible_pages.append(p)

    # === отправляем только новые и подходящие ===
    if eligible_pages:
        message_lines = ["*🆕 New public articles (older than 7 days):*", ""]
        for p in eligible_pages:
            message_lines.append(
                f":blue_book: *{p['title']}*\n"
                f":link: {p['url']}\n"
                f":writing_hand: {p['author']}\n"
            )
        message = "\n".join(message_lines)
        send_to_slack(message)
    else:
        print("No new public pages older than 7 days.")

    save_known_pages(current)


if __name__ == "__main__":
    main()
