from notion_client import Client
import json
import os
import time
import requests

# Settings
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
STORAGE_FILE = "notion_tracker_data/known_pages.json"

notion = Client(auth=NOTION_TOKEN)


def notion_url(page_id: str) -> str:
    ""Generate page URL."""
    clean_id = page_id.replace("-", "")
    return f"https://www.notion.so/{clean_id}"


def get_page_info(page_id):
    """Get page info."""
    page = notion.pages.retrieve(page_id=page_id)
    title = None

    # Get a page title
    if "properties" in page:
        for prop in page["properties"].values():
            if prop["type"] == "title" and prop["title"]:
                title = prop["title"][0]["plain_text"]
                break

    if not title:
        title = page.get("object", "Unknown page.")

    author_info = page.get("created_by", {})
    author_name = author_info.get("name") or author_info.get("id", "Unknown")

    # Get author's name if it is not added.
    if author_name == author_info.get("id"):
        try:
            user_data = notion.users.retrieve(user_id=author_info["id"])
            author_name = user_data.get("name") or user_data.get("id")
        except Exception:
            print(f"Can't get author's name {page_id}: {e}")
            author_name = "Unknown"

    return {
        "id": page_id,
        "title": title,
        "author": author_name,
        "url": notion_url(page_id),
    }


def get_all_pages_recursively(block_id):
    """Get all pages and subpages"""
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
                    info = {"id": page_id, "title": title, "author": "?", "url": notion_url(page_id)}
                pages_info.append(info)
                pages_info.extend(get_all_pages_recursively(page_id))

        if not response.get("has_more"):
            break
        response = notion.blocks.children.list(block_id=block_id, start_cursor=response["next_cursor"])
        time.sleep(0.2)

    return pages_info


def load_known_pages():
    """Return known pages."""
    os.makedirs(os.path.dirname(STORAGE_FILE), exist_ok=True)
    if os.path.exists(STORAGE_FILE):
        with open(STORAGE_FILE, "r") as f:
            return json.load(f)
            print(f"{len(data)} known pages are loaded.")
    return []


def save_known_pages(pages):
    """Saving a list of known pages."""
    os.makedirs(os.path.dirname(STORAGE_FILE), exist_ok=True)
    with open(STORAGE_FILE, "w") as f:
        json.dump(pages, f, indent=2, ensure_ascii=False)
        print(f"{len(pages)} pages are saved in {STORAGE_FILE}")


def send_to_slack(message):
    """Sending a message to Slack."""
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL не задан.")
        return
    requests.post(SLACK_WEBHOOK_URL, json={"text": message})


def main():
    known = load_known_pages()
    current = get_all_pages_recursively(ROOT_PAGE_ID)

    known_ids = {p["id"] for p in known}
    new_pages = [p for p in current if p["id"] not in known_ids]

    if new_pages:
        message_lines = ["*New articles in Notion:*", ""]
        for p in new_pages:
            message_lines.append(
                f":blue_book: *{p['title']}*\n"
                f":link: {p['url']}\n"
                f":writing_hand: {p['author']}\n\n"
            )
        message = "\n".join(message_lines)
        send_to_slack(message)
        save_known_pages(current)
    else:
        send_to_slack("No new pages.")
        # потом это надо удалить!!!


if __name__ == "__main__":
    main()
