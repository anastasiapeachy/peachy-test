from notion_client import Client
import json
import os
import time
import requests
from datetime import datetime, timezone, timedelta

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
STORAGE_FILE = "notion_tracker_data/known_pages.json"

notion = Client(auth=NOTION_TOKEN)


def notion_url(page_id):
    clean_id = page_id.replace("-", "")
    return f"https://www.notion.so/{clean_id}"


def get_page_info(page_id):
    page = notion.pages.retrieve(page_id=page_id)
    
    # Extract title
    title = "Untitled"
    if "properties" in page:
        for prop in page["properties"].values():
            if prop["type"] == "title" and prop.get("title"):
                title = prop["title"][0]["plain_text"]
                break
    
    # Get author name
    author_info = page.get("created_by", {})
    author_name = author_info.get("name", "Unknown")
    
    if not author_name or author_name == "Unknown":
        try:
            user_data = notion.users.retrieve(user_id=author_info["id"])
            author_name = user_data.get("name", "Unknown")
        except Exception:
            author_name = "Unknown"
    
    return {
        "id": page_id,
        "title": title,
        "author": author_name,
        "url": notion_url(page_id),
        "created_time": page.get("created_time", ""),
        "is_public": bool(page.get("public_url"))
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
                    # Recursively get subpages
                    pages.extend(get_all_pages(page_id))
                except Exception as e:
                    print(f"Skipping page {page_id}: {e}")
        
        if not response.get("has_more"):
            break
        
        response = notion.blocks.children.list(
            block_id=block_id, 
            start_cursor=response["next_cursor"]
        )
        time.sleep(0.2)
    
    return pages


def load_known_pages():
    os.makedirs(os.path.dirname(STORAGE_FILE), exist_ok=True)
    if os.path.exists(STORAGE_FILE):
        with open(STORAGE_FILE, "r") as f:
            data = json.load(f)
        print(f"Loaded {len(data)} known pages")
        return data
    return []


def save_known_pages(pages):
    os.makedirs(os.path.dirname(STORAGE_FILE), exist_ok=True)
    with open(STORAGE_FILE, "w") as f:
        json.dump(pages, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(pages)} pages")


def send_to_slack(pages):
    if not SLACK_WEBHOOK_URL:
        print("No Slack webhook configured")
        return
    
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"📚 *{len(pages)} new {'article' if len(pages) == 1 else 'articles'} published*"
            }
        },
        {"type": "divider"}
    ]
    
    for page in pages:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*<{page['url']}|{page['title']}>*\n✍️ {page['author']}"
            }
        })
    
    payload = {"blocks": blocks}
    
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        response.raise_for_status()
        print(f"Posted {len(pages)} pages to Slack")
    except Exception as e:
        print(f"Failed to post to Slack: {e}")


def main():
    print("Fetching pages from Notion...")
    current_pages = get_all_pages(ROOT_PAGE_ID)
    
    known_pages = load_known_pages()
    known_ids = {p["id"] for p in known_pages}
    
    # Find new pages
    new_pages = [p for p in current_pages if p["id"] not in known_ids]
    
    if not new_pages:
        print("No new pages found")
        save_known_pages(current_pages)
        return
    
    # Filter: public and older than 7 days
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=1)
    eligible = []
    
    for page in new_pages:
        if not page.get("created_time"):
            continue
        
        try:
            created = datetime.fromisoformat(page["created_time"].replace("Z", "+00:00"))
            if page.get("is_public") and created < cutoff_date:
                eligible.append(page)
        except Exception:
            continue
    
    if eligible:
        print(f"Found {len(eligible)} pages to post")
        send_to_slack(eligible)
        # Add posted pages to known list
        for page in eligible:
            known_pages.append(page)
    else:
        print("No eligible pages (must be public and >1 days old)")
    
    # Only save pages we've actually posted
    save_known_pages(known_pages)


if __name__ == "__main__":
    main()
