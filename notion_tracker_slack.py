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
                title = block["child]()
