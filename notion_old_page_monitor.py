from notion_client import Client
from notion_client.errors import APIResponseError
import os
import time
import requests
from datetime import datetime, timezone, timedelta
import csv
import json
import argparse
import base64


# ======================================================
# Phase 2 argument trigger
# ======================================================
parser = argparse.ArgumentParser()
parser.add_argument("--release-upload-url", default=None)
args = parser.parse_args()

RELEASE_UPLOAD_URL = args.release_upload_url  # triggers Phase 2


# ======================================================
# ENV
# ======================================================
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY")

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN not set")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID not set")
if not GITHUB_TOKEN:
    raise ValueError("GITHUB_TOKEN not set")
if not GITHUB_REPOSITORY:
    raise ValueError("GITHUB_REPOSITORY not set")

notion = Client(auth=NOTION_TOKEN)
ONE_YEAR_AGO = datetime.now(timezone.utc) - timedelta(days=365)


# ======================================================
# Safe Notion wrapper
# ======================================================
def safe_request(func, *args, **kwargs):
    retries = 7
    delay = 0.25
    backoff = 1

    for _ in range(retries):
        try:
            time.sleep(delay)
            return func(*args, **kwargs)
        except APIResponseError as e:
            if e.status == 429:
                wait = int(getattr(e, "headers", {}).get("Retry-After", 1))
                print(f"[429] Rate limit → {wait}s")
                time.sleep(wait)
                continue
            if 500 <= e.status <= 599:
                print(f"[{e.status}] Retry in {backoff}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 20)
                continue
            raise
    raise RuntimeError("Notion retries exceeded")


# ======================================================
# Notion helpers
# ======================================================
def notion_url(page_id):
    return f"https://www.notion.so/{page_id.replace('-', '')}"


def get_page_info(page_id):
    page = safe_request(notion.pages.retrieve, page_id=page_id)
    title = "Untitled"

    for prop in page.get("properties", {}).values():
        if prop["type"] == "title" and prop.get("title"):
            title = prop["title"][0]["plain_text"]
            break

    last = datetime.fromisoformat(
        page["last_edited_time"].replace("Z", "+00:00")
    )

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "last_edited": last
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
        blocks.extend(resp["results"])
        cursor = resp.get("next_cursor")
        if not cursor:
            break
        time.sleep(0.1)
    return blocks


def is_empty_page(page_id):
    try:
        kids = get_block_children(page_id)
        return len(kids) == 0
    except:
        return False


# ======================================================
# Full Notion traversal
# ======================================================
def get_all_pages(block_id):
    pages = []
    for block in get_block_children(block_id):
        btype = block["type"]

        # Page
        if btype == "child_page":
            pid = block["id"]
            try:
                info = get_page_info(pid)
                pages.append(info)
                pages.extend(get_all_pages(pid))
            except:
                pass

        # Database
        if btype == "child_database":
            db_id = block["id"]
            try:
                resp = safe_request(notion.databases.query, database_id=db_id)
                for row in resp["results"]:
                    pid = row["id"]
                    if is_empty_page(pid):
                        print(f"Skip empty DB row {pid}")
                        continue
                    info = get_page_info(pid)
                    pages.append(info)
                    pages.extend(get_all_pages(pid))
            except:
                pass

        # Nested blocks
        if block.get("has_children") and btype not in ("child_page", "child_database"):
            try:
                pages.extend(get_all_pages(block["id"]))
            except:
                pass

    return pages


# ======================================================
# GitHub Release upload (public assets)
# ======================================================
def upload_to_release_and_get_public_url(filepath):
    owner, repo = GITHUB_REPOSITORY.split("/")

    # 1) Find/create release
    rel_name = "notion-old-pages"
    print("Ensuring release exists...")

    # Get release by tag
    rel = requests.get(
        f"https://api.github.com/repos/{owner}/{repo}/releases/tags/{rel_name}",
        headers={"Authorization": f"Bearer {GITHUB_TOKEN}"}
    )

    if rel.status_code == 404:
        print("Creating release...")
        rel = requests.post(
            f"https://api.github.com/repos/{owner}/{repo}/releases",
            headers={"Authorization": f"Bearer {GITHUB_TOKEN}"},
            json={"tag_name": rel_name, "name": rel_name, "draft": False, "prerelease": False}
        )

    release = rel.json()
    upload_url = release["upload_url"].split("{")[0]

    print("Uploading CSV asset...")

    with open(filepath, "rb") as f:
        content = f.read()

    resp = requests.post(
        f"{upload_url}?name=notion_old_pages.csv",
        headers={
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Content-Type": "text/csv"
        },
        data=content
    )

    asset = resp.json()
    public_url = asset["browser_download_url"]

    print("Public URL:", public_url)
    return public_url


# ======================================================
# Slack notify
# ======================================================
def slack_notify(public_url, total):
    if not SLACK_WEBHOOK_URL:
        print("Slack webhook missing — skip")
        return

    message = (
        f"📄 Найдено *{total}* страниц, которые не редактировались больше года.\n"
        f"📎 CSV: {public_url}"
    )

    resp = requests.post(
        SLACK_WEBHOOK_URL,
        json={"text": message}
    )

    print("Slack response:", resp.status_code, resp.text)


# ======================================================
# Phase 1 — Notion scan
# ======================================================
def generate_csv_and_count():
    pages = get_all_pages(ROOT_PAGE_ID)
    print(f"Total pages: {len(pages)}")

    old = [
        p for p in pages if p["last_edited"] < ONE_YEAR_AGO
    ]

    old.sort(key=lambda x: x["last_edited"])
    print(f"Old pages: {len(old)}")

    with open("notion_old_pages.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["title", "last_edited", "url"])
        for p in old:
            w.writerow([p["title"], p["last_edited"].isoformat(), p["url"]])

    with open("notion_old_pages_count.json", "w") as f:
        json.dump({"count": len(old)}, f)


# ======================================================
# Phase 2 — Slack via public GitHub Release URL
# ======================================================
def phase2():
    with open("notion_old_pages_count.json") as f:
        total = json.load(f)["count"]

    if total == 0:
        print("No old pages → no Slack")
        return

    url = upload_to_release_and_get_public_url("notion_old_pages.csv")
    slack_notify(url, total)


# ======================================================
# MAIN
# ======================================================
if RELEASE_UPLOAD_URL:
    phase2()
else:
    generate_csv_and_count()
