import os
import re
import csv
import time
import json
import math
from notion_client import Client
from langdetect import detect

# ------------------------------------------
# ENV
# ------------------------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing env vars NOTION_TOKEN or ROOT_PAGE_ID")

notion = Client(auth=NOTION_TOKEN)


def clean_id(raw):
    raw = raw.replace("-", "")
    if len(raw) == 32:
        return raw
    m = re.search(r"([0-9a-fA-F]{32})", raw)
    return m.group(1) if m else raw


ROOT_PAGE_ID = clean_id(ROOT_PAGE_ID)


# ===============================================================
# BLOCK FETCHING (pagination-safe)
# ===============================================================
def get_children(block_id):
    out = []
    cursor = None
    while True:
        resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)
        out.extend(resp["results"])
        cursor = resp.get("next_cursor")
        if not cursor:
            break
        time.sleep(0.1)
    return out


# ===============================================================
# FULL TEXT EXTRACTOR (column_list, column, table, synced_block, nested)
# ===============================================================
def extract_page_mentions(block):
    """Return list of page IDs found inside rich_text → mention."""
    pages = []

    btype = block.get("type")
    payload = block.get(btype, {}) or {}

    # 1) rich_text fields
    if "rich_text" in payload:
        for rt in payload["rich_text"]:
            if rt.get("type") == "mention":
                mention = rt.get("mention", {})
                if mention.get("type") == "page":
                    pid = mention["page"]["id"]
                    pages.append(clean_id(pid))

    # 2) captions can also contain mentions
    if "caption" in payload:
        for rt in payload["caption"]:
            if rt.get("type") == "mention":
                mention = rt.get("mention", {})
                if mention.get("type") == "page":
                    pid = mention["page"]["id"]
                    pages.append(clean_id(pid))

    return pages


def get_all_subpages(root_id):
    """Find ALL child pages under root, including pages hidden inside lists/columns."""
    found = set()
    queue = [root_id]

    while queue:
        current = queue.pop()

        for block in get_children(current):
            btype = block.get("type")

            # --- case 1: real child_page block ---
            if btype == "child_page":
                pid = clean_id(block["id"])
                if pid not in found:
                    found.add(pid)
                    queue.append(pid)

            # --- case 2: page mentions inside rich_text ---
            for pid in extract_page_mentions(block):
                if pid not in found:
                    # MUST validate it is actually child of root (strict chain)
                    try:
                        parent = notion.pages.retrieve(page_id=pid)["parent"]
                        if parent.get("type") == "page_id":
                            if clean_id(parent["page_id"]) == current:
                                found.add(pid)
                                queue.append(pid)
                    except:
                        pass

            # --- case 3: go deeper into children ---
            if block.get("has_children"):
                queue.append(clean_id(block["id"]))

    # remove root itself
    found.discard(clean_id(root_id))

    return list(found)


# ===============================================================
# GET TEXT OF ENTIRE PAGE
# ===============================================================
def get_page_text(page_id):
    blocks = get_children(page_id)
    parts = []
    for b in blocks:
        parts.append(extract_text(b))
    return "\n".join(p for p in parts if p).strip()


# ===============================================================
# DETECT LANGUAGE
# ===============================================================
def count_lang(text):
    words_total = len(re.findall(r'\b\w+\b', text))
    if words_total == 0:
        return 0, 0

    lang = "unknown"
    try:
        lang = detect(text)
    except:
        pass

    if lang == "ru":
        return words_total, 0
    if lang == "en":
        return 0, words_total
    return 0, 0


# ===============================================================
# FIND ALL PAGES UNDER ROOT
# ===============================================================
def get_direct_children_pages(pid):
    out = []
    for b in get_children(pid):
        if b["type"] == "child_page":
            out.append(clean_id(b["id"]))
    return out


def get_all_subpages(root_id):
    result = []
    stack = [root_id]
    visited = set()

    while stack:
        pid = stack.pop()
        if pid in visited:
            continue
        visited.add(pid)

        children = get_direct_children_pages(pid)
        result.extend(children)
        stack.extend(children)

    return list(dict.fromkeys(result))  # remove duplicates


# ===============================================================
# MAIN BATCH PROCESS
# ===============================================================
def main():
    pages = get_all_subpages(ROOT_PAGE_ID)
    pages = [p for p in pages if p != ROOT_PAGE_ID]

    print(f"Total pages under root: {len(pages)}")

    batch_size = 20
    batches = [pages[i:i+batch_size] for i in range(0, len(pages), batch_size)]

    all_rows = []

    for bi, batch in enumerate(batches, start=1):
        print(f"Processing batch {bi}/{len(batches)} with {len(batch)} pages")

        for pid in batch:
            try:
                page = notion.pages.retrieve(page_id=pid)
            except:
                print("Skip page, access denied:", pid)
                continue

            # title
            title = "(untitled)"
            for prop in page.get("properties", {}).values():
                if prop.get("type") == "title" and prop.get("title"):
                    title = prop["title"][0]["plain_text"]
                    break

            # author
            author = "(unknown)"
            a = page.get("created_by", {})
            if a.get("name"):
                author = a["name"]
            elif a.get("id"):
                try:
                    u = notion.users.retrieve(user_id=a["id"])
                    author = u.get("name", "(unknown)")
                except:
                    pass

            # text
            text = get_page_text(pid)
            ru, en = count_lang(text)

            total = ru + en
            ru_pct = (ru/total*100) if total else 0
            en_pct = (en/total*100) if total else 0

            all_rows.append({
                "Page Title": title,
                "Page URL": f"https://www.notion.so/{pid}",
                "Author": author,
                "% Russian": round(ru_pct, 2),
                "% English": round(en_pct, 2)
            })

    # sort
    all_rows.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    # write CSV
    with open("notion_language_report.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["Page Title", "Page URL", "Author", "% Russian", "% English"]
        )
        w.writeheader()
        w.writerows(all_rows)

    print("Saved notion_language_report.csv")


if __name__ == "__main__":
    main()
