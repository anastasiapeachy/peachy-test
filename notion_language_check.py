"""
Notion Language Analyzer
Analyzes language distribution (Russian/English) across Notion workspace pages.
"""

import os
import csv
import re
import time
from typing import List, Dict, Tuple
from datetime import datetime
from notion_client import Client
from notion_client.errors import APIResponseError, HTTPResponseError
from langdetect import detect, LangDetectException

# Configuration
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN environment variable is required")
if not ROOT_PAGE_ID:
    raise ValueError("ROOT_PAGE_ID environment variable is required")


def normalize_id(raw_id: str) -> str:
    """Normalize Notion ID by removing hyphens and extracting 32-char hex."""
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip()
    m = re.search(r"([0-9a-fA-F]{32})", s.replace("-", ""))
    if m:
        return m.group(1)
    return s.replace("-", "")


ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)
notion = Client(auth=NOTION_TOKEN)


def notion_url(page_id: str) -> str:
    """Generate Notion page URL from page ID."""
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"


def get_page_basic_info(page_id: str) -> dict:
    """Return id, title, url, author for a page."""
    page = notion.pages.retrieve(page_id=page_id)
    time.sleep(0.1)

    # Extract title
    title = "(Untitled)"
    props = page.get("properties", {}) or {}
    for prop in props.values():
        if prop.get("type") == "title":
            parts = [t.get("plain_text", "") for t in prop.get("title", [])]
            if parts:
                title = "".join(parts)
                break

    # Extract author
    author_info = page.get("created_by", {}) or {}
    author = author_info.get("name")
    if not author:
        uid = author_info.get("id")
        if uid:
            try:
                user_data = notion.users.retrieve(user_id=uid)
                time.sleep(0.1)
                author = user_data.get("name")
            except Exception as e:
                print(f"  ⚠ Could not fetch user {uid}: {e}")
                author = None
    if not author:
        author = "Unknown"

    return {
        "id": page_id,
        "title": title,
        "url": notion_url(page_id),
        "author": author,
    }


def get_all_pages_from_root(root_page_id: str) -> List[dict]:
    """
    Walk blocks starting from root_page_id and collect ALL child_page pages,
    including those in columns, lists, toggles, etc.
    """
    pages = {}
    visited_blocks = set()

    def walk_blocks(block_or_page_id: str):
        cursor = None
        while True:
            try:
                resp = notion.blocks.children.list(
                    block_id=block_or_page_id,
                    start_cursor=cursor
                )
                time.sleep(0.1)
            except Exception as e:
                print(f"  ⚠ Can't list children for {block_or_page_id}: {e}")
                return

            for block in resp.get("results", []):
                bid = block.get("id")
                if not bid or bid in visited_blocks:
                    continue
                visited_blocks.add(bid)

                btype = block.get("type")

                # child_page = separate page
                if btype == "child_page":
                    page_id = block["id"]
                    if page_id not in pages:
                        try:
                            info = get_page_basic_info(page_id)
                            pages[page_id] = info
                        except Exception as e:
                            print(f"  ⚠ Skipping page {page_id}: {e}")
                    # Recursively walk its content to find sub-pages
                    walk_blocks(page_id)

                # Any block with has_children=True (columns, toggles, etc.)
                if block.get("has_children"):
                    walk_blocks(bid)

            cursor = resp.get("next_cursor")
            if not cursor:
                break

    # Add root page itself
    root_info = get_page_basic_info(root_page_id)
    pages[root_page_id] = root_info

    # Walk its blocks to find all nested pages
    walk_blocks(root_page_id)

    return list(pages.values())


def extract_all_text_from_block(block: dict) -> str:
    """
    Recursively extract ALL readable text from a block,
    including content in columns, lists, callouts, tables, etc.
    """
    texts = []
    btype = block.get("type")
    content = block.get(btype, {}) if btype else {}

    # 1) Generic rich_text
    if isinstance(content, dict) and "rich_text" in content:
        rich_text = content.get("rich_text", [])
        if rich_text:
            texts.append(" ".join(
                t.get("plain_text", "") for t in rich_text if t.get("plain_text")
            ))

    # 2) Explicit types with rich_text
    for key in [
        "paragraph", "heading_1", "heading_2", "heading_3",
        "quote", "callout", "bulleted_list_item",
        "numbered_list_item", "toggle", "to_do"
    ]:
        if btype == key:
            rt = block.get(key, {}).get("rich_text", [])
            if rt:
                texts.append(" ".join(
                    t.get("plain_text", "") for t in rt if t.get("plain_text")
                ))

    # 3) Caption (image, video, file, embed, bookmark, etc.)
    if isinstance(content, dict) and "caption" in content:
        cap = content.get("caption", [])
        if cap:
            texts.append(" ".join(
                t.get("plain_text", "") for t in cap if t.get("plain_text")
            ))

    # 4) Equation
    if btype == "equation":
        eq = block.get("equation", {}).get("expression")
        if eq:
            texts.append(eq)

    # 5) Table - read rows and cells
    if btype == "table":
        try:
            cursor = None
            while True:
                resp = notion.blocks.children.list(
                    block_id=block["id"],
                    start_cursor=cursor
                )
                time.sleep(0.1)
                for row in resp.get("results", []):
                    if row.get("type") == "table_row":
                        cells = row["table_row"].get("cells", [])
                        for cell in cells:
                            if cell:
                                texts.append(" ".join(
                                    t.get("plain_text", "") for t in cell if t.get("plain_text")
                                ))
                cursor = resp.get("next_cursor")
                if not cursor:
                    break
        except Exception:
            pass

    # 6) Synced_block - if referencing original block, read its children
    if btype == "synced_block":
        synced = block.get("synced_block", {})
        sf = synced.get("synced_from")
        if sf and isinstance(sf, dict):
            original_id = sf.get("block_id")
            if original_id:
                try:
                    cursor = None
                    while True:
                        resp = notion.blocks.children.list(
                            block_id=original_id,
                            start_cursor=cursor
                        )
                        time.sleep(0.1)
                        for child in resp.get("results", []):
                            texts.append(extract_all_text_from_block(child))
                        cursor = resp.get("next_cursor")
                        if not cursor:
                            break
                except Exception:
                    pass

    # 7) Recursively walk children of any block with has_children=True
    if block.get("has_children") and btype not in ("synced_block", "table"):
        try:
            cursor = None
            while True:
                resp = notion.blocks.children.list(
                    block_id=block["id"],
                    start_cursor=cursor
                )
                time.sleep(0.1)
                for child in resp.get("results", []):
                    # child_page = separate page, analyzed separately
                    if child.get("type") == "child_page":
                        continue
                    texts.append(extract_all_text_from_block(child))
                cursor = resp.get("next_cursor")
                if not cursor:
                    break
        except Exception:
            pass

    return " ".join(t for t in texts if t).strip()


def detect_lang(text: str) -> str:
    """Detect language of text."""
    try:
        return detect(text)
    except Exception:
        return "unknown"


def count_words(text: str) -> int:
    """Count words in text."""
    return len(re.findall(r"\b\w+\b", text))


def analyze_page_lang(page_id: str) -> Tuple[int, int]:
    """Return (russian_words, english_words) for one page."""
    ru = 0
    en = 0

    cursor = None
    while True:
        try:
            resp = notion.blocks.children.list(
                block_id=page_id,
                start_cursor=cursor
            )
            time.sleep(0.1)
        except Exception as e:
            print(f"  ⚠ Can't read blocks for page {page_id}: {e}")
            break

        for block in resp.get("results", []):
            # child_page = separate page, don't count its text in parent
            if block.get("type") == "child_page":
                continue

            text = extract_all_text_from_block(block)
            if not text:
                continue

            lang = detect_lang(text)
            words = count_words(text)
            if lang == "ru":
                ru += words
            elif lang == "en":
                en += words

        cursor = resp.get("next_cursor")
        if not cursor:
            break

    return ru, en


def main():
    """Main execution function."""
    start_time = time.time()
    print("=" * 70)
    print("🔍 Notion Language Analysis")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Root page ID: {ROOT_PAGE_ID}\n")

    print("📥 Collecting all pages from root...")
    pages = get_all_pages_from_root(ROOT_PAGE_ID)
    print(f"✅ Found {len(pages)} pages (including root)\n")

    print("🔬 Analyzing language distribution...")
    results = []
    
    for idx, page_info in enumerate(pages, 1):
        if idx % 5 == 0:
            elapsed = time.time() - start_time
            rate = idx / elapsed if elapsed > 0 else 0
            eta = (len(pages) - idx) / rate if rate > 0 else 0
            print(f"  📊 Progress: {idx}/{len(pages)} | "
                  f"Rate: {rate:.1f} pages/s | "
                  f"ETA: {eta/60:.1f}m")

        page_id = page_info["id"]
        title = page_info["title"]
        url = page_info["url"]
        author = page_info["author"]

        try:
            ru_words, en_words = analyze_page_lang(page_id)
            total = ru_words + en_words
            ru_pct = (ru_words / total * 100) if total else 0.0
            en_pct = (en_words / total * 100) if total else 0.0

            results.append({
                "Page Title": title,
                "Page URL": url,
                "Author": author,
                "% Russian": round(ru_pct, 2),
                "% English": round(en_pct, 2),
            })
        except Exception as e:
            print(f"  ❌ Error analyzing page {title}: {e}")
            continue

    # Sort by English percentage (descending), then Russian
    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    # Save to CSV
    output_file = "notion_language_percentages.csv"
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["Page Title", "Page URL", "Author", "% Russian", "% English"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    elapsed = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("✨ Analysis Complete")
    print("=" * 70)
    print(f"✅ Successfully analyzed: {len(results)} pages")
    print(f"📄 Output file: {output_file}")
    print(f"⏱️  Total duration: {elapsed/60:.1f} minutes ({elapsed:.1f}s)")
    print(f"⚡ Average speed: {len(pages)/elapsed:.1f} pages/second")
    print("=" * 70)


if __name__ == "__main__":
    main()
