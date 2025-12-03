"""
Notion Language Analyzer
Analyzes language distribution (Russian/English) across Notion workspace pages.
"""

import os
import csv
import re
import time
from typing import List, Dict, Tuple, Optional
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

notion = Client(auth=NOTION_TOKEN)

# Caches
BLOCK_CACHE = {}
VISITED_PAGES = set()


def safe_request(func, *args, **kwargs):
    """Execute Notion API request with exponential backoff retry logic."""
    max_retries = 7
    base_delay = 1

    for attempt in range(max_retries):
        try:
            result = func(*args, **kwargs)
            time.sleep(0.15)  # Rate limiting buffer
            return result

        except APIResponseError as e:
            status = getattr(e, "status", None)
            code = getattr(e, "code", None)

            if status == 429 or code == "rate_limited":
                retry_after = getattr(e, "headers", {}).get("Retry-After")
                wait_time = int(retry_after) if retry_after else (attempt + 1) * 2
                print(f"Rate limited. Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                continue

            if status and 500 <= status <= 599:
                wait_time = min(base_delay * (2 ** attempt), 10)
                print(f"Server error ({status}). Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue

            raise

        except HTTPResponseError:
            wait_time = min(base_delay * (2 ** attempt), 10)
            print(f"HTTP error. Retrying in {wait_time}s...")
            time.sleep(wait_time)

    raise RuntimeError("Notion API failed after maximum retries")


def normalize_id(raw_id: str) -> str:
    """Normalize Notion ID by removing hyphens and extracting 32-char hex."""
    if not isinstance(raw_id, str):
        return raw_id
    
    cleaned = raw_id.strip().replace("-", "")
    match = re.search(r"([0-9a-fA-F]{32})", cleaned)
    return match.group(1) if match else cleaned


def make_url(page_id: str) -> str:
    """Generate Notion page URL from page ID."""
    return f"https://www.notion.so/{page_id.replace('-', '')}"


def get_page(page_id: str) -> dict:
    """Retrieve page metadata from Notion."""
    return safe_request(notion.pages.retrieve, page_id=page_id)


def get_page_title(page: dict) -> str:
    """Extract title from page properties."""
    props = page.get("properties", {})
    for prop in props.values():
        if prop.get("type") == "title":
            title_parts = prop.get("title", [])
            return "".join([t.get("plain_text", "") for t in title_parts])
    return "(Untitled)"


def get_children(block_id: str) -> List[dict]:
    """Fetch all immediate children of a block."""
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


def query_database(db_id: str, cursor: Optional[str] = None) -> dict:
    """Query a Notion database with pagination support."""
    params = {"database_id": db_id, "page_size": 100}
    if cursor:
        params["start_cursor"] = cursor
    return safe_request(notion.databases.query, **params)


def get_blocks_recursive(block_id: str) -> List[dict]:
    """Recursively fetch all nested blocks with caching."""
    if block_id in BLOCK_CACHE:
        return BLOCK_CACHE[block_id]

    blocks = []
    cursor = None

    while True:
        response = safe_request(
            notion.blocks.children.list,
            block_id=block_id,
            start_cursor=cursor
        )

        for block in response.get("results", []):
            blocks.append(block)
            if block.get("has_children"):
                blocks.extend(get_blocks_recursive(block["id"]))

        if not response.get("has_more"):
            break
        cursor = response.get("next_cursor")

    BLOCK_CACHE[block_id] = blocks
    return blocks


def extract_rich_text(rich_text_list: List[dict]) -> str:
    """Extract plain text from Notion rich text objects."""
    parts = []
    for rt in rich_text_list:
        if isinstance(rt, dict):
            text = rt.get("plain_text")
            if text:
                parts.append(text)
    return " ".join(parts)


def extract_block_text(block: dict) -> str:
    """Extract text content from any Notion block type."""
    block_type = block.get("type")
    if not block_type:
        return ""
    
    data = block.get(block_type, {})

    # Standard rich text blocks
    if "rich_text" in data:
        text = extract_rich_text(data["rich_text"])
        if text.strip():
            return text

    # Captions (images, videos, etc.)
    if "caption" in data:
        text = extract_rich_text(data["caption"])
        if text.strip():
            return text

    # Table rows
    if block_type == "table_row":
        cell_texts = [extract_rich_text(cell) for cell in data.get("cells", [])]
        combined = " ".join(cell_texts).strip()
        if combined:
            return combined

    # Fallback for any string values
    if isinstance(data, dict):
        for value in data.values():
            if isinstance(value, str) and value.strip():
                return value

    return ""


def page_has_content(page_id: str) -> bool:
    """Check if page contains any readable text content."""
    blocks = get_blocks_recursive(page_id)

    for block in blocks:
        text = extract_block_text(block)
        if text.strip():
            return True

    # Fallback: check if title has content
    page = get_page(page_id)
    title = get_page_title(page)
    return bool(title.strip())


def collect_all_pages(root_id: str) -> List[str]:
    """Recursively collect all page IDs in workspace."""
    root_id = normalize_id(root_id)
    
    if root_id in VISITED_PAGES:
        return []
    
    VISITED_PAGES.add(root_id)
    pages = []
    children = get_children(root_id)

    for block in children:
        block_type = block.get("type")

        if block_type == "child_page":
            page_id = normalize_id(block["id"])
            pages.append(page_id)
            pages.extend(collect_all_pages(page_id))

        elif block_type == "child_database":
            db_id = block["id"]
            cursor = None

            while True:
                response = query_database(db_id, cursor)
                for row in response["results"]:
                    page_id = row["id"]
                    pages.append(page_id)
                    pages.extend(collect_all_pages(page_id))

                cursor = response.get("next_cursor")
                if not cursor:
                    break

        elif block.get("has_children") and block_type not in ("child_page", "child_database"):
            pages.extend(collect_all_pages(block["id"]))

    return pages


def detect_language(text: str) -> str:
    """Detect language of text using langdetect."""
    try:
        return detect(text)
    except LangDetectException:
        return "unknown"


def count_words(text: str) -> int:
    """Count words in text."""
    return len(re.findall(r"\b\w+\b", text))


def analyze_page_language(page_id: str) -> Tuple[int, int, bool]:
    """
    Analyze language distribution in a page.
    Returns: (russian_words, english_words, has_no_readable_text)
    """
    russian_words = 0
    english_words = 0

    blocks = get_blocks_recursive(page_id)

    for block in blocks:
        text = extract_block_text(block)
        if not text.strip():
            continue

        word_count = count_words(text)
        if word_count == 0:
            continue

        language = detect_language(text)

        if language == "ru":
            russian_words += word_count
        elif language == "en":
            english_words += word_count

    has_no_text = (russian_words + english_words) == 0
    return russian_words, english_words, has_no_text


def main():
    """Main execution function."""
    start_time = time.time()
    print("=" * 60)
    print("Notion Language Analysis")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    print("Collecting pages from workspace...")
    root_normalized = normalize_id(ROOT_PAGE_ID)
    page_ids = list(dict.fromkeys(collect_all_pages(root_normalized)))
    print(f"✓ Found {len(page_ids)} total pages\n")

    print("Analyzing language distribution...")
    results = []
    skipped_count = 0

    for idx, page_id in enumerate(page_ids, 1):
        if idx % 10 == 0:
            print(f"  Progress: {idx}/{len(page_ids)} pages analyzed")

        if not page_has_content(page_id):
            skipped_count += 1
            continue

        page = get_page(page_id)
        title = get_page_title(page)
        url = make_url(page_id)
        author = page.get("created_by", {}).get("name", "Unknown")

        russian_words, english_words, unreadable = analyze_page_language(page_id)

        if unreadable:
            continue

        total_words = russian_words + english_words
        russian_pct = (russian_words * 100 / total_words) if total_words else 0
        english_pct = (english_words * 100 / total_words) if total_words else 0

        results.append({
            "Page Title": title,
            "Page URL": url,
            "Author": author,
            "% Russian": round(russian_pct, 2),
            "% English": round(english_pct, 2)
        })

    # Sort by English percentage (descending), then Russian
    results.sort(key=lambda x: (x["% English"], x["% Russian"]), reverse=True)

    output_file = "notion_language_percentages.csv"
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        fieldnames = ["Page Title", "Page URL", "Author", "% Russian", "% English"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    elapsed = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("Analysis Complete")
    print("=" * 60)
    print(f"✓ Analyzed pages: {len(results)}")
    print(f"✓ Skipped (empty): {skipped_count}")
    print(f"✓ Output file: {output_file}")
    print(f"✓ Duration: {elapsed:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
