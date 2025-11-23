import os
import re
from notion_client import Client
from langdetect import detect

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
notion = Client(auth=NOTION_TOKEN)

PAGE_IDS = [
    "d3848d6caa5c444a801993d7af5f3cca",
    "6781d00a0aae41e8ab8fa0d114d52074"
]


# ================================
# BASIC HELPERS
# ================================
def normalize_id(s):
    return s.replace("-", "").strip()


def get_blocks(block_id):
    blocks = []
    cursor = None
    while True:
        resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)
        blocks.extend(resp["results"])
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return blocks


def print_tree(block, indent=0):
    pad = " " * indent
    btype = block["type"]
    print(f"{pad}- {btype} ({block['id']})")

    # Show text if exists
    text = extract_text(block)
    if text:
        print(f"{pad}   text: {text[:120]}")

    if block.get("has_children"):
        children = get_blocks(block["id"])
        for child in children:
            print_tree(child, indent + 4)


# ================================
# TEXT EXTRACTION
# ================================
def extract_text(block):
    parts = []

    btype = block["type"]
    data = block.get(btype, {})

    # rich_text content
    if isinstance(data, dict) and "rich_text" in data:
        parts.append(" ".join(t.get("plain_text", "") for t in data["rich_text"]))

    # caption
    if isinstance(data, dict) and "caption" in data:
        parts.append(" ".join(t.get("plain_text", "") for t in data["caption"]))

    # synced_block → load original
    if btype == "synced_block":
        sf = data.get("synced_from")
        if sf and sf.get("block_id"):
            orig = sf["block_id"]
            children = get_blocks(orig)
            for c in children:
                parts.append(extract_text(c))

    # table
    if btype == "table":
        rows = get_blocks(block["id"])
        for row in rows:
            cells = row.get("table_row", {}).get("cells", [])
            for cell in cells:
                parts.append(" ".join(t.get("plain_text", "") for t in cell))

    # recursion for child blocks
    if block.get("has_children"):
        children = get_blocks(block["id"])
        for child in children:
            parts.append(extract_text(child))

    return " ".join(p for p in parts if p).strip()


def count_words(text):
    return len(re.findall(r"\b\w+\b", text))


def detect_lang_safe(text):
    try:
        return detect(text)
    except:
        return "unknown"


# ================================
# PAGE ANALYSIS
# ================================
def analyze_page(page_id):
    print("\n" + "=" * 70)
    print(f"DEBUG PAGE: {page_id}")
    print("=" * 70)

    page = notion.pages.retrieve(page_id=page_id)

    title = None
    for prop in page.get("properties", {}).values():
        if prop.get("type") == "title":
            if prop.get("title"):
                title = prop["title"][0]["plain_text"]
    print(f"Title: {title}")

    print("\n=== BLOCK TREE ===")
    top_blocks = get_blocks(page_id)
    for b in top_blocks:
        print_tree(b, 0)

    # Full text
    print("\n=== FULL PAGE TEXT ===")
    full = []
    for b in top_blocks:
        full.append(extract_text(b))
    full_text = "\n".join(full)

    print(full_text[:4000])  # avoid flooding

    ru = en = 0

    for chunk in full:
        if not chunk.strip():
            continue
        lang = detect_lang_safe(chunk)
        words = count_words(chunk)
        if lang == "ru":
            ru += words
        elif lang == "en":
            en += words

    total = ru + en
    print("\n=== LANGUAGE STATS ===")
    print(f"Russian words: {ru}")
    print(f"English words: {en}")
    print(f"Total: {total}")
    print(f"RU %: {round(ru/total*100, 2) if total else 0}")
    print(f"EN %: {round(en/total*100, 2) if total else 0}")


# ================================
# RUN
# ================================
if __name__ == "__main__":
    for pid in PAGE_IDS:
        analyze_page(pid)
