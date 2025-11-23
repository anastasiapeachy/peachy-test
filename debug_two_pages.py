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
def extract_all_text_from_block(block):
    """Extract absolutely all text from any Notion block, with full recursion and column support."""
    texts = []

    btype = block.get("type")
    data = block.get(btype, {}) if btype else {}

    # ========= 1. FULL RICH TEXT PARSER =========
    def extract_rich_text(rt_list):
        collected = []
        for rt in rt_list:
            if not isinstance(rt, dict):
                continue

            # plain text (normal case)
            if "plain_text" in rt:
                collected.append(rt["plain_text"])

            # mention text
            if rt.get("type") == "mention":
                m = rt.get("mention", {})
                if "database" in m and m["database"].get("name"):
                    collected.append(m["database"]["name"])
                if "page" in m and m["page"].get("title"):
                    collected.append(m["page"]["title"])
                if "user" in m and m["user"].get("name"):
                    collected.append(m["user"]["name"])

            # href text (inline links)
            href = rt.get("href")
            if href and rt.get("plain_text"):
                collected.append(rt["plain_text"])

        return " ".join(collected)

    # ========= 2. HANDLE ALL BLOCK TYPES =========

    # Paragraphs, headings, calls, list items, etc.
    rich_containers = [
        "paragraph", "heading_1", "heading_2", "heading_3", "heading_4", "heading_5", "heading_6",
        "quote", "callout", "bulleted_list_item", "numbered_list_item",
        "toggle", "to_do"
    ]
    if btype in rich_containers:
        rt = data.get("rich_text", [])
        texts.append(extract_rich_text(rt))

    # Code blocks
    if btype == "code":
        code_text = data.get("rich_text", [])
        texts.append(extract_rich_text(code_text))

    # Captions (images, files, videos…)
    cap = data.get("caption")
    if cap:
        texts.append(extract_rich_text(cap))

    # Equations
    if btype == "equation" and "expression" in data:
        texts.append(data["expression"])

    # ========= 3. SPECIAL HANDLING: COLUMNS =========

    if btype in ("column_list", "column"):
        # fetch children of the column
        try:
            cursor = None
            while True:
                resp = notion.blocks.children.list(block_id=block["id"], start_cursor=cursor)
                for child in resp.get("results", []):
                    texts.append(extract_all_text_from_block(child))
                cursor = resp.get("next_cursor")
                if not cursor:
                    break
        except:
            pass

        return " ".join(t for t in texts if t).strip()

    # ========= 4. TABLES =========

    if btype == "table":
        try:
            cursor = None
            while True:
                resp = notion.blocks.children.list(block_id=block["id"], start_cursor=cursor)
                for row in resp.get("results", []):
                    if row.get("type") == "table_row":
                        cells = row["table_row"]["cells"]
                        for cell in cells:
                            texts.append(extract_rich_text(cell))
                cursor = resp.get("next_cursor")
                if not cursor:
                    break
        except:
            pass

    # ========= 5. RECURSE INTO CHILDREN IF ANY =========

    if block.get("has_children"):
        try:
            cursor = None
            while True:
                resp = notion.blocks.children.list(block_id=block["id"], start_cursor=cursor)
                for child in resp.get("results", []):
                    texts.append(extract_all_text_from_block(child))
                cursor = resp.get("next_cursor")
                if not cursor:
                    break
        except:
            pass

    # ========= 6. JOIN RESULT =========

    return " ".join(t for t in texts if t).strip()


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
