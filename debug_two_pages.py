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


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def normalize_id(s):
    return s.replace("-", "").strip()


def detect_lang_safe(text):
    try:
        return detect(text)
    except:
        return "unknown"


def count_words(t):
    return len(re.findall(r"\b\w+\b", t))


def get_blocks(block_id):
    blocks = []
    cursor = None
    while True:
        resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)
        blocks.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return blocks


# ---------------------------------------------------------
# FULL TEXT EXTRACTOR (stable)
# ---------------------------------------------------------
def extract_all_text_from_block(block):
    texts = []
    btype = block.get("type")
    data = block.get(btype, {}) if btype else {}

    # ---- helper for rich_text ----
    def extract_rt(rt_list):
        out = []
        for rt in rt_list:
            if not isinstance(rt, dict):
                continue

            if "plain_text" in rt:
                out.append(rt["plain_text"])

            if rt.get("type") == "mention":
                m = rt.get("mention", {})
                if "page" in m and m["page"].get("title"):
                    out.append(m["page"]["title"])
                if "user" in m and m["user"].get("name"):
                    out.append(m["user"]["name"])
                if "database" in m and m["database"].get("name"):
                    out.append(m["database"]["name"])

            href = rt.get("href")
            if href and rt.get("plain_text"):
                out.append(rt["plain_text"])
        return " ".join(out)

    # ---- standard rich_text containers ----
    rich_containers = [
        "paragraph", "heading_1", "heading_2", "heading_3", "heading_4", "heading_5",
        "quote", "callout", "bulleted_list_item", "numbered_list_item", "toggle", "to_do"
    ]

    if btype in rich_containers:
        texts.append(extract_rt(data.get("rich_text", [])))

    # ---- code blocks ----
    if btype == "code":
        texts.append(extract_rt(data.get("rich_text", [])))

    # ---- captions ----
    if isinstance(data, dict) and "caption" in data:
        texts.append(extract_rt(data.get("caption", [])))

    # ---- equations ----
    if btype == "equation" and "expression" in data:
        texts.append(data["expression"])

    # ---- COLUMNS ----
    if btype in ("column_list", "column"):
        cursor = None
        while True:
            resp = notion.blocks.children.list(block_id=block["id"], start_cursor=cursor)
            for child in resp.get("results", []):
                texts.append(extract_all_text_from_block(child))
            cursor = resp.get("next_cursor")
            if not cursor:
                break
        return " ".join(t for t in texts if t).strip()

    # ---- TABLES ----
    if btype == "table":
        cursor = None
        while True:
            resp = notion.blocks.children.list(block_id=block["id"], start_cursor=cursor)
            for row in resp.get("results", []):
                if row["type"] == "table_row":
                    cells = row["table_row"]["cells"]
                    for cell in cells:
                        texts.append(extract_rt(cell))
            cursor = resp.get("next_cursor")
            if not cursor:
                break

    # ---- generic recursion ----
    if block.get("has_children"):
        cursor = None
        while True:
            resp = notion.blocks.children.list(block_id=block["id"], start_cursor=cursor)
            for child in resp.get("results", []):
                texts.append(extract_all_text_from_block(child))
            cursor = resp.get("next_cursor")
            if not cursor:
                break

    return " ".join(t for t in texts if t).strip()


# ---------------------------------------------------------
# Pretty tree output
# ---------------------------------------------------------
def print_tree(block, indent=0):
    pad = " " * indent
    print(f"{pad}- {block['type']} ({block['id']})")

    text = extract_all_text_from_block(block)
    if text:
        print(f"{pad}    text: {text[:160]}")

    if block.get("has_children"):
        children = get_blocks(block["id"])
        for child in children:
            print_tree(child, indent + 4)


# ---------------------------------------------------------
# PAGE ANALYSIS
# ---------------------------------------------------------
def analyze_page(page_id):
    print("\n" + "=" * 80)
    print(f"DEBUG PAGE {page_id}")
    print("=" * 80)

    page = notion.pages.retrieve(page_id=page_id)

    title = None
    for prop in page.get("properties", {}).values():
        if prop.get("type") == "title" and prop.get("title"):
            title = prop["title"][0]["plain_text"]

    print(f"Title: {title}")

    top_blocks = get_blocks(page_id)

    print("\n=== BLOCK TREE ===")
    for b in top_blocks:
        print_tree(b, 0)

    print("\n=== FULL EXTRACTED TEXT ===")
    full_text = []
    for b in top_blocks:
        full_text.append(extract_all_text_from_block(b))

    combined = "\n".join(full_text)
    print(combined[:4000])

    # --- language stats ---
    ru = en = 0
    for chunk in full_text:
        chunk = chunk.strip()
        if not chunk:
            continue
        lang = detect_lang_safe(chunk)
        words = count_words(chunk)
        if lang == "ru":
            ru += words
        elif lang == "en":
            en += words

    total = ru + en
    print("\n=== LANGUAGE ===")
    print(f"Russian words: {ru}")
    print(f"English words: {en}")
    print(f"Total: {total}")
    print(f"RU %: {round(ru/total*100, 2) if total else 0}")
    print(f"EN %: {round(en/total*100, 2) if total else 0}")


# ---------------------------------------------------------
# RUN
# ---------------------------------------------------------
if __name__ == "__main__":
    for pid in PAGE_IDS:
        analyze_page(pid)
