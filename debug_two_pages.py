import os
import re
from notion_client import Client

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
notion = Client(auth=NOTION_TOKEN)

PAGE_IDS = [
    "d3848d6caa5c444a801993d7af5f3cca",
    "6781d00a0aae41e8ab8fa0d114d52074"
]

# ================================
# HELPERS
# ================================

def extract_text_from_rich_text(items):
    parts = []
    for rt in items:
        if "plain_text" in rt:
            parts.append(rt["plain_text"])
    return " ".join(parts)

def get_child_blocks(block_id):
    all_blocks = []
    cursor = None
    while True:
        resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)
        all_blocks.extend(resp["results"])
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return all_blocks

def extract_full_text(block):
    """Recursively extract all text from any block including columns & tables."""
    t = []

    btype = block.get("type")
    data = block.get(btype, {}) if btype else {}

    # text-based blocks
    if btype in (
        "paragraph", "heading_1", "heading_2", "heading_3",
        "heading_4", "heading_5", "heading_6",
        "bulleted_list_item", "numbered_list_item",
        "callout", "quote", "to_do", "toggle"
    ):
        t.append(extract_text_from_rich_text(data.get("rich_text", [])))

    # code
    if btype == "code":
        t.append(extract_text_from_rich_text(data.get("rich_text", [])))

    # captions (figures, videos, images)
    if "caption" in data:
        t.append(extract_text_from_rich_text(data["caption"]))

    # table handling
    if btype == "table":
        rows = get_child_blocks(block["id"])
        for row in rows:
            if row.get("type") == "table_row":
                for cell in row["table_row"]["cells"]:
                    t.append(extract_text_from_rich_text(cell))

    # column_list / column blocks
    if btype in ("column_list", "column"):
        children = get_child_blocks(block["id"])
        for c in children:
            t.append(extract_full_text(c))

        return " ".join(x for x in t if x).strip()

    # recurse into children
    if block.get("has_children"):
        children = get_child_blocks(block["id"])
        for c in children:
            t.append(extract_full_text(c))

    return " ".join(x for x in t if x).strip()


# ================================
# WORD-LEVEL LANGUAGE DETECTION
# ================================

def word_lang(word):
    # Skip URLs
    if "http://" in word or "https://" in word:
        return "other"

    # Skip code-like fragments
    if re.match(r"^[0-9\W_]+$", word):
        return "other"

    # Russian letters?
    if re.search(r"[а-яА-ЯёЁ]", word):
        return "ru"

    # English letters?
    if re.search(r"[a-zA-Z]", word):
        return "en"

    return "other"

def count_lang_words(text):
    ru = en = 0
    words = re.findall(r"[^\s]+", text)

    for w in words:
        lang = word_lang(w)
        if lang == "ru":
            ru += 1
        elif lang == "en":
            en += 1

    return ru, en


# ================================
# DEBUG PROCESS
# ================================

def analyze_page(page_id):
    print("\n" + "="*70)
    print(f"PAGE: {page_id}")
    print("="*70)

    page = notion.pages.retrieve(page_id=page_id)

    # title
    title = "Untitled"
    for prop in page.get("properties", {}).values():
        if prop.get("type") == "title":
            if prop["title"]:
                title = prop["title"][0]["plain_text"]
    print(f"TITLE: {title}")

    # get blocks
    top_blocks = get_child_blocks(page_id)

    full_text = []
    for b in top_blocks:
        full_text.append(extract_full_text(b))

    combined = "\n".join(full_text)

    print("\n=== RAW EXTRACTED TEXT START ===")
    print(combined[:4000])
    print("=== RAW EXTRACTED TEXT END ===")

    # language stats
    ru, en = count_lang_words(combined)
    total = ru + en

    print("\n=== LANGUAGE ===")
    print(f"Russian words: {ru}")
    print(f"English words: {en}")
    print(f"Total: {total}")
    print(f"RU %: {round(ru/total*100, 2) if total else 0}")
    print(f"EN %: {round(en/total*100, 2) if total else 0}")


if __name__ == "__main__":
    for pid in PAGE_IDS:
        analyze_page(pid)
