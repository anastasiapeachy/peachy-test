import os
import re
from notion_client import Client
from langdetect import detect

# -------------------------
# ENV
# -------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
PAGE_ID = os.getenv("TEST_PAGE_ID") or os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not PAGE_ID:
    raise ValueError("Environment vars NOTION_TOKEN and TEST_PAGE_ID or ROOT_PAGE_ID are required")

def normalize_id(raw_id: str) -> str:
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip()
    m = re.search(r"([0-9a-fA-F]{32})", s.replace("-", ""))
    if m:
        return m.group(1)
    return s.replace("-", "")

client = Client(auth=NOTION_TOKEN)
PAGE_ID = normalize_id(PAGE_ID)

# -------------------------
# FETCH CHILDREN
# -------------------------
def get_children(block_id: str):
    all_blocks = []
    cursor = None
    while True:
        resp = client.blocks.children.list(block_id=block_id, start_cursor=cursor)
        all_blocks.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return all_blocks

# -------------------------
# TEXT EXTRACTION — FIXED for synced blocks + columns
# -------------------------
def extract_all_text_from_block(block: dict) -> str:
    texts = []
    btype = block.get("type")
    content = block.get(btype, {}) if btype else {}

    # ---- 1. rich_text ----
    if isinstance(content, dict):
        if "rich_text" in content:
            rt = content["rich_text"]
            texts.append(" ".join(t.get("plain_text", "") for t in rt if t.get("plain_text")))

        if "caption" in content:
            cap = content["caption"]
            texts.append(" ".join(t.get("plain_text", "") for t in cap if t.get("plain_text")))

    # ---- 2. equation ----
    if btype == "equation":
        eq = content.get("expression")
        if eq:
            texts.append(eq)

    # ---- 3. synced_block (MAIN FIX) ----
    if btype == "synced_block":
        synced = block.get("synced_block", {})
        src = synced.get("synced_from")

        # If this is a reference — fetch ORIGINAL children
        if src and isinstance(src, dict):
            original_id = src.get("block_id")
            if original_id:
                try:
                    original_children = get_children(original_id)
                    for oc in original_children:
                        texts.append(extract_all_text_from_block(oc))
                except Exception as e:
                    print("Error fetching synced original:", e)

    # ---- 4. Always descend into children for：
    # column_list, column, and any block with has_children = true
    force_children = btype in ("column_list", "column")

    try:
        if force_children or block.get("has_children"):
            children = get_children(block["id"])
            for child in children:
                texts.append(extract_all_text_from_block(child))
    except Exception as e:
        print("Error fetching children:", e)

    return " ".join(t for t in texts if t).strip()

# -------------------------
# LANG + WORD COUNT
# -------------------------
def detect_lang_safe(text: str) -> str:
    try:
        return detect(text)
    except:
        return "unknown"

def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))

# -------------------------
# MAIN
# -------------------------
def main():
    print(f"\n🔍 Checking page: {PAGE_ID}")
    blocks = get_children(PAGE_ID)
    print(f"Top-level blocks: {len(blocks)}")

    total_ru = 0
    total_en = 0

    for block in blocks:
        btype = block.get("type")

        if btype == "child_page":
            continue

        text = extract_all_text_from_block(block)
        if not text:
            continue

        lang = detect_lang_safe(text)
        words = count_words(text)

        print("\n----------------------------------------")
        print(f"Block {block.get('id')} type={btype}")
        print(f"Detected lang: {lang} | words: {words}")
        print(text[:300] + ("..." if len(text) > 300 else ""))

        if lang == "ru":
            total_ru += words
        elif lang == "en":
            total_en += words

    total = total_ru + total_en
    if total == 0:
        ru_pct = en_pct = 0.0
    else:
        ru_pct = total_ru / total * 100
        en_pct = total_en / total * 100

    print("\n============================================================")
    print(f"RU words: {total_ru}")
    print(f"EN words: {total_en}")
    print(f"RU %: {ru_pct:.2f}")
    print(f"EN %: {en_pct:.2f}")
    print("============================================================")

if __name__ == "__main__":
    main()
