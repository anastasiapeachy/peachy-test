import os
import csv
import re
import time
from notion_client import Client
from langdetect import detect

# ===== Env =====
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or ROOT_PAGE_ID env vars")

notion = Client(auth=NOTION_TOKEN)


# ================================
# Helpers
# ================================

def normalize_id(i):
    return i.replace("-", "").strip()


ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)


def notion_url(pid):
    clean = pid.replace("-", "")
    return f"https://www.notion.so/{clean}"


def count_words(text):
    return len(re.findall(r"\b\w+\b", text))


# ================================
#  FULL RECURSIVE TEXT EXTRACTION
# ================================

def extract_text_from_block(block):
    """Extract ALL text from block (rich_text, children, columns, tables, synced blocks)."""
    results = []
    btype = block.get("type")
    data = block.get(btype, {}) if btype else {}

    # --- 1) rich_text ---
    if isinstance(data, dict) and "rich_text" in data:
        rt = data["rich_text"]
        if isinstance(rt, list):
            for t in rt:
                if t.get("plain_text"):
                    results.append(t["plain_text"])

    # --- 2) captions ---
    if isinstance(data, dict) and "caption" in data:
        cap = data["caption"]
        if isinstance(cap, list):
            for t in cap:
                if t.get("plain_text"):
                    results.append(t["plain_text"])

    # --- 3) equation ---
    if btype == "equation":
        expr = data.get("expression")
        if expr:
            results.append(expr)

    # --- 4) table rows ---
    if btype == "table":
        try:
            rows = notion.blocks.children.list(block["id"])["results"]
            for row in rows:
                if row.get("type") == "table_row":
                    cells = row["table_row"]["cells"]
                    for cell in cells:
                        for t in cell:
                            if t.get("plain_text"):
                                results.append(t["plain_text"])
        except:
            pass

    # --- 5) column_list / column FIX ---
    if btype in ("column_list", "column"):
        try:
            children = notion.blocks.children.list(block["id"])["results"]
            for child in children:
                results.append(extract_text_from_block(child))
        except:
            pass
        return " ".join(r for r in results if r).strip()

    # --- 6) synced_block (real source) ---
    if btype == "synced_block":
        synced = data
        if synced.get("synced_from"):
            src = synced["synced_from"].get("block_id")
            if src:
                try:
                    children = notion.blocks.children.list(src)["results"]
                    for c in children:
                        results.append(extract_text_from_block(c))
                except:
                    pass

    # --- 7) children blocks recursion ---
    if block.get("has_children"):
        try:
            children = notion.blocks.children.list(block["id"])["results"]
            for child in children:
                results.append(extract_text_from_block(child))
        except:
            pass

    return " ".join(r for r in results if r).strip()


def get_page_text(page_id):
    full_text_parts = []
    cursor = None

    while True:
        resp = notion.blocks.children.list(block_id=page_id, start_cursor=cursor)

        for block in resp["results"]:
            if block["type"] == "child_page":
                continue
            txt = extract_text_from_block(block)
            if txt:
                full_text_parts.append(txt)

        if not resp.get("has_more"):
            break
        cursor = resp["next_cursor"]

    return " ".join(full_text_parts)


# ================================
# STRICT HIERARCHY VALIDATION
# ================================

def parent_chain(page_id):
    chain = []
    cur = notion.pages.retrieve(page_id=page_id)
    while True:
        parent = cur.get("parent", {})
        ptype = parent.get("type")
        if ptype == "page_id":
            pid = normalize_id(parent["page_id"])
            chain.append(pid)
            if pid == ROOT_PAGE_ID:
                return chain
            cur = notion.pages.retrieve(page_id=pid)
            continue
        return None  # not allowed


def is_child_of_root(page):
    pid = normalize_id(page["id"])
    if pid == ROOT_PAGE_ID:
        return True
    ch = parent_chain(pid)
    return ch is not None


# ================================
# MAIN
# ================================

def analyze_batch(pages):
    rows = []

    for p in pages:
        page_id = normalize_id(p["id"])
        title = get_title(p)
        url = notion_url(page_id)

        text = get_page_text(page_id)
        if not text:
            rows.append((title, url, 0, 0))
            continue

        try:
            lang = detect(text)
        except:
            lang = "unknown"

        words = count_words(text)
        if words == 0:
            rows.append((title, url, 0, 0))
            continue

        # naive split
        ru_words = len(re.findall(r"[А-Яа-яЁё]+", text))
        en_words = len(re.findall(r"[A-Za-z]+", text))

        total = ru_words + en_words
        if total == 0:
            ru_pct = en_pct = 0
        else:
            ru_pct = ru_words / total * 100
            en_pct = en_words / total * 100

        rows.append((title, url, round(ru_pct, 2), round(en_pct, 2)))

    return rows


def get_title(page):
    props = page.get("properties", {})
    for prop in props.values():
        if prop.get("type") == "title":
            t = prop.get("title", [])
            if t:
                return t[0]["plain_text"]
    return "(untitled)"


def main():
    # Step 1: search all pages
    print("Searching all pages…")
    all_pages = notion.search(
        filter={"value": "page", "property": "object"},
        query=""
    )["results"]

    filtered = [p for p in all_pages if is_child_of_root(p)]
    print(f"Total pages under root: {len(filtered)}")

    # Step 2: batch into 20
    batches = [filtered[i:i+20] for i in range(0, len(filtered), 20)]

    all_rows = []

    for idx, batch in enumerate(batches, 1):
        print(f"Processing batch {idx}/{len(batches)} (size {len(batch)})")
        rows = analyze_batch(batch)
        all_rows.extend(rows)
        time.sleep(0.3)

    # Step 3: write CSV
    fname = "notion_language_report.csv"
    with open(fname, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Title", "URL", "% Russian", "% English"])
        for r in all_rows:
            w.writerow(r)

    print(f"\nSaved report: {fname}")


if __name__ == "__main__":
    main()
