import os
from notion_client import Client

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
PAGE_ID = "d3848d6caa5c444a801993d7af5f3cca"  # твоя тестовая страница

if not NOTION_TOKEN:
    raise Exception("Missing NOTION_TOKEN")

client = Client(auth=NOTION_TOKEN)


# ================================================================
# PAGINATION-AWARE FETCH
# ================================================================
def fetch_children(block_id):
    """Fetch ALL children of a block with FULL pagination."""
    results = []
    cursor = None

    while True:
        resp = client.blocks.children.list(
            block_id=block_id,
            start_cursor=cursor
        )
        results.extend(resp["results"])

        if not resp.get("has_more"):
            break

        cursor = resp.get("next_cursor")

    return results


# ================================================================
# FULL RECURSIVE TEXT EXTRACTOR
# ================================================================
def extract_text(block):
    """Extract text from any block type recursively."""
    texts = []

    btype = block.get("type")
    data = block.get(btype, {})

    # 1. rich_text (common case)
    if isinstance(data, dict) and "rich_text" in data:
        texts.append(" ".join(t["plain_text"] for t in data["rich_text"]))

    # 2. caption
    if isinstance(data, dict) and "caption" in data:
        texts.append(" ".join(t["plain_text"] for t in data["caption"]))

    # 3. table rows
    if btype == "table":
        rows = fetch_children(block["id"])
        for row in rows:
            cells = row.get("table_row", {}).get("cells", [])
            for cell in cells:
                texts.append(" ".join(t["plain_text"] for t in cell))

    # 4. synced block
    if btype == "synced_block":
        original = data.get("synced_from")
        if original:
            # follow the original block
            orig_id = original.get("block_id")
            if orig_id:
                children = fetch_children(orig_id)
                for ch in children:
                    texts.append(extract_text(ch))

    # 5. recurse into children
    if block.get("has_children"):
        children = fetch_children(block["id"])
        for ch in children:
            texts.append(extract_text(ch))

    return "\n".join([t for t in texts if t]).strip()


# ================================================================
# PROCESS PAGE
# ================================================================
def process_page(page_id):
    print("=== RUNNING FULL PAGINATION TEST ===\n")
    print(f"Page ID: {page_id}\n")

    blocks = fetch_children(page_id)
    print(f"Total top-level blocks: {len(blocks)}\n")

    col1_text = ""
    col2_text = ""
    normal_text = ""

    for block in blocks:
        btype = block["type"]

        if btype == "column_list":
            # fetch columns
            columns = fetch_children(block["id"])

            if len(columns) >= 1:
                col1_text = extract_text(columns[0])
            if len(columns) >= 2:
                col2_text = extract_text(columns[1])

        else:
            # non-column content
            normal_text += extract_text(block) + "\n"

    print("\n=== COLUMN 1 LENGTH:", len(col1_text), "chars ===")
    print(col1_text[:2000], "\n... [truncated]\n")

    print("\n=== COLUMN 2 LENGTH:", len(col2_text), "chars ===")
    print(col2_text[:2000], "\n... [truncated]\n")

    print("\n=== NORMAL CONTENT LENGTH:", len(normal_text), "chars ===")
    print(normal_text[:1000], "\n... [truncated]\n")

    total_len = len(col1_text) + len(col2_text) + len(normal_text)
    print("\n=== TOTAL TEXT LENGTH:", total_len, "chars ===")


if __name__ == "__main__":
    process_page(PAGE_ID)
