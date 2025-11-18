import os
import re
from notion_client import Client

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
PAGE_ID = os.getenv("TEST_PAGE_ID") or os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN and TEST_PAGE_ID/ROOT_PAGE_ID env vars")

def normalize_id(raw_id: str):
    s = str(raw_id).strip()
    m = re.search(r"([0-9a-fA-F]{32})", s.replace("-", ""))
    return m.group(1) if m else s.replace("-", "")

client = Client(auth=NOTION_TOKEN)
PAGE_ID = normalize_id(PAGE_ID)

def get_children(block_id):
    items = []
    cursor = None
    while True:
        resp = client.blocks.children.list(block_id=block_id, start_cursor=cursor)
        items.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return items

def get_block(block_id):
    try:
        return client.blocks.retrieve(block_id=block_id)
    except Exception as e:
        return {"error": str(e)}

def get_page(page_id):
    try:
        return client.pages.retrieve(page_id=page_id)
    except Exception as e:
        return {"error": str(e)}

missing_access = []

def diagnose_block(block, level=0):
    prefix = "  " * level
    btype = block.get("type")
    bid = block.get("id")

    print(f"{prefix}🔹 Block {bid} type={btype}")

    # 1. Is this a synced block?
    if btype == "synced_block":
        sb = block.get("synced_block", {})
        src = sb.get("synced_from")

        if src and isinstance(src, dict):
            original_id = src.get("block_id")
            print(f"{prefix}   🔁 SYNCED BLOCK → COPY")
            print(f"{prefix}   🔗 Original block id: {original_id}")

            original_block = get_block(original_id)

            # If we couldn't access original — missing access!
            if "error" in original_block:
                print(f"{prefix}   ❗ Integration CANNOT access original block!")
                missing_access.append(original_id)
            else:
                print(f"{prefix}   ✔ Access to original block OK")
                # Diagnose children of the original
                children = get_children(original_id)
                for child in children:
                    diagnose_block(child, level + 1)

        else:
            print(f"{prefix}   🔁 SYNCED BLOCK → ORIGINAL")

    # 2. Recurse into children
    if block.get("has_children"):
        children = get_children(bid)
        if not children:
            print(f"{prefix}   ⚠ No children returned by API (possible missing access)")
        for child in children:
            diagnose_block(child, level + 1)

def main():
    print(f"🔍 Diagnosing page: {PAGE_ID}\n")
    top = get_children(PAGE_ID)

    for block in top:
        diagnose_block(block, 0)

    print("\n====================================================")
    print("🔎 FINAL REPORT")
    print("====================================================")

    if not missing_access:
        print("✔ No missing synced block access detected.")
    else:
        print("❗ Missing access to synced blocks:")
        for bid in missing_access:
            print(f" - Block: {bid}")

            # Try to find the original page this block comes from
            orig_block = get_block(bid)
            parent = orig_block.get("parent", {})
            if parent.get("type") == "page_id":
                pgid = parent.get("page_id")
                url = f"https://www.notion.so/{pgid.replace('-', '')}"
                print(f"   Page: {url} (share this page with integration)")

    print("====================================================")

if __name__ == "__main__":
    main()
