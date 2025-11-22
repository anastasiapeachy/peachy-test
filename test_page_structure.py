from notion_client import Client
import os
import json
import time

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
PAGE_ID = "6781d00a0aae41e8ab8fa0d114d52074"

notion = Client(auth=NOTION_TOKEN)

def list_children(block_id, indent=0):
    prefix = " " * indent
    try:
        resp = notion.blocks.children.list(block_id=block_id)
    except Exception as e:
        print(prefix, f"ERROR loading children of {block_id}: {e}")
        return

    for block in resp.get("results", []):
        btype = block["type"]
        bid = block["id"]

        print(f"{prefix}- {btype}  ({bid})")

        # print rich_text preview
        t = extract_preview(block)
        if t:
            print(f"{prefix}   text: {t[:80]}")

        # recursive
        if block.get("has_children"):
            list_children(bid, indent + 4)

def extract_preview(block):
    t = []

    btype = block.get("type")
    data = block.get(btype, {})

    if isinstance(data, dict):
        if "rich_text" in data:
            t.append(
                " ".join(part.get("plain_text", "") 
                         for part in data["rich_text"] if part.get("plain_text"))
            )
        if "caption" in data:
            t.append(
                " ".join(part.get("plain_text", "")
                         for part in data["caption"] if part.get("plain_text"))
            )

    return " ".join(t).strip()


print("=== PAGE STRUCTURE DUMP ===")
print("Page:", PAGE_ID)
print()

# Top-level blocks
try:
    page = notion.pages.retrieve(PAGE_ID)
    print("Page title:", page["properties"]["title"]["title"][0]["plain_text"])
except:
    print("Cannot read page metadata!")

print("\n=== BLOCK TREE ===")
list_children(PAGE_ID)
