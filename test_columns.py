import os
from notion_client import Client

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
PAGE_ID = "d3848d6caa5c444a801993d7af5f3cca"  # твой page_id

if not NOTION_TOKEN:
    raise Exception("NOTION_TOKEN missing")

notion = Client(auth=NOTION_TOKEN)


def get_children(block_id):
    """Return list of child blocks (full pagination)."""
    out = []
    cursor = None
    while True:
        resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)
        out.extend(resp["results"])
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return out


def extract_text(block):
    """Extract any rich_text from a block."""
    btype = block["type"]
    data = block[btype]

    txt = []

    if "rich_text" in data:
        txt.extend(t["plain_text"] for t in data["rich_text"] if t.get("plain_text"))

    if "caption" in data:
        txt.extend(t["plain_text"] for t in data["caption"] if t.get("plain_text"))

    return " ".join(txt).strip()


def extract_recursive(block_id):
    """Full recursive reader covering column_list and column."""
    all_texts = []
    blocks = get_children(block_id)

    for b in blocks:
        btype = b["type"]

        # extract text in current block
        t = extract_text(b)
        if t:
            all_texts.append(t)

        # SPECIAL CASE: Notion columns
        if btype in ("column_list", "column"):
            col_children = get_children(b["id"])
            for col in col_children:
                all_texts.append(extract_recursive(col["id"]))
            continue

        # normal recursion
        if b.get("has_children"):
            all_texts.append(extract_recursive(b["id"]))

    return "\n".join(x for x in all_texts if x)


print("=== START TEST ===")
print(f"Testing page → {PAGE_ID}\n")

result_text = extract_recursive(PAGE_ID)

print("\n=== RESULT TEXT ===\n")
print(result_text[:4000])  # первые 4000 символов
print("\n=== LENGTH:", len(result_text), "chars ===")
