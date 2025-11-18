from notion_client import Client

notion = Client(auth=YOUR_TOKEN)
page_id = "d3848d6c-aa5c-444a-8019-93d7af5f3cca"

# 1. First-level blocks (you already did)
level1 = notion.blocks.children.list(page_id)
print("LEVEL 1:", [b["type"] for b in level1["results"]])

# find column_list block
col_list_id = None
for b in level1["results"]:
    if b["type"] == "column_list":
        col_list_id = b["id"]

print("column_list id:", col_list_id)

# 2. Get columns
cols = notion.blocks.children.list(col_list_id)
print("COLUMNS:", [b["id"] for b in cols["results"]])

# 3. For each column, get content
for col in cols["results"]:
    cid = col["id"]
    children = notion.blocks.children.list(cid)
    print("COLUMN", cid, "contains:", [c["type"] for c in children["results"]])
