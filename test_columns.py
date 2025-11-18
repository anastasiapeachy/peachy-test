from notion_client import Client

# токен и ID страницы подставляет GitHub Actions через env
notion = Client(auth=os.getenv("NOTION_TOKEN"))
page_id = os.getenv("TEST_PAGE_ID")

print("\n🔍 Fetching level 1 blocks…")
level1 = notion.blocks.children.list(page_id)
print("LEVEL 1 TYPES:", [b["type"] for b in level1["results"]])

# ищем блок column_list
col_list_id = None
for b in level1["results"]:
    if b["type"] == "column_list":
        col_list_id = b["id"]
        break

print("\ncolumn_list id:", col_list_id)

if not col_list_id:
    print("❌ No column_list found on this page")
    exit()

print("\n🔍 Fetching columns inside column_list…")
cols = notion.blocks.children.list(col_list_id)
print("COLUMNS:", [c["id"] for c in cols["results"]])

for col in cols["results"]:
    cid = col["id"]
    print(f"\n🔍 Fetching children of column {cid} …")
    children = notion.blocks.children.list(cid)
    types = [ch["type"] for ch in children["results"]]
    print("CHILD TYPES:", types)

    for ch in children["results"]:
        data = ch.get(ch["type"], {})
        rt = data.get("rich_text")
        if rt:
            text = [t.get("plain_text", "") for t in rt]
            print(" •", ch["type"], "→", text)
        else:
            print(" •", ch["type"], "(no rich_text)")
