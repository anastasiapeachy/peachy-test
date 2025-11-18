from notion_client import Client
import os, json

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT = "d3848d6caa5c444a801993d7af5f3cca"

notion = Client(auth=NOTION_TOKEN)

resp = notion.blocks.children.list(ROOT)
print(json.dumps(resp, indent=2))
