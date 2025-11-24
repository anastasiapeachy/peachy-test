import os
import requests

TOKEN = os.getenv("SLACK_BOT_TOKEN")
CHANNEL = os.getenv("SLACK_CHANNEL")

# Тестовый CSV
with open("test.csv", "w", encoding="utf-8") as f:
    f.write("name,value\nLegacy,789\n")

print("Uploading via legacy chat.postMessage…")

with open("test.csv", "rb") as file:
    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {TOKEN}"
        },
        data={
            "channel": CHANNEL,
            "text": "Legacy file upload test"
        },
        files={
            "file": ("test.csv", file, "text/csv")
        }
    )

print(resp.status_code, resp.text)
