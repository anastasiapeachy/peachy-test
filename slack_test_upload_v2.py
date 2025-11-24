import os
import requests

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")

if not SLACK_BOT_TOKEN or not SLACK_CHANNEL:
    raise RuntimeError("Missing SLACK_BOT_TOKEN or SLACK_CHANNEL")

# Создаём тестовый CSV
with open("test.csv", "w", encoding="utf-8") as f:
    f.write("name,value\nTest,123\n")

def test_upload_v2():
    print("Uploading test.csv via files.uploadV2...")

    with open("test.csv", "rb") as file:
        resp = requests.post(
            "https://slack.com/api/files.uploadV2",
            headers={
                "Authorization": f"Bearer {SLACK_BOT_TOKEN}"
            },
            data={
                "channel_id": SLACK_CHANNEL,
                "initial_comment": "Test upload (V2)"
            },
            files={
                "file": ("test.csv", file, "text/csv")
            }
        )

    print(resp.status_code, resp.text)


test_upload_v2()
