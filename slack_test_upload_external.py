import os
import requests
import json

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")

# Создаём тестовый CSV
with open("test.csv", "w", encoding="utf-8") as f:
    f.write("name,value\nExternal,456\n")

def test_upload_external():
    print("Step 1: Request upload URL")

    payload = {
        "filename": "test.csv",
        "length": os.path.getsize("test.csv")
    }

    r1 = requests.post(
        "https://slack.com/api/files.getUploadURLExternal",
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}"
        },
        json=payload
    )

    print("getUploadURLExternal:", r1.status_code, r1.text)

    j = r1.json()
    if not j.get("ok"):
        print("Error:", j.get("error"))
        return

    upload_url = j["upload_url"]
    file_id = j["file_id"]

    print("Step 2: PUT upload...")
    with open("test.csv", "rb") as f:
        r2 = requests.put(upload_url, data=f)
    print("PUT:", r2.status_code)

    print("Step 3: Complete upload")
    r3 = requests.post(
        "https://slack.com/api/files.completeUploadExternal",
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}"
        },
        json={
            "files": [{"id": file_id}],
            "channel_id": SLACK_CHANNEL,
            "initial_comment": "External upload test OK"
        }
    )

    print("completeUploadExternal:", r3.status_code, r3.text)


test_upload_external()
