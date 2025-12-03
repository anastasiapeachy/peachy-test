name: Notion New Pages Test

on:
  workflow_dispatch:
  schedule:
    - cron: "0 9 * * *"   # каждый день в 9 UTC

jobs:
  run-test:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install notion-client==2.2.1
          pip install requests

      - name: Run test scanner
        env:
          NOTION_TOKEN: ${{ secrets.NOTION_TOKEN }}
          ROOT_PAGE_ID: ${{ secrets.ROOT_PAGE_ID }}
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
        run: python notion_new_pages_monitor.py
