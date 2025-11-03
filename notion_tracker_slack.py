name: Notion Tracker

on:
  schedule:
    - cron: "0 9 * * *" # каждый день в 9 утра по UTC
  workflow_dispatch:

jobs:
  check:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Restore known pages cache
        id: cache-known
        uses: actions/cache@v4
        with:
          path: notion_tracker_data/known_pages.json
          key: known-pages-${{ github.ref }}
          restore-keys: |
            known-pages-

      - name: Run Notion Tracker
        env:
          NOTION_TOKEN: ${{ secrets.NOTION_TOKEN }}
          ROOT_PAGE_ID: ${{ secrets.ROOT_PAGE_ID }}
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
        run: |
          mkdir -p notion_tracker_data
          python notion_tracker_slack.py

      - name: Save updated known pages
        if: always()
        uses: actions/cache/save@v4
        with:
          path: notion_tracker_data/known_pages.json
          key: known-pages-${{ github.ref }}
