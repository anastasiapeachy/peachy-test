name: Notion Old Pages Report

on:
  workflow_dispatch:
  schedule:
    # 1 число чётных месяцев (фев, апр, июн, авг, окт, дек) в 09:00 UTC
    - cron: "0 9 1 2,4,6,8,10,12 *"

jobs:
  run-report:
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

      # -----------------------------
      # Phase 1 — Notion → CSV
      # -----------------------------
      - name: Run Notion Scanner
        run: python notion_old_page_monitor.py
        env:
          NOTION_TOKEN: ${{ secrets.NOTION_TOKEN }}
          ROOT_PAGE_ID: ${{ secrets.ROOT_PAGE_ID }}

      - name: Read old pages count
        id: old_count
        run: |
          python - << 'PY'
          import json
          with open("notion_old_pages_count.json", "r", encoding="utf-8") as f:
              data = json.load(f)
          print(f"count={data['count']}")
          with open(os.environ["GITHUB_OUTPUT"], "a") as out:
              out.write(f"count={data['count']}\n")
          PY

      # -----------------------------
      # Create / update GitHub Release with CSV
      # -----------------------------
      - name: Create or update GitHub Release
        if: steps.old_count.outputs.count != '0'
        id: create_release
        uses: softprops/action-gh-release@v2
        with:
          tag_name: notion-old-pages
          name: Notion Old Pages Report
          make_latest: true
          files: notion_old_pages.csv
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}

      # -----------------------------
      # Phase 2 — Slack notification
      # -----------------------------
      - name: Send Slack message with Download button
        if: steps.old_count.outputs.count != '0'
        run: python notion_old_page_monitor.py --artifact-url "${{ steps.create_release.outputs.html_url }}"
        env:
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
