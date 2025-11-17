# Requirements: notion-client==2.2.1
# Install with: pip install notion-client==2.2.1

import os
import sys
import subprocess

# Auto-install dependencies
def install_dependencies():
    """Install required packages if not already installed"""
    required_packages = ['notion-client==2.2.1']
    for package in required_packages:
        try:
            __import__(package.split('==')[0].replace('-', '_'))
        except ImportError:
            print(f"Installing {package}...")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])

install_dependencies()

from datetime import datetime, timedelta
from notion_client import Client
import csv

# Configuration from environment variables
NOTION_TOKEN = os.environ.get('NOTION_TOKEN')
ROOT_PAGE_ID = os.environ.get('ROOT_PAGE_ID')
MONTHS_THRESHOLD = int(os.environ.get('MONTHS_THRESHOLD', '12'))

# Initialize Notion client
notion = Client(auth=NOTION_TOKEN)

# Calculate date threshold
threshold_date = datetime.now() - timedelta(days=MONTHS_THRESHOLD * 30)

def get_all_child_pages(page_id, all_pages=None):
    """Recursively get all child pages under a parent page"""
    if all_pages is None:
        all_pages = []
    
    try:
        response = notion.blocks.children.list(block_id=page_id, page_size=100)
        
        for block in response['results']:
            if block['type'] in ['child_page', 'child_database']:
                try:
                    page = notion.pages.retrieve(page_id=block['id'])
                    all_pages.append(page)
                    get_all_child_pages(block['id'], all_pages)
                except Exception as e:
                    print(f"Error retrieving page {block['id']}: {e}")
        
        while response.get('has_more'):
            response = notion.blocks.children.list(
                block_id=page_id,
                page_size=100,
                start_cursor=response['next_cursor']
            )
            
            for block in response['results']:
                if block['type'] in ['child_page', 'child_database']:
                    try:
                        page = notion.pages.retrieve(page_id=block['id'])
                        all_pages.append(page)
                        get_all_child_pages(block['id'], all_pages)
                    except Exception as e:
                        print(f"Error retrieving page {block['id']}: {e}")
        
        return all_pages
    
    except Exception as e:
        print(f"Error listing children of {page_id}: {e}")
        return all_pages

def get_page_title(page):
    """Extract page title from various property locations"""
    try:
        if 'title' in page['properties'] and page['properties']['title']['title']:
            return page['properties']['title']['title'][0]['plain_text']
        
        if 'Name' in page['properties'] and page['properties']['Name']['title']:
            return page['properties']['Name']['title'][0]['plain_text']
        
        if 'child_page' in page and 'title' in page['child_page']:
            return page['child_page']['title']
        
        return 'Untitled'
    except:
        return 'Untitled'

def get_user_name(user_id):
    """Get user name from user ID"""
    try:
        user = notion.users.retrieve(user_id=user_id)
        return user.get('name', user_id)
    except:
        return user_id

def find_stale_pages():
    """Main function to find and report stale pages"""
    print(f"Searching for pages not edited since {threshold_date.strftime('%Y-%m-%d')}...\n")
    
    all_pages = get_all_child_pages(ROOT_PAGE_ID)
    print(f"Found {len(all_pages)} total pages. Analyzing...\n")
    
    stale_pages = []
    
    for page in all_pages:
        last_edited_time = datetime.fromisoformat(page['last_edited_time'].replace('Z', '+00:00'))
        
        if last_edited_time < threshold_date:
            title = get_page_title(page)
            author = get_user_name(page['created_by']['id'])
            last_editor = get_user_name(page['last_edited_by']['id'])
            days_since_edit = (datetime.now(last_edited_time.tzinfo) - last_edited_time).days
            
            stale_pages.append({
                'title': title,
                'url': page['url'],
                'author': author,
                'last_editor': last_editor,
                'last_edited_time': page['last_edited_time'],
                'days_since_edit': days_since_edit
            })
    
    stale_pages.sort(key=lambda x: x['last_edited_time'])
    
    print(f"Found {len(stale_pages)} pages not edited in {MONTHS_THRESHOLD} months:\n")
    print("=" * 100)
    
    for i, page in enumerate(stale_pages, 1):
        print(f"{i}. {page['title']}")
        print(f"   Author: {page['author']}")
        print(f"   Last Edited By: {page['last_editor']}")
        print(f"   Last Edited: {page['last_edited_time'][:10]} ({page['days_since_edit']} days ago)")
        print(f"   URL: {page['url']}")
        print("-" * 100)
    
    csv_filename = 'stale_notion_pages.csv'
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Title', 'Author', 'Last Editor', 'Last Edited', 'Days Since Edit', 'URL']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for page in stale_pages:
            writer.writerow({
                'Title': page['title'],
                'Author': page['author'],
                'Last Editor': page['last_editor'],
                'Last Edited': page['last_edited_time'][:10],
                'Days Since Edit': page['days_since_edit'],
                'URL': page['url']
            })
    
    print(f"\n\nResults saved to {csv_filename}")
    
    # Create summary for GitHub Actions
    with open(os.environ.get('GITHUB_STEP_SUMMARY', 'summary.md'), 'w') as f:
        f.write(f"# Notion Stale Pages Report\n\n")
        f.write(f"**Total Pages Scanned:** {len(all_pages)}\n\n")
        f.write(f"**Stale Pages Found:** {len(stale_pages)}\n\n")
        f.write(f"**Threshold:** Pages not edited in {MONTHS_THRESHOLD} months\n\n")
        
        if stale_pages:
            f.write("## Top 10 Oldest Pages\n\n")
            f.write("| Title | Last Edited | Days Ago | Author |\n")
            f.write("|-------|-------------|----------|--------|\n")
            for page in stale_pages[:10]:
                f.write(f"| [{page['title']}]({page['url']}) | {page['last_edited_time'][:10]} | {page['days_since_edit']} | {page['author']} |\n")

if __name__ == "__main__":
    find_stale_pages()
