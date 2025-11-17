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

print("=" * 100)
print("DEBUG INFORMATION")
print("=" * 100)
print(f"NOTION_TOKEN present: {bool(NOTION_TOKEN)}")
print(f"ROOT_PAGE_ID: {ROOT_PAGE_ID}")
print(f"MONTHS_THRESHOLD: {MONTHS_THRESHOLD}")
print("=" * 100)

# Initialize Notion client
notion = Client(auth=NOTION_TOKEN)

# Calculate date threshold
threshold_date = datetime.now() - timedelta(days=MONTHS_THRESHOLD * 30)

def test_root_page_access():
    """Test if we can access the root page"""
    try:
        print(f"\nTesting access to root page: {ROOT_PAGE_ID}")
        page = notion.pages.retrieve(page_id=ROOT_PAGE_ID)
        print(f"✓ Successfully retrieved root page!")
        print(f"  Page object type: {page.get('object')}")
        print(f"  Page ID: {page.get('id')}")
        print(f"  Created time: {page.get('created_time')}")
        print(f"  Last edited: {page.get('last_edited_time')}")
        
        # Try to get the title
        title = get_page_title(page)
        print(f"  Page title: {title}")
        
        # Check if it's archived
        if page.get('archived'):
            print(f"  ⚠️  WARNING: This page is archived!")
        
        return True
    except Exception as e:
        print(f"✗ ERROR accessing root page: {e}")
        print(f"\nError type: {type(e).__name__}")
        print("\nPossible solutions:")
        print("1. Make sure the ROOT_PAGE_ID is correct")
        print("   - It should be 32 characters with no spaces")
        print("   - Format: abc123def456... (can include dashes)")
        print("2. Share the page with your integration in Notion:")
        print("   - Open the page in Notion")
        print("   - Click '...' menu → 'Connections' → Select your integration")
        print("3. Make sure your NOTION_TOKEN is correct")
        return False

def get_all_child_pages(page_id, all_pages=None, depth=0, processed_ids=None):
    """Recursively get all child pages under a parent page at ALL levels"""
    if all_pages is None:
        all_pages = []
    if processed_ids is None:
        processed_ids = set()
    
    # Avoid infinite loops by tracking processed pages
    if page_id in processed_ids:
        print(f"{'  ' * depth}⚠️  Skipping {page_id} - already processed")
        return all_pages
    processed_ids.add(page_id)
    
    indent = "  " * depth
    print(f"{indent}{'=' * 50}")
    print(f"{indent}Scanning page at depth {depth}: {page_id[:8]}...")
    print(f"{indent}{'=' * 50}")
    
    try:
        # Get all blocks in this page
        response = notion.blocks.children.list(block_id=page_id, page_size=100)
        
        total_blocks = len(response['results'])
        print(f"{indent}Found {total_blocks} blocks/items in this page")
        
        # Debug: show what types of blocks we found
        block_types = {}
        for block in response['results']:
            block_type = block['type']
            block_types[block_type] = block_types.get(block_type, 0) + 1
        
        print(f"{indent}Block types found: {block_types}")
        
        child_pages_count = 0
        pages_to_process = []
        
        # First pass: collect all child pages
        for block in response['results']:
            if block['type'] in ['child_page', 'child_database']:
                child_pages_count += 1
                pages_to_process.append(block['id'])
                print(f"{indent}  → Found {block['type']}: {block['id'][:8]}...")
        
        # Handle pagination for first page of results
        page_num = 1
        while response.get('has_more'):
            page_num += 1
            print(f"{indent}📄 Loading page {page_num} of blocks...")
            response = notion.blocks.children.list(
                block_id=page_id,
                page_size=100,
                start_cursor=response['next_cursor']
            )
            
            for block in response['results']:
                if block['type'] in ['child_page', 'child_database']:
                    child_pages_count += 1
                    pages_to_process.append(block['id'])
                    print(f"{indent}  → Found {block['type']}: {block['id'][:8]}...")
        
        print(f"{indent}✓ Total child pages/databases at this level: {child_pages_count}")
        
        if child_pages_count == 0:
            print(f"{indent}⚠️  No child pages found at this level")
            if depth == 0:
                print(f"{indent}\n⚠️  IMPORTANT: The root page has NO child pages!")
                print(f"{indent}   Make sure you're pointing to a page that contains subpages.")
        
        # Second pass: retrieve each page and recursively process its children
        for i, page_id_to_process in enumerate(pages_to_process, 1):
            if page_id_to_process in processed_ids:
                print(f"{indent}  [{i}/{len(pages_to_process)}] Skipping - already processed")
                continue
            
            print(f"{indent}  [{i}/{len(pages_to_process)}] Processing page {page_id_to_process[:8]}...")
            
            try:
                page = notion.pages.retrieve(page_id=page_id_to_process)
                all_pages.append(page)
                title = get_page_title(page)
                last_edited = page['last_edited_time'][:10]
                print(f"{indent}      ✓ Title: '{title}' (Last edited: {last_edited})")
                
                # Recursively get children of this page
                get_all_child_pages(page_id_to_process, all_pages, depth + 1, processed_ids)
                
            except Exception as e:
                print(f"{indent}      ✗ Error: {e}")
        
        return all_pages
    
    except Exception as e:
        print(f"{indent}✗ ERROR listing children: {e}")
        print(f"{indent}   Error type: {type(e).__name__}")
        import traceback
        print(f"{indent}   Traceback:")
        for line in traceback.format_exc().split('\n'):
            print(f"{indent}   {line}")
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
    print(f"\n{'=' * 100}")
    print(f"STARTING SCAN")
    print(f"{'=' * 100}")
    print(f"Searching for pages not edited since {threshold_date.strftime('%Y-%m-%d')}...\n")
    
    # First test if we can access the root page
    if not test_root_page_access():
        print("\n⚠️  Cannot access root page. Please fix the issues above and try again.")
        # Create empty CSV even on failure
        create_empty_csv()
        return
    
    print(f"\n{'=' * 100}")
    print("SCANNING FOR CHILD PAGES")
    print(f"{'=' * 100}\n")
    
    # Get all pages under the root page
    all_pages = get_all_child_pages(ROOT_PAGE_ID)
    
    print(f"\n{'=' * 100}")
    print(f"SCAN COMPLETE")
    print(f"{'=' * 100}")
    print(f"Total pages found: {len(all_pages)}\n")
    
    if len(all_pages) == 0:
        print("⚠️  No child pages found under the root page!")
        print("\nPossible reasons:")
        print("1. The root page has no subpages")
        print("2. The subpages are not shared with your integration")
        print("3. You might need to connect the integration to child pages as well")
        # Create empty CSV
        create_empty_csv()
        return
    
    # Filter pages that haven't been edited in the threshold period
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
    
    # Sort by last edited time (oldest first)
    stale_pages.sort(key=lambda x: x['last_edited_time'])
    
    # Print results
    print(f"\n{'=' * 100}")
    print(f"RESULTS")
    print(f"{'=' * 100}")
    print(f"Found {len(stale_pages)} pages not edited in {MONTHS_THRESHOLD} months:\n")
    print("=" * 100)
    
    for i, page in enumerate(stale_pages, 1):
        print(f"{i}. {page['title']}")
        print(f"   Author: {page['author']}")
        print(f"   Last Edited By: {page['last_editor']}")
        print(f"   Last Edited: {page['last_edited_time'][:10]} ({page['days_since_edit']} days ago)")
        print(f"   URL: {page['url']}")
        print("-" * 100)
    
    # Always create CSV file
    create_csv_report(stale_pages, len(all_pages))
    
    # Create summary for GitHub Actions
    create_github_summary(stale_pages, len(all_pages))

def create_empty_csv():
    """Create an empty CSV file when no data is available"""
    csv_filename = 'stale_notion_pages.csv'
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Title', 'Author', 'Last Editor', 'Last Edited', 'Days Since Edit', 'URL']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
    print(f"\n✓ Empty CSV file created: {csv_filename}")

def create_csv_report(stale_pages, total_pages):
    """Create CSV report with results"""
    csv_filename = 'stale_notion_pages.csv'
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Title', 'Author', 'Last Editor', 'Last Edited', 'Days Since Edit', 'URL']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        if stale_pages:
            for page in stale_pages:
                writer.writerow({
                    'Title': page['title'],
                    'Author': page['author'],
                    'Last Editor': page['last_editor'],
                    'Last Edited': page['last_edited_time'][:10],
                    'Days Since Edit': page['days_since_edit'],
                    'URL': page['url']
                })
        else:
            # Add a note row if no stale pages found
            writer.writerow({
                'Title': f'No stale pages found (scanned {total_pages} pages)',
                'Author': '',
                'Last Editor': '',
                'Last Edited': '',
                'Days Since Edit': '',
                'URL': ''
            })
    
    print(f"\n✓ CSV report created: {csv_filename}")
    print(f"  Total pages scanned: {total_pages}")
    print(f"  Stale pages found: {len(stale_pages)}")

def create_github_summary(stale_pages, total_pages):
    """Create GitHub Actions summary"""
    if not os.environ.get('GITHUB_STEP_SUMMARY'):
        return
        
    with open(os.environ.get('GITHUB_STEP_SUMMARY'), 'w') as f:
        f.write(f"# Notion Stale Pages Report\n\n")
        f.write(f"**Total Pages Scanned:** {total_pages}\n\n")
        f.write(f"**Stale Pages Found:** {len(stale_pages)}\n\n")
        f.write(f"**Threshold:** Pages not edited in {MONTHS_THRESHOLD} months\n\n")
        
        if stale_pages:
            f.write("## Top 10 Oldest Pages\n\n")
            f.write("| Title | Last Edited | Days Ago | Author |\n")
            f.write("|-------|-------------|----------|--------|\n")
            for page in stale_pages[:10]:
                f.write(f"| [{page['title']}]({page['url']}) | {page['last_edited_time'][:10]} | {page['days_since_edit']} | {page['author']} |\n")
        else:
            f.write("## ✅ No Stale Pages Found\n\n")
            f.write(f"All {total_pages} pages have been edited within the last {MONTHS_THRESHOLD} months.\n")

if __name__ == "__main__":
    try:
        find_stale_pages()
    except Exception as e:
        print(f"\n{'=' * 100}")
        print(f"CRITICAL ERROR")
        print(f"{'=' * 100}")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        
        # Still create an empty CSV so the workflow doesn't fail
        print(f"\n{'=' * 100}")
        print("Creating empty CSV file due to error...")
        create_empty_csv()
        
        # Exit with error code
        sys.exit(1)
