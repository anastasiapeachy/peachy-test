import os
import json
import requests
from datetime import datetime, timedelta
from pathlib import Path
from notion_client import Client

# Initialize Notion client
notion = Client(auth=os.environ.get('NOTION_TOKEN'))

# Configuration from environment variables
ROOT_PAGE_ID = os.environ.get('ROOT_PAGE_ID')
SLACK_CHANNEL = os.environ.get('SLACK_CHANNEL')
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')

# Cache file configuration
CACHE_DIR = Path(__file__).parent.parent / 'cache'
CACHE_FILE = CACHE_DIR / 'posted_pages.json'

# Ensure cache directory exists
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def load_cache():
    """Load cache of already posted pages"""
    try:
        if CACHE_FILE.exists():
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f'Error loading cache: {e}')
    return {'posted_pages': []}


def save_cache(cache):
    """Save cache to file"""
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f'Error saving cache: {e}')


def get_all_pages_recursive(page_id, pages=None, is_root=True):
    """Recursively get all pages from a root page"""
    if pages is None:
        pages = []
    
    try:
        # Get the page details (skip for root page as we don't want to include it)
        if not is_root:
            page = notion.pages.retrieve(page_id=page_id)
            if page.get('object') == 'page':
                pages.append(page)
        
        # Get all child blocks
        has_more = True
        start_cursor = None
        
        while has_more:
            response = notion.blocks.children.list(
                block_id=page_id,
                page_size=100,
                start_cursor=start_cursor
            )
            
            # Process each block
            for block in response.get('results', []):
                if block.get('type') == 'child_page':
                    # Recursively get this child page and its children
                    get_all_pages_recursive(block['id'], pages, False)
            
            has_more = response.get('has_more', False)
            start_cursor = response.get('next_cursor')
    
    except Exception as e:
        print(f'Error fetching pages for {page_id}: {e}')
    
    return pages


def get_page_title(page):
    """Extract page title"""
    try:
        if 'properties' in page:
            # Try different property types
            for prop_name, prop_value in page['properties'].items():
                if prop_value.get('type') == 'title':
                    title_content = prop_value.get('title', [])
                    if title_content:
                        return ''.join([t.get('plain_text', '') for t in title_content])
        
        return 'Untitled'
    except Exception as e:
        print(f'Error extracting title: {e}')
        return 'Untitled'


def get_author(page):
    """Get author from page"""
    try:
        # Try to find author in properties
        if 'properties' in page:
            for prop_name, prop_value in page['properties'].items():
                prop_type = prop_value.get('type')
                
                # Check for people property
                if prop_type == 'people':
                    people = prop_value.get('people', [])
                    if people:
                        return people[0].get('name', 'Unknown')
                
                # Check for rich_text property with 'author' in name
                if prop_type == 'rich_text' and 'author' in prop_name.lower():
                    rich_text = prop_value.get('rich_text', [])
                    if rich_text:
                        return ''.join([t.get('plain_text', '') for t in rich_text])
        
        # Fallback to created_by
        if 'created_by' in page:
            return page['created_by'].get('name', 'Unknown Author')
        
        return 'Unknown Author'
    except Exception as e:
        print(f'Error extracting author: {e}')
        return 'Unknown Author'


def is_public_page(page):
    """Check if page is public"""
    try:
        # Check if page has public URL
        if page.get('public_url'):
            return True
        
        # Check if page has public sharing enabled via properties
        if 'properties' in page:
            for prop_name, prop_value in page['properties'].items():
                prop_type = prop_value.get('type')
                
                # Check status property
                if prop_type == 'status':
                    status = prop_value.get('status', {})
                    status_name = status.get('name', '').lower()
                    if status_name in ['public', 'published']:
                        return True
                
                # Check select property with 'status' in name
                if prop_type == 'select' and 'status' in prop_name.lower():
                    select = prop_value.get('select', {})
                    if select:
                        select_name = select.get('name', '').lower()
                        if select_name in ['public', 'published']:
                            return True
        
        # Default to considering it public if no clear indication
        return True
    except Exception as e:
        print(f'Error checking public status: {e}')
        return False


def post_to_slack(page, title, author, url):
    """Post message to Slack using webhook"""
    message = {
        "channel": SLACK_CHANNEL,
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "📄 *New article published!*"
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*📝 Article:*\n<{url}|{title}>"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*✍️ Author:*\n{author}"
                    }
                ]
            },
            {
                "type": "divider"
            }
        ]
    }
    
    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json=message,
            headers={'Content-Type': 'application/json'}
        )
        response.raise_for_status()
        print(f'✅ Posted to Slack: {title}')
    except Exception as e:
        print(f'❌ Error posting to Slack: {e}')
        raise


def main():
    print('🚀 Starting Notion to Slack sync...')
    
    # Load cache
    cache = load_cache()
    print(f"📦 Loaded cache with {len(cache['posted_pages'])} posted pages")
    
    # Get all pages from Notion root page
    print('🔍 Fetching pages from Notion root page...')
    all_pages = get_all_pages_recursive(ROOT_PAGE_ID)
    print(f'📄 Found {len(all_pages)} total pages (excluding root)')
    
    # Calculate date threshold (7 days ago)
    one_week_ago = datetime.now() - timedelta(days=7)
    
    # Filter pages: created more than a week ago, public, and not already posted
    pages_to_post = []
    
    for page in all_pages:
        page_id = page['id']
        created_time = datetime.fromisoformat(page['created_time'].replace('Z', '+00:00'))
        is_old_enough = created_time < one_week_ago
        is_public = is_public_page(page)
        already_posted = page_id in cache['posted_pages']
        
        if is_old_enough and is_public and not already_posted:
            pages_to_post.append(page)
    
    print(f'📬 Found {len(pages_to_post)} new pages to post')
    
    # Post to Slack
    for page in pages_to_post:
        title = get_page_title(page)
        author = get_author(page)
        url = page['url']
        
        print(f'\n📤 Posting: "{title}" by {author}')
        
        try:
            post_to_slack(page, title, author, url)
            cache['posted_pages'].append(page['id'])
        except Exception as e:
            print(f'Failed to post page: {title}')
        
        # Small delay to avoid rate limits
        import time
        time.sleep(1)
    
    # Save updated cache
    save_cache(cache)
    print('\n✨ Sync complete!')


if __name__ == '__main__':
    main()
