import os
import csv
import re
import time
from notion_client import Client
from langdetect import detect

# --- Settings ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("ROOT_PAGE_ID")

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("❌ Environment variables NOTION_TOKEN and ROOT_PAGE_ID must be set")

notion = Client(auth=NOTION_TOKEN)

# --- Helper Functions ---

def clean_page_id(page_id):
    """Remove hyphens from page ID"""
    return page_id.replace("-", "")

def get_page_title(page):
    """Extract page title"""
    props = page.get("properties", {})
    for prop in props.values():
        if prop.get("type") == "title":
            title_parts = [t["plain_text"] for t in prop["title"]]
            if title_parts:
                return "".join(title_parts)
    return "(Untitled)"

def get_page_url(page_id):
    """Generate Notion URL for page"""
    clean_id = clean_page_id(page_id)
    return f"https://www.notion.so/{clean_id}"

def get_block_children(block_id):
    """Get all child blocks of a page/block"""
    children = []
    next_cursor = None
    while True:
        try:
            response = notion.blocks.children.list(
                block_id=block_id, 
                start_cursor=next_cursor,
                page_size=100
            )
            children.extend(response["results"])
            if not response.get("has_more"):
                break
            next_cursor = response.get("next_cursor")
            time.sleep(0.3)  # Rate limiting
        except Exception as e:
            print(f"⚠️ Error getting blocks for {block_id}: {e}")
            break
    return children

def extract_text_from_block(block):
    """Extract text content from a block"""
    text = ""
    btype = block.get("type")
    
    if btype and isinstance(block.get(btype), dict):
        block_content = block[btype]
        
        # Handle rich_text field
        if "rich_text" in block_content:
            text = "".join([t.get("plain_text", "") for t in block_content["rich_text"]])
        
        # Handle title field (for child_page, child_database)
        elif "title" in block_content:
            text = block_content.get("title", "")
    
    return text.strip()

def detect_language(text):
    """Detect language of text"""
    try:
        if len(text.strip()) < 3:
            return "unknown"
        return detect(text)
    except:
        return "unknown"

def count_words(text):
    """Count words in text"""
    return len(re.findall(r'\b\w+\b', text))

def get_all_subpages(page_id, visited=None):
    """Recursively find all subpages under a given page"""
    if visited is None:
        visited = set()
    
    if page_id in visited:
        return []
    
    visited.add(page_id)
    subpages = []
    
    try:
        blocks = get_block_children(page_id)
        
        for block in blocks:
            block_type = block.get("type")
            
            # Found a child page
            if block_type == "child_page":
                child_id = block["id"]
                subpages.append(child_id)
                # Recursively get subpages of this child
                subpages.extend(get_all_subpages(child_id, visited))
            
            # Check if block has children (like toggle blocks, columns, etc.)
            elif block.get("has_children"):
                subpages.extend(get_all_subpages(block["id"], visited))
        
    except Exception as e:
        print(f"⚠️ Error processing page {page_id}: {e}")
    
    return subpages

def analyze_page_content(page_id):
    """Analyze language content of a page and all its nested blocks"""
    ru_words = 0
    en_words = 0
    
    def process_blocks(block_id):
        nonlocal ru_words, en_words
        
        blocks = get_block_children(block_id)
        
        for block in blocks:
            # Extract text from current block
            text = extract_text_from_block(block)
            
            if text:
                lang = detect_language(text)
                words = count_words(text)
                
                if lang == "ru":
                    ru_words += words
                elif lang == "en":
                    en_words += words
            
            # Process nested blocks (but not child_page - those are separate pages)
            if block.get("has_children") and block.get("type") != "child_page":
                process_blocks(block["id"])
    
    process_blocks(page_id)
    return ru_words, en_words

def export_to_csv(results, filename="notion_language_analysis.csv"):
    """Export results to CSV file"""
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, 
            fieldnames=["Page Title", "Page URL", "% Russian", "% English"]
        )
        writer.writeheader()
        writer.writerows(results)
    print(f"✅ CSV saved: {filename}")

# --- Main Execution ---

if __name__ == "__main__":
    start_time = time.time()
    results = []
    
    print(f"🔍 Starting analysis from root page: {ROOT_PAGE_ID}")
    
    # Get all subpages recursively
    all_page_ids = [ROOT_PAGE_ID] + get_all_subpages(ROOT_PAGE_ID)
    print(f"📄 Found {len(all_page_ids)} pages (including root)")
    
    # Analyze each page
    for idx, page_id in enumerate(all_page_ids, 1):
        try:
            print(f"[{idx}/{len(all_page_ids)}] Processing {page_id}...", end=" ")
            
            # Get page info
            page = notion.pages.retrieve(page_id=page_id)
            title = get_page_title(page)
            url = get_page_url(page_id)
            
            # Analyze content
            ru_words, en_words = analyze_page_content(page_id)
            total = ru_words + en_words
            
            ru_percent = (ru_words / total * 100) if total else 0
            en_percent = (en_words / total * 100) if total else 0
            
            results.append({
                "Page Title": title,
                "Page URL": url,
                "% Russian": round(ru_percent, 2),
                "% English": round(en_percent, 2)
            })
            
            print(f"✓ {title[:50]}")
            
        except Exception as e:
            print(f"✗ Error: {e}")
            continue
    
    # Export results
    export_to_csv(results)
    
    elapsed = time.time() - start_time
    print(f"\n⏱ Completed in {elapsed:.1f} seconds ({len(results)} pages analyzed)")
    
    # Summary statistics for GitHub Actions
    if results:
        total_pages = len(results)
        avg_russian = sum(r["% Russian"] for r in results) / total_pages
        avg_english = sum(r["% English"] for r in results) / total_pages
        
        print(f"\n📊 Summary:")
        print(f"   Total pages analyzed: {total_pages}")
        print(f"   Average Russian content: {avg_russian:.2f}%")
        print(f"   Average English content: {avg_english:.2f}%")
