import os
import csv
import re
import time
from notion_client import Client
from langdetect import detect

# grab env vars
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ROOT_PAGE_ID = os.getenv("SLACK_WEBHOOK_URL")  # yeah, reusing this var for page ID

if not NOTION_TOKEN or not ROOT_PAGE_ID:
    raise ValueError("Missing NOTION_TOKEN or SLACK_WEBHOOK_URL env vars")

# clean up page IDs - handle URLs, dashes, etc
def normalize_id(raw_id):
    if not isinstance(raw_id, str):
        return raw_id
    s = raw_id.strip()
    # extract hex ID from URL if present
    match = re.search(r"([0-9a-fA-F]{32})", s.replace("-", ""))
    if match:
        return match.group(1)
    return s.replace("-", "")

ROOT_PAGE_ID = normalize_id(ROOT_PAGE_ID)
notion = Client(auth=NOTION_TOKEN)

def get_all_pages():
    """Pull all pages from workspace using search"""
    pages = []
    cursor = None
    
    while True:
        try:
            resp = notion.search(
                query="",
                filter={"value": "page", "property": "object"},
                start_cursor=cursor
            )
        except Exception as e:
            print(f"Search failed: {e}")
            break
            
        pages.extend(resp.get("results", []))
        
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
        
    return pages

def get_title(page):
    """Extract page title, with fallback"""
    props = page.get("properties", {}) or {}
    
    for prop in props.values():
        if prop.get("type") == "title":
            parts = [t.get("plain_text", "") for t in prop.get("title", [])]
            if parts:
                return "".join(parts)
    
    # fallback - try block API
    try:
        blk = notion.blocks.retrieve(block_id=page["id"])
        if blk.get("type") == "child_page":
            return blk["child_page"].get("title", "(untitled)")
    except:
        pass
        
    return "(untitled)"

def make_url(page_id):
    clean = page_id.replace("-", "")
    return f"https://www.notion.so/{clean}"

def get_blocks(block_id):
    """Fetch all child blocks"""
    blocks = []
    cursor = None
    
    while True:
        try:
            resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)
        except Exception as e:
            print(f"Can't get blocks for {block_id}: {e}")
            break
            
        blocks.extend(resp.get("results", []))
        
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
        
    return blocks

def extract_text(block):
    """Pull text from a block"""
    btype = block.get("type")
    if btype and isinstance(block.get(btype), dict):
        rich = block[btype].get("rich_text", [])
        return "".join([t.get("plain_text", "") for t in rich]).strip()
    return ""

def detect_lang(text):
    try:
        return detect(text)
    except:
        return "unknown"

def count_words(text):
    return len(re.findall(r'\b\w+\b', text))

def is_child_of_root(page, root_id, page_index):
    """
    Walk up parent chain to see if this page descends from root.
    Uses page_index dict for fast lookups, falls back to API if needed.
    """
    visited = set()
    current = page
    
    while True:
        parent = current.get("parent", {}) or {}
        ptype = parent.get("type")
        
        if ptype == "page_id":
            pid = parent.get("page_id")
            if not pid:
                return False
                
            pid_clean = normalize_id(pid)
            
            # found our root!
            if pid_clean == root_id:
                return True
                
            # avoid loops
            if pid_clean in visited:
                return False
            visited.add(pid_clean)
            
            # try to get parent page
            parent_page = page_index.get(pid_clean)
            if not parent_page:
                try:
                    parent_page = notion.pages.retrieve(page_id=pid_clean)
                except:
                    return False
                    
            current = parent_page
            continue
            
        elif ptype == "workspace":
            return False
            
        elif ptype == "database_id":
            # sometimes pages live in databases - check db's parent
            db_id = parent.get("database_id")
            if not db_id:
                return False
                
            try:
                db = notion.databases.retrieve(database_id=db_id)
                db_parent = db.get("parent", {})
                
                if db_parent.get("type") == "page_id":
                    pid_clean = normalize_id(db_parent.get("page_id"))
                    if pid_clean == root_id:
                        return True
                    if pid_clean in visited:
                        return False
                    visited.add(pid_clean)
                    
                    parent_page = page_index.get(pid_clean) or notion.pages.retrieve(page_id=pid_clean)
                    current = parent_page
                    continue
                    
                return False
            except:
                return False
                
        elif ptype == "block_id":
            # blocks can be parents too (rare but happens)
            bid = parent.get("block_id")
            if not bid:
                return False
                
            try:
                blk = notion.blocks.retrieve(block_id=bid)
                blk_parent = blk.get("parent", {})
                if not blk_parent:
                    return False
                current = {"id": bid, "parent": blk_parent}
                continue
            except:
                return False
        else:
            return False

def analyze_page(page_id):
    """Count Russian and English words on a single page"""
    ru = 0
    en = 0
    
    blocks = get_blocks(page_id)
    
    for block in blocks:
        # skip child pages - they're counted separately
        if block.get("type") == "child_page":
            continue
            
        text = extract_text(block)
        if not text:
            continue
            
        lang = detect_lang(text)
        words = count_words(text)
        
        if lang == "ru":
            ru += words
        elif lang == "en":
            en += words
            
    return ru, en

def main():
    start = time.time()
    
    print("Fetching all pages...")
    pages = get_all_pages()
    print(f"Found {len(pages)} total pages")
    
    # build index for fast lookups
    page_index = {}
    for p in pages:
        pid = normalize_id(p.get("id"))
        page_index[pid] = p
    
    # filter to pages under our root
    selected = []
    for p in pages:
        pid = normalize_id(p.get("id"))
        
        # include root itself
        if pid == ROOT_PAGE_ID:
            selected.append(p)
            continue
            
        try:
            if is_child_of_root(p, ROOT_PAGE_ID, page_index):
                selected.append(p)
        except Exception as e:
            print(f"Error checking {pid}: {e}")
    
    print(f"Found {len(selected)} pages under root")
    
    # analyze each page
    results = []
    for p in selected:
        pid = normalize_id(p.get("id"))
        title = get_title(p)
        url = make_url(pid)
        
        # get author
        author = "(unknown)"
        try:
            author_info = p.get("created_by", {})
            author = author_info.get("name", "Unknown")
            
            if not author or author == "Unknown":
                user_id = author_info.get("id")
                if user_id:
                    try:
                        user_data = notion.users.retrieve(user_id=user_id)
                        author = user_data.get("name", "Unknown")
                    except:
                        pass
        except:
            pass
        
        # analyze content
        ru, en = analyze_page(pid)
        total = ru + en
        ru_pct = (ru / total * 100) if total else 0
        en_pct = (en / total * 100) if total else 0
        
        results.append({
            "Page Title": title,
            "Page URL": url,
            "Author": author,
            "% Russian": round(ru_pct, 2),
            "% English": round(en_pct, 2)
        })
    
    # sort by English % (high to low)
    results.sort(key=lambda x: x["% English"], reverse=True)
    
    # write CSV
    fname = "notion_language_percentages.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Page Title", "Page URL", "Author", "% Russian", "% English"]
        )
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\nSaved {len(results)} rows to {fname}")
    print(f"Took {time.time() - start:.1f}s")

if __name__ == "__main__":
    main()
