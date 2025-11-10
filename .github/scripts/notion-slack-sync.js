const { Client } = require('@notionhq/client');
const { WebClient } = require('@slack/web-api');
const fs = require('fs');
const path = require('path');

// Initialize clients
const notion = new Client({ auth: process.env.NOTION_TOKEN });
const slack = new WebClient(process.env.SLACK_BOT_TOKEN);

const ROOT_PAGE_ID = process.env.NOTION_ROOT_PAGE_ID;
const SLACK_CHANNEL = process.env.SLACK_CHANNEL_ID;
const CACHE_DIR = path.join(__dirname, '..', 'cache');
const CACHE_FILE = path.join(CACHE_DIR, 'posted-pages.json');

// Ensure cache directory exists
if (!fs.existsSync(CACHE_DIR)) {
  fs.mkdirSync(CACHE_DIR, { recursive: true });
}

// Load cache of already posted pages
function loadCache() {
  try {
    if (fs.existsSync(CACHE_FILE)) {
      const data = fs.readFileSync(CACHE_FILE, 'utf8');
      return JSON.parse(data);
    }
  } catch (error) {
    console.error('Error loading cache:', error);
  }
  return { postedPages: [] };
}

// Save cache
function saveCache(cache) {
  try {
    fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2));
  } catch (error) {
    console.error('Error saving cache:', error);
  }
}

// Get all pages recursively from a root page
async function getAllPagesRecursive(pageId, pages = [], isRoot = true) {
  try {
    // Get the page details (skip for root page as we don't want to include it)
    if (!isRoot) {
      const page = await notion.pages.retrieve({ page_id: pageId });
      if (page.object === 'page') {
        pages.push(page);
      }
    }
    
    // Get all child blocks
    let hasMore = true;
    let cursor = undefined;
    
    while (hasMore) {
      const response = await notion.blocks.children.list({
        block_id: pageId,
        page_size: 100,
        start_cursor: cursor
      });
      
      // Process each block
      for (const block of response.results) {
        if (block.type === 'child_page') {
          // Recursively get this child page and its children
          await getAllPagesRecursive(block.id, pages, false);
        }
      }
      
      hasMore = response.has_more;
      cursor = response.next_cursor;
    }
  } catch (error) {
    console.error(`Error fetching pages for ${pageId}:`, error.message);
  }
  
  return pages;
}

// Extract page title
function getPageTitle(page) {
  try {
    if (page.properties) {
      // Try different property types
      const titleProp = Object.values(page.properties).find(
        prop => prop.type === 'title'
      );
      
      if (titleProp && titleProp.title && titleProp.title.length > 0) {
        return titleProp.title.map(t => t.plain_text).join('');
      }
    }
    
    // Fallback to child_page title if available
    if (page.child_page && page.child_page.title) {
      return page.child_page.title;
    }
    
    return 'Untitled';
  } catch (error) {
    console.error('Error extracting title:', error);
    return 'Untitled';
  }
}

// Get author from page
function getAuthor(page) {
  try {
    // Try to find author in properties
    if (page.properties) {
      const authorProp = Object.values(page.properties).find(
        prop => prop.type === 'people' || 
               prop.type === 'created_by' ||
               (prop.type === 'rich_text' && prop.name && prop.name.toLowerCase().includes('author'))
      );
      
      if (authorProp) {
        if (authorProp.type === 'people' && authorProp.people && authorProp.people.length > 0) {
          return authorProp.people[0].name || 'Unknown';
        }
        if (authorProp.type === 'rich_text' && authorProp.rich_text && authorProp.rich_text.length > 0) {
          return authorProp.rich_text.map(t => t.plain_text).join('');
        }
      }
    }
    
    // Fallback to created_by
    if (page.created_by && page.created_by.name) {
      return page.created_by.name;
    }
    
    return 'Unknown Author';
  } catch (error) {
    console.error('Error extracting author:', error);
    return 'Unknown Author';
  }
}

// Check if page is public
function isPublicPage(page) {
  try {
    if (page.public_url) return true;
    
    // Check if page has public sharing enabled
    if (page.properties) {
      const statusProp = Object.values(page.properties).find(
        prop => prop.type === 'status' || 
               (prop.type === 'select' && prop.name && prop.name.toLowerCase().includes('status'))
      );
      
      if (statusProp) {
        if (statusProp.type === 'status' && statusProp.status) {
          return statusProp.status.name.toLowerCase() === 'public' ||
                 statusProp.status.name.toLowerCase() === 'published';
        }
        if (statusProp.type === 'select' && statusProp.select) {
          return statusProp.select.name.toLowerCase() === 'public' ||
                 statusProp.select.name.toLowerCase() === 'published';
        }
      }
    }
    
    // Default to considering it public if no clear indication
    return true;
  } catch (error) {
    console.error('Error checking public status:', error);
    return false;
  }
}

// Post message to Slack
async function postToSlack(page, title, author, url) {
  const message = {
    channel: SLACK_CHANNEL,
    blocks: [
      {
        type: 'section',
        text: {
          type: 'mrkdwn',
          text: `📄 *New article published!*`
        }
      },
      {
        type: 'section',
        fields: [
          {
            type: 'mrkdwn',
            text: `*📝 Article:*\n<${url}|${title}>`
          },
          {
            type: 'mrkdwn',
            text: `*✍️ Author:*\n${author}`
          }
        ]
      },
      {
        type: 'divider'
      }
    ]
  };
  
  try {
    await slack.chat.postMessage(message);
    console.log(`✅ Posted to Slack: ${title}`);
  } catch (error) {
    console.error(`❌ Error posting to Slack:`, error.message);
    throw error;
  }
}

// Main function
async function main() {
  console.log('🚀 Starting Notion to Slack sync...');
  
  // Load cache
  const cache = loadCache();
  console.log(`📦 Loaded cache with ${cache.postedPages.length} posted pages`);
  
  // Get all pages from Notion root page
  console.log('🔍 Fetching pages from Notion root page...');
  const allPages = await getAllPagesRecursive(ROOT_PAGE_ID);
  console.log(`📄 Found ${allPages.length} total pages (excluding root)`);
  
  // Current date
  const oneWeekAgo = new Date();
  oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);
  
  // Filter pages: created more than a week ago, public, and not already posted
  const pagesToPost = [];
  
  for (const page of allPages) {
    const pageId = page.id;
    const createdTime = new Date(page.created_time);
    const isOldEnough = createdTime < oneWeekAgo;
    const isPublic = isPublicPage(page);
    const alreadyPosted = cache.postedPages.includes(pageId);
    
    if (isOldEnough && isPublic && !alreadyPosted) {
      pagesToPost.push(page);
    }
  }
  
  console.log(`📬 Found ${pagesToPost.length} new pages to post`);
  
  // Post to Slack
  for (const page of pagesToPost) {
    const title = getPageTitle(page);
    const author = getAuthor(page);
    const url = page.url;
    
    console.log(`\n📤 Posting: "${title}" by ${author}`);
    
    try {
      await postToSlack(page, title, author, url);
      cache.postedPages.push(page.id);
    } catch (error) {
      console.error(`Failed to post page: ${title}`);
    }
    
    // Small delay to avoid rate limits
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  
  // Save updated cache
  saveCache(cache);
  console.log('\n✨ Sync complete!');
}

// Run the script
main().catch(error => {
  console.error('❌ Fatal error:', error);
  process.exit(1);
});
