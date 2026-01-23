import os
import csv
import time
import sys
import gc
import threading
import requests
import re
from typing import Optional, List, Dict
from datetime import datetime
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "").rstrip("/")
SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
API_BASE_URL = os.getenv("API_BASE_URL", "").rstrip("/")
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.15"))

OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# ================= LOGGER =================

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()

# ================= HTTP SESSION =================

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
})

def http_get(url: str) -> Optional[str]:
    for attempt in range(3):
        try:
            r = session.get(url, timeout=30, verify=False)
            if r.status_code == 200:
                return r.text
            else:
                log(f"Status {r.status_code} for {url}", "WARNING")
        except Exception as e:
            log(f"Attempt {attempt+1} failed for {url}: {type(e).__name__}", "WARNING")
            time.sleep(1)
    return None

def fetch_json(url: str) -> Optional[dict]:
    try:
        r = session.get(url, timeout=30)
        return r.json() if r.status_code == 200 else None
    except Exception as e:
        log(f"Error fetching JSON from {url}: {e}", "ERROR")
        return None

# ================= SITEMAP PROCESSING =================

def load_xml(url: str) -> Optional[ET.Element]:
    data = http_get(url)
    if not data:
        return None
    try:
        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML parsing failed for {url}: {e}", "WARNING")
        return None

def extract_product_id(product_url: str) -> Optional[str]:
    """Extract product ID from URL using various patterns"""
    patterns = [
        r'/(\d+)/product\.html$',
        r'/product/(\d+)',
        r'/(\d+)/',
        r'\?product=(\d+)',
        r'&product=(\d+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, product_url)
        if match:
            product_id = match.group(1)
            return product_id
    
    log(f"No product ID found in URL: {product_url}", "WARNING")
    return None

# ================= PRODUCT PROCESSING =================

csv_lock = threading.Lock()

def normalize_image_url(url: str) -> str:
    """Normalize image URL"""
    if url and url.startswith("//"):
        return "https:" + url
    elif url and not url.startswith("http"):
        return f"{CURR_URL}{url}" if CURR_URL else url
    return url or ""

def process_product_data(product_url: str, writer, seen: set, stats: dict):
    """Process a single product URL"""
    if product_url in seen:
        return
    seen.add(product_url)
    
    # Extract product ID
    product_id = extract_product_id(product_url)
    if not product_id:
        stats['errors'] += 1
        return
    
    # Fetch product data from API
    api_url = f"{API_BASE_URL}/{product_id}"
    data = fetch_json(api_url)
    
    if not data or data.get('statusCode') != 200 or not data.get('ok'):
        stats['errors'] += 1
        return
    
    # Process product data
    try:
        # Check for multiple variations
        multiple_variations = data.get('multipleInStockVariations', False)
        variations = data.get('variations', [])
        
        if multiple_variations and len(variations) > 1:
            # Process each variation
            for variation in variations:
                variation_id = variation.get('variationId', '')
                full_sku = variation.get('fullSku', '')
                
                # Create variation-specific URL
                if variation_id:
                    variation_url = f"{product_url}?option={variation_id}"
                else:
                    variation_url = product_url
                
                # Get breadcrumbs for category
                breadcrumbs = data.get('breadcrumbs', [])
                if breadcrumbs:
                    last_url = breadcrumbs[-1].get('url', '').lstrip('/')
                    category = breadcrumbs[-1].get('label', '')
                    category_url = f"{CURR_URL}/{last_url}" if last_url else ''
                else:
                    category = ''
                    category_url = ''
                
                # Prepare row data
                row = [
                    variation_url,  # Ref Product URL
                    product_id,     # Ref Product ID
                    variation_id,   # Ref Varient ID
                    category,       # Ref Category
                    category_url,   # Ref Category URL
                    data.get('brand', {}).get('name', ''),  # Ref Brand Name
                    data.get('name', ''),  # Ref Product Name
                    full_sku,       # Ref SKU
                    data.get('specifications', {}).get('Model Number', [''])[0] if data.get('specifications', {}).get('Model Number') else '',  # Ref MPN
                    '',  # Ref GTIN (empty for now)
                    variation.get('prices', {}).get('salePrice', {}).get('amount', 
                        variation.get('prices', {}).get('basePrice', {}).get('amount',
                        data.get('selectedPrice', {}).get('amount', ''))),  # Ref Price
                    normalize_image_url(data.get('images', [{}])[0].get('url', '') if data.get('images') else data.get('imageData', {}).get('productImageUrl', '')),  # Ref Main Image
                    variation.get('quantityAvailable', ''),  # Ref Quantity
                    data.get('specifications', {}).get('Color', [''])[0] if data.get('specifications', {}).get('Color') else '',  # Ref Group Attr 1
                    data.get('specifications', {}).get('Material', [''])[0] if data.get('specifications', {}).get('Material') else data.get('specifications', {}).get('Top Material', [''])[0] if data.get('specifications', {}).get('Top Material') else '',  # Ref Group Attr 2
                    'In Stock' if variation.get('status') == 'SELLABLE' or variation.get('sellableStatus') == 'SELLABLE' else 'Out of Stock',  # Ref Status
                    SCRAPED_DATE  # Date Scrapped
                ]
                
                with csv_lock:
                    writer.writerow(row)
                
                stats['products_fetched'] += 1
            
            log(f"Fetched {len(variations)} variations for product {product_id}")
        else:
            # Single product or no variations
            breadcrumbs = data.get('breadcrumbs', [])
            if breadcrumbs:
                last_url = breadcrumbs[-1].get('url', '').lstrip('/')
                category = breadcrumbs[-1].get('label', '')
                category_url = f"{CURR_URL}/{last_url}" if last_url else ''
            else:
                category = ''
                category_url = ''
            
            # Prepare row data for single product
            row = [
                product_url,  # Ref Product URL
                product_id,   # Ref Product ID
                data.get('variations', [{}])[0].get('variationId', '') if data.get('variations') else '',  # Ref Varient ID
                category,     # Ref Category
                category_url, # Ref Category URL
                data.get('brand', {}).get('name', ''),  # Ref Brand Name
                data.get('name', ''),  # Ref Product Name
                data.get('details', {}).get('sku', ''),  # Ref SKU
                data.get('specifications', {}).get('Model Number', [''])[0] if data.get('specifications', {}).get('Model Number') else '',  # Ref MPN
                '',  # Ref GTIN
                data.get('selectedPrice', {}).get('amount', ''),  # Ref Price
                normalize_image_url(data.get('images', [{}])[0].get('url', '') if data.get('images') else data.get('imageData', {}).get('productImageUrl', '')),  # Ref Main Image
                data.get('variations', [{}])[0].get('quantityAvailable', '') if data.get('variations') else '',  # Ref Quantity
                data.get('specifications', {}).get('Color', [''])[0] if data.get('specifications', {}).get('Color') else '',  # Ref Group Attr 1
                data.get('specifications', {}).get('Material', [''])[0] if data.get('specifications', {}).get('Material') else data.get('specifications', {}).get('Top Material', [''])[0] if data.get('specifications', {}).get('Top Material') else '',  # Ref Group Attr 2
                'In Stock' if data.get('inStock', False) else 'Out of Stock',  # Ref Status
                SCRAPED_DATE  # Date Scrapped
            ]
            
            with csv_lock:
                writer.writerow(row)
            
            stats['products_fetched'] += 1
            log(f"Fetched single product {product_id}")
        
        stats['urls_processed'] += 1
        
    except Exception as e:
        log(f"Error processing product {product_id}: {e}", "ERROR")
        stats['errors'] += 1
    
    # Respect request delay
    time.sleep(REQUEST_DELAY)

# ================= MAIN =================

def main():
    log("=" * 60)
    log("Starting Parallel Scraper")
    log(f"Timestamp: {SCRAPED_DATE}")
    log(f"Base URL: {CURR_URL}")
    log(f"API Base URL: {API_BASE_URL}")
    log(f"Sitemap Index: {SITEMAP_INDEX}")
    log(f"Sitemap Offset: {SITEMAP_OFFSET}")
    log(f"Max Sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'All'}")
    log(f"Max URLs per Sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'All'}")
    log(f"Max Workers: {MAX_WORKERS}")
    log(f"Request Delay: {REQUEST_DELAY}s")
    log("=" * 60)
    
    # Load sitemap index
    index = load_xml(SITEMAP_INDEX)
    if not index:
        log("Failed to load sitemap index", "ERROR")
        sys.exit(1)
    
    # Extract sitemap URLs
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemaps = [e.text for e in index.findall(".//ns:sitemap/ns:loc", ns)]
    
    # Fallback if namespace not found
    if not sitemaps:
        sitemaps = [e.text for e in index.findall(".//sitemap/loc")]
    
    if not sitemaps:
        log("No sitemaps found in index", "ERROR")
        sys.exit(1)
    
    # Apply offset and limit
    if SITEMAP_OFFSET >= len(sitemaps):
        log(f"Offset {SITEMAP_OFFSET} exceeds total sitemaps ({len(sitemaps)})", "WARNING")
        sys.exit(0)
    
    end_index = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(sitemaps)
    sitemaps_to_process = sitemaps[SITEMAP_OFFSET:end_index]
    
    log(f"Total sitemaps found: {len(sitemaps)}")
    log(f"Sitemaps to process: {len(sitemaps_to_process)}")
    
    # Initialize CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow([
            "Ref Product URL",
            "Ref Product ID",
            "Ref Varient ID",
            "Ref Category",
            "Ref Category URL",
            "Ref Brand Name",
            "Ref Product Name",
            "Ref SKU",
            "Ref MPN",
            "Ref GTIN",
            "Ref Price",
            "Ref Main Image",
            "Ref Quantity",
            "Ref Group Attr 1",
            "Ref Group Attr 2",
            "Ref Status",
            "Date Scrapped"
        ])
        
        # Initialize tracking
        seen = set()
        stats = {
            'sitemaps_processed': 0,
            'urls_processed': 0,
            'products_fetched': 0,
            'errors': 0
        }
        
        # Process each sitemap
        for sitemap_url in sitemaps_to_process:
            stats['sitemaps_processed'] += 1
            log(f"Processing sitemap {stats['sitemaps_processed']}/{len(sitemaps_to_process)}: {sitemap_url}")
            
            # Load product sitemap
            xml = load_xml(sitemap_url)
            if not xml:
                log(f"Failed to load sitemap: {sitemap_url}", "ERROR")
                continue
            
            # Extract product URLs
            urls = [e.text for e in xml.findall(".//ns:url/ns:loc", ns)]
            if not urls:
                urls = [e.text for e in xml.findall(".//url/loc")]
            
            if not urls:
                log(f"No URLs found in sitemap: {sitemap_url}", "WARNING")
                continue
            
            # Apply URL limit
            if MAX_URLS_PER_SITEMAP > 0:
                original_count = len(urls)
                urls = urls[:MAX_URLS_PER_SITEMAP]
                log(f"Limited to {len(urls)} out of {original_count} URLs")
            else:
                log(f"Found {len(urls)} product URLs in this sitemap")
            
            # Process URLs in parallel
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(process_product_data, url, writer, seen, stats)
                    for url in urls
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Error in thread execution: {e}", "ERROR")
                        stats['errors'] += 1
            
            # Clean up memory
            gc.collect()
    
    # Print statistics
    log("=" * 60)
    log("SCRAPING STATISTICS")
    log("=" * 60)
    log(f"Sitemaps processed: {stats['sitemaps_processed']}")
    log(f"URLs processed: {stats['urls_processed']}")
    log(f"Products successfully fetched: {stats['products_fetched']}")
    log(f"Errors encountered: {stats['errors']}")
    if stats['urls_processed'] > 0:
        success_rate = (stats['products_fetched'] / stats['urls_processed']) * 100
        log(f"Success rate: {success_rate:.1f}%")
    log("=" * 60)
    log(f"Completed: {OUTPUT_CSV}")
    log("=" * 60)

if __name__ == "__main__":
    # Validate environment variables
    if not CURR_URL:
        log("Error: CURR_URL environment variable is required", "ERROR")
        sys.exit(1)
    
    if not API_BASE_URL:
        log("Error: API_BASE_URL environment variable is required", "ERROR")
        sys.exit(1)
    
    main()