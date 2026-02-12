import os
import csv
import time
import sys
import gc
import threading
import json
import html
import ast
import re
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict, Tuple
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import cloudscraper
from curl_cffi import requests as cc_requests
from bs4 import BeautifulSoup

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "https://www.furniturepick.com").rstrip("/")
SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY", "1.0"))
SAMPLE_SIZE = int(os.getenv("SAMPLE_SIZE", "5"))

OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ================= CONSTANTS & COMPILED PATTERNS =================

SITEMAP_NS = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
IMAGE_EXTS = frozenset(['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg'])
DATALAYER_PATTERN = re.compile(r'dataLayer\.push\s*\(\s*(\{[\s\S]*?\})\s*\);', re.MULTILINE)
BOOL_REPLACEMENTS = [
    (':true', ':True'),
    (':false', ':False'),
    (':null', ':None'),
    ('true,', 'True,'),
    ('false,', 'False,'),
    ('null,', 'None,'),
]
CATEGORY_FIELDS = [
    'item_category', 'item_category2', 'item_category3',
    'item_category4', 'item_category5', 'item_category6',
    'item_category7', 'item_category8', 'item_category9'
]

# ================= LOGGER =================

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()

# ================= UTILITY FUNCTIONS =================

def _clean_strings(obj):
    """Recursively clean JSON-escaped strings like \\/"""
    if isinstance(obj, dict):
        return {k: _clean_strings(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_strings(v) for v in obj]
    if isinstance(obj, str):
        return obj.replace('\\/', '/')
    return obj

def normalize_image_url(url: str) -> str:
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return f"{CURR_URL}{url}"
    if not url.startswith("http"):
        return f"https://ak1.ostkcdn.com{url}" if 'ostkcdn.com' not in url else f"https://{url}"
    return url

# ================= HTTP SESSION =================

class RequestManager:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
            "Referer": f"{CURR_URL}/",
        }
        
        self.cloudscraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
            delay=10
        )
        self.cloudscraper.headers.update(self.headers)
        
        self.retry_delays = [1, 2, 4]
        self.request_count = 0
        self.last_request_time = 0
        
    def _respect_rate_limit(self, crawl_delay=None):
        current_time = time.time()
        if self.request_count > 0:
            elapsed = current_time - self.last_request_time
            base_delay = crawl_delay if crawl_delay else REQUEST_DELAY_BASE
            target_delay = random.uniform(base_delay * 0.8, base_delay * 1.5)
            
            if elapsed < target_delay:
                time.sleep(target_delay - elapsed)
        
        self.last_request_time = time.time()
        self.request_count += 1
        
        if self.request_count % 20 == 0:
            long_pause = random.uniform(0, 1)
            log(f"Taking longer pause after {self.request_count} requests: {long_pause:.1f}s")
            time.sleep(long_pause)
    
    def fetch(self, url: str, retry_count: int = 0, crawl_delay=None) -> Optional[str]:
        if retry_count >= len(self.retry_delays):
            log(f"Max retries exceeded for {url}")
            return None
        
        try:
            self._respect_rate_limit(crawl_delay)
            
            # Alternate between cloudscraper and curl_cffi
            if retry_count % 2 == 0:
                response = self.cloudscraper.get(url, timeout=45)
            else:
                response = cc_requests.get(
                    url, 
                    headers=self.headers,
                    timeout=45,
                    impersonate="chrome110"
                )
            
            if response.status_code == 200:
                return response.text
            
            if response.status_code in [403, 429, 503]:
                delay = self.retry_delays[retry_count] + random.uniform(0, 1)
                log(f"HTTP {response.status_code} for {url}, retry {retry_count+1} in {delay:.1f}s")
                time.sleep(delay)
                return self.fetch(url, retry_count + 1, crawl_delay)
            elif response.status_code == 404:
                log(f"URL not found: {url}")
                return None
            else:
                delay = self.retry_delays[retry_count]
                log(f"Retry {retry_count+1} for {url} in {delay}s")
                time.sleep(delay)
                return self.fetch(url, retry_count + 1, crawl_delay)
                
        except Exception as e:
            log(f"Request error for {url}: {e}")
            delay = self.retry_delays[retry_count]
            time.sleep(delay)
            return self.fetch(url, retry_count + 1, crawl_delay)

request_manager = RequestManager()

def http_get(url: str, crawl_delay=None) -> Optional[str]:
    return request_manager.fetch(url, crawl_delay=crawl_delay)

def load_xml(url: str, crawl_delay=None) -> Optional[ET.Element]:
    data = http_get(url, crawl_delay)
    if not data:
        return None
    try:
        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML parse error for {url}: {e}")
        return None

# ================= DATA EXTRACTION =================

def extract_datalayer(html_text: str) -> Optional[dict]:
    match = DATALAYER_PATTERN.search(html_text)
    if not match:
        return None
    
    raw = html.unescape(match.group(1))
    
    # Clean boolean/null values
    for old, new in BOOL_REPLACEMENTS:
        raw = raw.replace(old, new)
    
    # Try JSON first, then ast.literal_eval
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        try:
            if raw.strip().startswith('{'):
                raw = f'[{raw}]'
            data = ast.literal_eval(raw)
        except (SyntaxError, ValueError):
            # Clean trailing commas and try again
            raw = re.sub(r',\s*}', '}', raw)
            raw = re.sub(r',\s*]', ']', raw)
            try:
                data = json.loads(raw)
            except:
                return None
    
    return _clean_strings(data)

def extract_additional_product_info(html_text: str) -> str:
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        # More specific selector
        table = soup.select_one('table#product-attribute-specs-table, table.additional-attributes')
        
        if not table:
            return json.dumps({})
        
        additional_info = {}
        rows = table.select('tbody tr, tr')
        
        for row in rows:
            th = row.find('th')
            td = row.find('td')
            
            if th and td:
                label = th.get_text(strip=True)
                value = td.get_text(strip=True)
                
                if label and value:
                    # Create clean JSON key
                    key = re.sub(r'[^a-zA-Z0-9]+', '_', label.lower()).strip('_')
                    additional_info[key] = value
        
        return json.dumps(additional_info, ensure_ascii=False)
    except Exception as e:
        log(f"Error extracting additional data: {e}")
        return json.dumps({})

def fetch_json(url: str, crawl_delay=None, check_is_pdp_only: bool = False) -> Optional[dict]:
    """Fetch JSON data with optional isPDP check only."""
    data = http_get(url, crawl_delay)
    if not data:
        return None
    
    try:
        data_layer = extract_datalayer(data)
        if not data_layer:
            return None
        
        product_data = data_layer[0] if isinstance(data_layer, list) else data_layer
        is_pdp = product_data.get("ecommerce", {}).get("isPDP", None)
        
        if check_is_pdp_only:
            return {"isPDP": is_pdp}
        
        if is_pdp == 0:
            return None
            
        product_data["additional_product_info_html"] = extract_additional_product_info(data)
        return product_data
        
    except Exception as e:
        log(f"Error processing data for {url}: {e}")
        return None

# ================= SITEMAP CHECK =================

def check_sitemap_contains_products(sitemap_url: str, crawl_delay=None) -> bool:
    """Check if sitemap contains any product pages by sampling URLs."""
    log(f"Checking sitemap for product pages: {sitemap_url}")
    
    xml = load_xml(sitemap_url, crawl_delay)
    if not xml:
        return False
    
    # Extract URLs
    urls = []
    for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
        elements = xml.findall(path, SITEMAP_NS) if "ns:" in path else xml.findall(path)
        if elements:
            urls = [
                e.text.strip() for e in elements if e.text and 
                '.html' in e.text and 
                not any(ext in e.text for ext in IMAGE_EXTS)
            ]
            if urls:
                break
    
    if not urls:
        return False
    
    # Sample URLs
    sample_size = min(SAMPLE_SIZE, len(urls))
    sample_urls = random.sample(urls, sample_size) if len(urls) > sample_size else urls
    
    products_found = 0
    for url in sample_urls:
        data = fetch_json(url, crawl_delay, check_is_pdp_only=True)
        if data and data.get("isPDP", 0) != 0:
            products_found += 1
        time.sleep(0.5)
    
    return products_found > 0

# ================= PRODUCT DATA PROCESSING =================

def extract_product_data(product_data: dict) -> dict:
    """Extract product information from dataLayer."""
    try:
        # Get product ID
        product_id = ''
        if isinstance(product_data.get('ecomm_prodid'), list) and product_data['ecomm_prodid']:
            product_id = str(product_data['ecomm_prodid'][0])
        
        ecommerce_items = product_data.get('ecommerce', {}).get('items', [])
        
        # Get name
        name = ''
        if ecommerce_items:
            name = ecommerce_items[0].get('item_name', '').strip()
        if not name:
            name = product_data.get('product', {}).get('name', '').strip()
        
        # Get SKU
        sku = product_data.get('ecomm_prodsku', '') or product_data.get('product', {}).get('sku', '')
        
        # Get brand, quantity, price
        brand = ''
        quantity = 0
        price = ''
        
        if ecommerce_items:
            brand = ecommerce_items[0].get('item_brand', '')
            quantity = ecommerce_items[0].get('quantity', 0)
            price = str(ecommerce_items[0].get('price', ''))
        
        if not price:
            price = str(product_data.get('ecommerce', {}).get('value', ''))
        
        # Parse additional info
        mpn = sku
        category = ''
        additional_data = product_data.get('additional_product_info_html', '{}')
        
        try:
            additional_info = json.loads(additional_data)
            mpn = additional_info.get('item_number', sku)
            category = additional_info.get('product_type', '')
        except (json.JSONDecodeError, AttributeError):
            pass
        
        # Get category from ecommerce items if not found
        if not category and ecommerce_items:
            categories = []
            for field in CATEGORY_FIELDS:
                cat_value = ecommerce_items[0].get(field, '')
                if cat_value:
                    categories.append(cat_value)
            if categories:
                category = ' | '.join(categories)
        
        # Determine status
        availability = product_data.get('ecommerce', {}).get('magentoProductAvailability', '')
        status = 'SELLABLE' if availability == 'InStock' else 'OUT_OF_STOCK'
        
        return {
            'product_id': product_id,
            'name': name,
            'brand': brand,
            'price': price,
            'main_image': '',  # No image extraction in current logic
            'sku': sku,
            'mpn': mpn,
            'category': category,
            'category_url': '',
            'quantity': quantity,
            'status': status,
            'variation_id': '',
            'group_attr_1': '',
            'group_attr_2': '',
            'additional_data': additional_data,
        }
        
    except Exception as e:
        log(f"Error extracting product data: {e}")
        return {}

def process_product_data(product_url: str, writer, seen: set, stats: dict, crawl_delay=None):
    """Process a single product URL."""
    if product_url in seen:
        return
    seen.add(product_url)
    
    data = fetch_json(product_url, crawl_delay)
    if not data:
        stats['errors'] += 1
        return
    
    product_info = extract_product_data(data)
    if not product_info.get('product_id'):
        stats['errors'] += 1
        return
    
    try:
        row = [
            product_url,
            product_info['product_id'],
            product_info['variation_id'],
            product_info['category'],
            product_info['category_url'],
            product_info['brand'],
            product_info['name'],
            product_info['sku'],
            product_info['mpn'],
            '',  # GTIN
            product_info['price'],
            normalize_image_url(product_info['main_image']),
            str(product_info['quantity']),
            product_info['group_attr_1'],
            product_info['group_attr_2'],
            product_info['status'],
            product_info['additional_data'],
            SCRAPED_DATE
        ]
        
        with threading.Lock():
            writer.writerow(row)
        
        stats['products_fetched'] += 1
        stats['urls_processed'] += 1
        log(f"Fetched product {product_info['product_id']}")
        
    except Exception as e:
        log(f"Error creating row: {e}")
        stats['errors'] += 1
    
    time.sleep(REQUEST_DELAY_BASE)

# ================= ROBOTS.TXT PARSER =================

def check_robots_txt():
    """Check robots.txt for crawl delays and sitemap location."""
    robots_url = f"{CURR_URL}/robots.txt"
    log(f"Checking robots.txt: {robots_url}")
    
    content = http_get(robots_url)
    if not content:
        return None, None
    
    crawl_delay = None
    sitemap_url = None
    
    for line in content.split('\n'):
        line = line.strip().lower()
        if line.startswith('sitemap:'):
            parts = line.split(':', 1)
            if len(parts) > 1:
                url = parts[1].strip()
                if url.startswith('http'):
                    sitemap_url = url
        elif line.startswith('crawl-delay:'):
            try:
                parts = line.split(':', 1)
                if len(parts) > 1:
                    crawl_delay = float(parts[1].strip())
            except (ValueError, IndexError):
                pass
    
    return crawl_delay, sitemap_url

# ================= MAIN =================

def main():
    # Check robots.txt
    crawl_delay, robots_sitemap = check_robots_txt()
    crawl_delay = 0  # Override as per original code
    sitemap = robots_sitemap if robots_sitemap and robots_sitemap.startswith('http') else SITEMAP_INDEX
    
    # Log configuration
    log("=" * 60)
    log("Overstock Parallel Scraper")
    log(f"Timestamp: {SCRAPED_DATE}")
    log(f"Base URL: {CURR_URL}")
    log(f"Sitemap: {sitemap}")
    log(f"Offset: {SITEMAP_OFFSET}")
    log(f"Max Sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'All'}")
    log(f"Max URLs per Sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'All'}")
    log(f"Workers: {MAX_WORKERS}")
    log(f"Delay: {REQUEST_DELAY_BASE}s")
    log("=" * 60)
    
    # Load sitemap index
    log(f"Loading sitemap index from {sitemap}")
    index = load_xml(sitemap, crawl_delay)
    if not index:
        log("Failed to load sitemap index", "ERROR")
        sys.exit(1)
    
    # Extract sitemap URLs
    sitemaps = []
    for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
        elements = index.findall(path, SITEMAP_NS) if "ns:" in path else index.findall(path)
        if elements:
            sitemaps = [e.text.strip() for e in elements if e.text]
            break
    
    if not sitemaps:
        log("No sitemaps found", "ERROR")
        sys.exit(1)
    
    # Filter sitemaps based on offset and max
    if SITEMAP_OFFSET >= len(sitemaps):
        log(f"Offset {SITEMAP_OFFSET} exceeds total sitemaps ({len(sitemaps)})", "WARNING")
        sys.exit(0)
    
    end_idx = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(sitemaps)
    sitemaps_to_process = sitemaps[SITEMAP_OFFSET:end_idx]
    
    log(f"Total sitemaps: {len(sitemaps)}")
    log(f"Processing: {len(sitemaps_to_process)}")
    
    # Initialize CSV with buffered writer
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, buffer_lines=1000)  # Add buffering
        writer.writerow([
            "Ref Product URL", "Ref Product ID", "Ref Varient ID", "Ref Category",
            "Ref Category URL", "Ref Brand Name", "Ref Product Name", "Ref SKU",
            "Ref MPN", "Ref GTIN", "Ref Price", "Ref Main Image", "Ref Quantity",
            "Ref Group Attr 1", "Ref Group Attr 2", "Ref Status",
            "Additional Product Data", "Date Scrapped"
        ])
        
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
            log(f"Processing sitemap {stats['sitemaps_processed']}/{len(sitemaps_to_process)}")
            
            # Optional: Check if sitemap contains products
            # Uncomment if needed
            # if not check_sitemap_contains_products(sitemap_url, crawl_delay):
            #     log(f"Skipping non-product sitemap: {sitemap_url}")
            #     continue
            
            xml = load_xml(sitemap_url, crawl_delay)
            if not xml:
                continue
            
            # Extract URLs
            urls = []
            for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
                elements = xml.findall(path, SITEMAP_NS) if "ns:" in path else xml.findall(path)
                if elements:
                    urls = [
                        e.text.strip() for e in elements if e.text and 
                        '.html' in e.text and 
                        not any(ext in e.text for ext in IMAGE_EXTS)
                    ]
                    if urls:
                        break
            
            if not urls:
                log(f"No product URLs found in sitemap: {sitemap_url}", "WARNING")
                continue
            
            # Apply URL limit
            if MAX_URLS_PER_SITEMAP > 0:
                urls = urls[:MAX_URLS_PER_SITEMAP]
            
            log(f"Processing {len(urls)} URLs from sitemap")
            
            # Process URLs in parallel
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(process_product_data, url, writer, seen, stats, crawl_delay)
                    for url in urls
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Thread error: {e}", "ERROR")
                        stats['errors'] += 1
            
            gc.collect()
    
    # Print statistics
    log("=" * 60)
    log("SCRAPING STATISTICS")
    log("=" * 60)
    log(f"Sitemaps processed: {stats['sitemaps_processed']}")
    log(f"URLs processed: {stats['urls_processed']}")
    log(f"Products fetched: {stats['products_fetched']}")
    log(f"Errors: {stats['errors']}")
    if stats['urls_processed'] > 0:
        log(f"Success rate: {(stats['products_fetched']/stats['urls_processed']*100):.1f}%")
    log("=" * 60)
    log(f"Completed: {OUTPUT_CSV}")
    log("=" * 60)

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    if not CURR_URL:
        log("Error: CURR_URL environment variable is required", "ERROR")
        sys.exit(1)
    
    main()