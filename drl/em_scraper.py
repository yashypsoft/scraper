import os
import csv
import time
import sys
import gc
import threading
import requests
import random
import re
import json
import html
import ast
import cloudscraper
from curl_cffi import requests as cc_requests
from typing import Optional, List, Dict, Tuple
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "https://www.emmamason.com").rstrip("/")
SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY", "1.0"))  # Fixed variable name
SAMPLE_SIZE = int(os.getenv("SAMPLE_SIZE", "5"))  # Number of URLs to sample for checking

OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ================= LOGGER =================

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()

# ================= HTTP SESSION =================

session = cloudscraper.create_scraper()

def get_sitemap_from_robots_txt():
    try:
        robots_url = f"{CURR_URL}/robots.txt"
        scraper = cloudscraper.create_scraper()
        response = scraper.get(robots_url, timeout=10)
        
        sitemap_url = None
        for line in response.text.split('\n'):
            if line.lower().startswith('sitemap:'):
                sitemap_url = line.split(':', 1)[1].strip()
                break
        
        if sitemap_url:
            print(f"Extracted Sitemap URL: {sitemap_url}")
            return sitemap_url
        else:
            print("No Sitemap directive found in robots.txt")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching robots.txt: {e}")
        return None

def check_robots_txt():
    """Check robots.txt for crawl delays and sitemap location"""
    robots_url = f"{CURR_URL}/robots.txt"
    log(f"Checking robots.txt: {robots_url}")
    
    robots_content = http_get(robots_url)
    if robots_content:
        lines = robots_content.split('\n')
        crawl_delay = None
        sitemap_url = None
        
        for line in lines:
            line = line.strip()
            # Handle sitemap entries - correctly parse the full URL
            if line.lower().startswith('sitemap:'):
                parts = line.split(':', 1)
                if len(parts) > 1:
                    potential_url = parts[1].strip()
                    # Validate it's a proper URL
                    if potential_url.startswith('http'):
                        sitemap_url = potential_url
                        log(f"Found valid sitemap in robots.txt: {sitemap_url}")
            # Handle crawl-delay entries
            elif line.lower().startswith('crawl-delay:'):
                try:
                    parts = line.split(':', 1)
                    if len(parts) > 1:
                        crawl_delay = float(parts[1].strip())
                        log(f"Found Crawl-delay: {crawl_delay} seconds")
                except (ValueError, IndexError) as e:
                    log(f"Error parsing crawl-delay: {e}")
        
        return crawl_delay, sitemap_url
    
    log("No robots.txt found or couldn't fetch it")
    return None, None

def _clean_strings(obj):
    """Recursively clean JSON-escaped strings like \\/"""
    if isinstance(obj, dict):
        return {k: _clean_strings(v) for k, v in obj.items()}
    
    if isinstance(obj, list):
        return [_clean_strings(v) for v in obj]
    
    if isinstance(obj, str):
        return obj.replace('\\/', '/')
    
    return obj

def extract_datalayer(html_text):
    patterns = [
        r'dataLayer\.push\s*\(\s*(\{[\s\S]*?\})\s*\);',
    ]
    
    raw = None
    for pattern in patterns:
        match = re.search(pattern, html_text)
        if match:
            raw = match.group(1)
            break
    
    if not raw:
        return None
    
    raw = html.unescape(raw)

    raw = raw.replace(':true', ':True') \
             .replace(':false', ':False') \
             .replace(':null', ':None') \
             .replace('true,', 'True,') \
             .replace('false,', 'False,') \
             .replace('null,', 'None,')
    
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        try:
            if raw.strip().startswith('{'):
                raw = f'[{raw}]'
            data = ast.literal_eval(raw)
        except (SyntaxError, ValueError):
            raw = re.sub(r',\s*}', '}', raw)
            raw = re.sub(r',\s*]', ']', raw)
            try:
                data = json.loads(raw)
            except:
                return None

    return _clean_strings(data)

def extract_additional_product_info(html_text):
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        table = soup.find('table', id='product-attribute-specs-table')
        
        if not table:
            table = soup.find('table', class_='additional-attributes')
            if not table:
                return json.dumps({})
        
        additional_info = {}
        
        tbody = table.find('tbody')
        if tbody:
            rows = tbody.find_all('tr')
        else:
            rows = table.find_all('tr')
        
        for row in rows:
            th = row.find('th')
            td = row.find('td')
            
            if th and td:
                label_text = th.get_text(strip=True)
                data_text = td.get_text(strip=True)
                
                if label_text and data_text:
                    json_key = re.sub(r'[^a-zA-Z0-9_]', '_', label_text.lower().replace(' ', '_'))
                    json_key = json_key.strip('_')
                    additional_info[json_key] = data_text
        
        return json.dumps(additional_info, ensure_ascii=False)
    except Exception as e:
        print(f"Error while processing additional Data: {e}")
        return json.dumps({})

class RequestManager:
    def __init__(self):
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            },
            delay=10
        )
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
            "Referer": CURR_URL + "/",
        }
        
        self.scraper.headers.update(self.headers)
        self.retry_delays = [1, 2, 4, 8, 16]
        self.request_count = 0
        self.last_request_time = 0
        
    def _respect_rate_limit(self, crawl_delay=None):
        current_time = time.time()
        if self.request_count > 0:
            elapsed = current_time - self.last_request_time
            base_delay = crawl_delay if crawl_delay else REQUEST_DELAY_BASE
            min_delay = base_delay * 0.8
            max_delay = base_delay * 1.5
            target_delay = random.uniform(min_delay, max_delay)
            
            if elapsed < target_delay:
                sleep_time = target_delay - elapsed
                time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.request_count += 1
        
        if self.request_count % 20 == 0:
            long_pause = random.uniform(0, 1)
            log(f"Taking longer pause after {self.request_count} requests: {long_pause:.1f}s")
            time.sleep(long_pause)
    
    def _fetch_with_cloudscraper(self, url: str, crawl_delay=None) -> Optional[Tuple[str, int]]:
        try:
            self._respect_rate_limit(crawl_delay)
            response = self.scraper.get(url, timeout=45)
            if response.status_code == 200:
                return response.text, response.status_code
            return None, response.status_code
        except Exception as e:
            log(f"Cloudscraper error for {url}: {e}")
            return None, 0
    
    def _fetch_with_curl_cffi(self, url: str, crawl_delay=None) -> Optional[Tuple[str, int]]:
        try:
            self._respect_rate_limit(crawl_delay)
            response = cc_requests.get(
                url, 
                headers=self.headers,
                timeout=45,
                impersonate="chrome110"
            )
            if response.status_code == 200:
                return response.text, response.status_code
            return None, response.status_code
        except Exception as e:
            log(f"Curl_cffi error for {url}: {e}")
            return None, 0
    
    def fetch(self, url: str, retry_count: int = 0, crawl_delay=None) -> Optional[str]:
        if retry_count >= len(self.retry_delays):
            log(f"Max retries exceeded for {url}")
            return None
        
        if retry_count == 0:
            content, status = self._fetch_with_cloudscraper(url, crawl_delay)
        elif retry_count % 2 == 1:
            content, status = self._fetch_with_curl_cffi(url, crawl_delay)
        else:
            content, status = self._fetch_with_cloudscraper(url, crawl_delay)
        
        if content:
            return content
        
        if status in [403, 429, 503]:
            delay = self.retry_delays[retry_count] + random.uniform(0, 1)
            log(f"HTTP {status} for {url}, retry {retry_count+1} in {delay:.1f}s")
            time.sleep(delay)
            return self.fetch(url, retry_count + 1, crawl_delay)
        elif status == 404:
            log(f"URL not found: {url}")
            return None
        
        if status != 200:
            delay = self.retry_delays[retry_count]
            log(f"Retry {retry_count+1} for {url} in {delay}s")
            time.sleep(delay)
            return self.fetch(url, retry_count + 1, crawl_delay)
        
        return None

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

def fetch_json(url: str, crawl_delay=None, check_is_pdp_only: bool = False) -> Optional[dict]:
    """
    Fetch JSON data with optional isPDP check only.
    If check_is_pdp_only is True, returns minimal dict with isPDP flag.
    """
    data = http_get(url, crawl_delay)
    if not data:
        print('data not fetched for json')
        return None
    try:
        data_layer = extract_datalayer(data)
        if not data_layer:
            print("No dataLayer found")
            return None
        
        product_data = data_layer[0]
        is_pdp = product_data.get("ecommerce", {}).get("isPDP", None)
        
        if check_is_pdp_only:
            # Return minimal dict with isPDP flag for checking
            return {"isPDP": is_pdp}
        
        if is_pdp == 0:
            print(f"isPDP is 0 for {url}, returning early")
            return None
            
        additional_info = extract_additional_product_info(data)
        product_data["additional_product_info_html"] = additional_info
        return product_data
    except json.JSONDecodeError as e:
        log(f"JSON decode error for {url}: {e}")
        return None
    except Exception as e:
        log(f"Error processing data for {url}: {e}")
        return None

def check_sitemap_contains_products(sitemap_url: str, crawl_delay=None) -> bool:
    """
    Check if a sitemap contains any product pages by sampling URLs.
    Returns True if at least one URL has isPDP != 0, False otherwise.
    """
    log(f"Checking sitemap for product pages: {sitemap_url}")
    
    xml = load_xml(sitemap_url, crawl_delay)
    if not xml:
        log(f"Failed to load sitemap for checking: {sitemap_url}", "ERROR")
        return False
    
    # Extract URLs
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
        elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
        if elements:
            urls = [
                e.text.strip()
                for e in elements
                if e.text
                and not any(ext in e.text for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg'])
                and ('.html' in e.text)
            ]
            if urls:
                break
    
    if not urls:
        log(f"No valid URLs found in sitemap for checking", "WARNING")
        return False
    
    # Sample URLs to check
    sample_size = min(SAMPLE_SIZE, len(urls))
    sample_urls = random.sample(urls, sample_size) if len(urls) > sample_size else urls
    
    log(f"Sampling {sample_size} URLs from sitemap to check for product pages")
    
    products_found = 0
    for i, url in enumerate(sample_urls):
        log(f"  Checking sample {i+1}/{sample_size}: {url}")
        
        data = fetch_json(url, crawl_delay, check_is_pdp_only=True)
        if data and data.get("isPDP", 0) != 0:
            products_found += 1
            log(f"  ✓ Found product page (isPDP != 0)")
        else:
            log(f"  ✗ Not a product page (isPDP == 0 or no data)")
        
        time.sleep(0.5)  # Small delay between checks
    
    if products_found > 0:
        log(f"Sitemap contains product pages ({products_found}/{sample_size} samples are products)")
        return True
    else:
        log(f"Sitemap appears to have NO product pages (0/{sample_size} samples are products)")
        return False

csv_lock = threading.Lock()

def normalize_image_url(url: str) -> str:
    if not url:
        return ""
    
    if url.startswith("//"):
        return "https:" + url
    elif url.startswith("/"):
        return f"{CURR_URL}{url}"
    elif not url.startswith("http"):
        return f"https://ak1.ostkcdn.com{url}" if 'ostkcdn.com' not in url else f"https://{url}"
    
    return url

def extract_product_data(product_data: dict) -> dict:
    try:
        product_id = str(product_data.get('ecomm_prodid', [''])[0] if isinstance(product_data.get('ecomm_prodid'), list) and product_data.get('ecomm_prodid') else '')
        
        ecommerce_items = product_data.get('ecommerce', {}).get('items', [])
        name = ''
        if ecommerce_items:
            name = ecommerce_items[0].get('item_name', '').strip()
        if not name:
            name = product_data.get('product', {}).get('name', '').strip()
        if not product_id:
            name = product_data.get('product', {}).get('id', '').strip()
        
        sku = product_data.get('ecomm_prodsku', '')
        if not sku:
            sku = product_data.get('product', {}).get('sku', '')
        
        brand = ''
        quantity = 0
        price = ''
        
        if ecommerce_items:
            brand = ecommerce_items[0].get('item_brand', '')
            quantity = ecommerce_items[0].get('quantity', 0)
            price_item = ecommerce_items[0].get('price', '')
            if price_item:
                price = str(price_item)
        
        if not price:
            ecomm_value = product_data.get('ecommerce', {}).get('value', '')
            if ecomm_value:
                price = str(ecomm_value)
        
        main_image = ''
        additional_data = product_data.get('additional_product_info_html', '')
        mpn = sku
        category = ''
        
        try:
            additional_info_dict = json.loads(additional_data)
            mpn = additional_info_dict.get('item_number',"")
            category = additional_info_dict.get('product_type',"")
        except Exception as e:
            print(f"Error setting mpn or category : {e}")
        
        category_url = ''
        
        if ecommerce_items and not category:
            category_fields = [
                'item_category', 'item_category2', 'item_category3',
                'item_category4', 'item_category5', 'item_category6',
                'item_category7', 'item_category8', 'item_category9'
            ]
            categories = []
            for field in category_fields:
                cat_value = ecommerce_items[0].get(field, '')
                if cat_value:
                    categories.append(cat_value)
            
            if categories:
                category = ' | '.join(categories)
        
        availability = product_data.get('ecommerce', {}).get('magentoProductAvailability', '')
        status = 'OUT_OF_STOCK'
        
        if availability == 'InStock':
            status = 'SELLABLE'
        
        variation_id = ''
        group_attr_1 = ''
        group_attr_2 = ''
        
        return {
            'product_id': product_id,
            'name': name,
            'brand': brand,
            'price': price,
            'main_image': main_image,
            'sku': sku,
            'mpn': mpn,
            'category': category,
            'category_url': category_url,
            'quantity': quantity,
            'status': status,
            'variation_id': variation_id,
            'group_attr_1': group_attr_1,
            'group_attr_2': group_attr_2,
            'additional_data': additional_data,
        }
        
    except Exception as e:
        print(f"Error extracting product data: {e}")
        return {}

def process_product_data(product_url: str, writer, seen: set, stats: dict, crawl_delay=None):
    if product_url in seen:
        return
    seen.add(product_url)
    
    log(f"Processing product URL: {product_url}", "DEBUG")
    
    data = fetch_json(product_url, crawl_delay)

    if not data:
        stats['errors'] += 1
        log(f"No data found for product {product_url}", "ERROR")
        return
    
    product_info = extract_product_data(data)
    if not product_info.get('product_id'):
        stats['errors'] += 1
        log(f"Invalid data for product {product_info.get('product_id', 'unknown')}", "ERROR")
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
            '',
            product_info['price'],
            normalize_image_url(product_info['main_image']),
            product_info['quantity'],
            product_info['group_attr_1'],
            product_info['group_attr_2'],
            product_info['status'],
            product_info['additional_data'],
            SCRAPED_DATE
        ]
        
        with csv_lock:
            writer.writerow(row)
        
        stats['products_fetched'] += 1
        log(f"Fetched product {product_info['product_id']}: {product_info['name'][:50]}...", "INFO")
        
    except Exception as e:
        log(f"Error creating row for product {product_info.get('product_id', 'unknown')}: {e}", "ERROR")
        stats['errors'] += 1
    
    time.sleep(REQUEST_DELAY_BASE)
    stats['urls_processed'] += 1

# ================= MAIN =================

def main():
    crawl_delay, robots_sitemap = check_robots_txt()
    crawl_delay = 0
    sitemap = SITEMAP_INDEX
    if robots_sitemap and robots_sitemap.startswith('http'):
        sitemap = robots_sitemap
        log(f"Using sitemap from robots.txt: {sitemap}")
    else:
        if robots_sitemap:
            log(f"Invalid sitemap URL in robots.txt: '{robots_sitemap}', using default")
        else:
            log(f"No valid sitemap in robots.txt, using default: {sitemap}")

    if crawl_delay:
        if crawl_delay > 30:
            log(f"Crawl-delay {crawl_delay}s is too high, capping at 30s")
            crawl_delay = 30
        log(f"Respecting crawl-delay: {crawl_delay} seconds between requests")
    else:
        log(f"Using default request delay: {REQUEST_DELAY_BASE} seconds")
    
    log("=" * 60)
    log("Overstock Parallel Scraper")
    log(f"Timestamp: {SCRAPED_DATE}")
    log(f"Base URL: {CURR_URL}")
    log(f"Sitemap Index: {sitemap}")
    log(f"Sitemap Offset: {SITEMAP_OFFSET}")
    log(f"Max Sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'All'}")
    log(f"Max URLs per Sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'All'}")
    log(f"Max Workers: {MAX_WORKERS}")
    log(f"Request Delay: {REQUEST_DELAY_BASE}s")
    log(f"Sample Size for Checking: {SAMPLE_SIZE}")
    log("=" * 60)
    
    log(f"Loading sitemap index from {sitemap}")
    index = load_xml(sitemap, crawl_delay)
    if index is None:
        log("Failed to load sitemap index", "ERROR")
        sys.exit(1)
    
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemaps = []
    
    for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
        elements = index.findall(path, ns) if "ns:" in path else index.findall(path)
        if elements:
            sitemaps = [e.text.strip() for e in elements if e.text]
            break
    
    if not sitemaps:
        log("No sitemaps found with XML parsing, trying regex", "WARNING")
    
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
            "Additional Product Data",
            "Date Scrapped"
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
            log(f"Processing sitemap {stats['sitemaps_processed']}/{len(sitemaps_to_process)}: {sitemap_url}")
            
            # Check if this sitemap contains product pages before processing
            # if not check_sitemap_contains_products(sitemap_url, crawl_delay):
            #     log(f"Sitemap {sitemap_url} contains no product pages. Skipping and continuing...", "WARNING")
            #     log("NOTE: This might indicate the entire sitemap index has no product pages.")
            #     log("Consider checking the sitemap structure or stopping the process.")
            #     continue
            
            xml = load_xml(sitemap_url, crawl_delay)
            if not xml:
                log(f"Failed to load sitemap: {sitemap_url}", "ERROR")
                continue
            
            urls = []
            for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
                elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
                if elements:
                    urls = [
                        e.text.strip()
                        for e in elements
                        if e.text
                        and not any(ext in e.text for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg'])
                        and ('.html' in e.text)
                    ]
                    if urls:
                        break
            
            if not urls:
                log(f"No product URLs found in sitemap: {sitemap_url}", "WARNING")
                continue
            
            if MAX_URLS_PER_SITEMAP > 0:
                original_count = len(urls)
                urls = urls[:MAX_URLS_PER_SITEMAP]
                log(f"Limited to {len(urls)} out of {original_count} URLs")
            else:
                log(f"Found {len(urls)} product URLs in this sitemap")
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(process_product_data, url, writer, seen, stats, crawl_delay)
                    for url in urls
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Error in thread execution: {e}", "ERROR")
                        stats['errors'] += 1
            
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
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    if not CURR_URL:
        log("Error: CURR_URL environment variable is required", "ERROR")
        sys.exit(1)
    
    main()