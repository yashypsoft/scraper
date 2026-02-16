import os
import csv
import time
import sys
import gc
import random
import threading
import gzip
import cloudscraper
from typing import Optional, Tuple
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import re

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "https://www.cymax.com").rstrip("/")
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "500"))  # Limit per sitemap
MAX_PRODUCTS = int(os.getenv("MAX_PRODUCTS", "1000"))

# Workers and delays
MAX_WORKERS = min(int(os.getenv("MAX_WORKERS", "4")), 6)
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY", "0.5"))  # Low delay

SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
OUTPUT_CSV = f"cymax_products_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ================= LOGGER =================

def log(msg: str):
    sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")
    sys.stderr.flush()

# ================= SIMPLIFIED REQUEST MANAGER =================

class RequestManager:
    def __init__(self):
        # Use ONLY cloudscraper (curl_cffi causes issues)
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            },
            delay=5
        )
        
        # Minimal headers
        self.scraper.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        })
        
        self.request_count = 0
        self.last_request_time = 0
        self.lock = threading.Lock()
    
    def _rate_limit(self):
        """Minimal rate limiting"""
        with self.lock:
            current_time = time.time()
            if self.request_count > 0:
                elapsed = current_time - self.last_request_time
                if elapsed < REQUEST_DELAY_BASE:
                    time.sleep(REQUEST_DELAY_BASE - elapsed)
            
            self.last_request_time = time.time()
            self.request_count += 1
    
    def fetch(self, url: str, retry_count: int = 0) -> Optional[str]:
        """Simple fetch with retries"""
        for retry in range(3):  # Max 3 retries
            try:
                self._rate_limit()
                response = self.scraper.get(url, timeout=30)
                
                if response.status_code == 200:
                    return response.text
                elif response.status_code == 404:
                    return None
                else:
                    if retry < 2:
                        time.sleep(1 * (retry + 1))
                        continue
                    return None
            except Exception:
                if retry < 2:
                    time.sleep(1)
                    continue
                return None
        return None

# Initialize global request manager
request_manager = RequestManager()

# ================= HTTP FUNCTIONS =================

def http_get(url: str) -> Optional[str]:
    return request_manager.fetch(url)

def load_xml(url: str) -> Optional[ET.Element]:
    data = http_get(url)
    if not data:
        return None
    try:
        return ET.fromstring(data)
    except ET.ParseError:
        # Some sitemap URLs are .xml.gz and require manual decompress.
        if url.endswith(".gz"):
            try:
                response = request_manager.scraper.get(url, timeout=30)
                if response.status_code == 200 and response.content:
                    xml_bytes = gzip.decompress(response.content)
                    return ET.fromstring(xml_bytes)
            except Exception:
                return None
        return None

def normalize_image(url: str) -> str:
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    elif url.startswith("/"):
        return CURR_URL + url
    return url


def extract_loc_values(root: ET.Element) -> list[str]:
    ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    elements = root.findall(".//ns:loc", ns)
    if not elements:
        elements = root.findall(".//loc")
    values = []
    for elem in elements:
        if elem.text:
            values.append(elem.text.strip())
    return values


def is_nested_sitemap_url(url: str) -> bool:
    lower = url.lower()
    return lower.endswith(".xml") or lower.endswith(".xml.gz")


def collect_product_urls_from_sitemap(sitemap_url: str, visited: set, depth: int = 0, max_depth: int = 10) -> list[str]:
    if sitemap_url in visited:
        return []
    if depth > max_depth:
        log(f"  ⚠️ Max nested sitemap depth reached at: {sitemap_url}")
        return []

    visited.add(sitemap_url)
    xml_root = load_xml(sitemap_url)
    if xml_root is None:
        log(f"  ❌ Failed to parse sitemap XML: {sitemap_url}")
        return []

    loc_values = extract_loc_values(xml_root)
    if not loc_values:
        return []

    tag = xml_root.tag.lower()
    # Sitemap index nodes point to other sitemap XML files.
    if "sitemapindex" in tag:
        nested_urls = [u for u in loc_values if is_nested_sitemap_url(u)]
        all_urls = []
        for nested in nested_urls:
            all_urls.extend(collect_product_urls_from_sitemap(nested, visited, depth + 1, max_depth))
        return all_urls

    # Some "urlset" files still contain nested XML links; handle that too.
    nested_urls = [u for u in loc_values if is_nested_sitemap_url(u)]
    if nested_urls and len(nested_urls) == len(loc_values):
        all_urls = []
        for nested in nested_urls:
            all_urls.extend(collect_product_urls_from_sitemap(nested, visited, depth + 1, max_depth))
        return all_urls

    product_urls = []
    for loc in loc_values:
        if '.htm' in loc and not any(x in loc for x in ['--C', '--PC', 'sitemap', 'robots']):
            product_urls.append(loc)
    return product_urls

# ================= SITEMAP HANDLER - FIXED =================

def get_all_product_urls():
    """Properly traverse sitemap index and extract product URLs"""
    log("=" * 60)
    log("SITEMAP TRAVERSAL STARTED")
    log("=" * 60)
    
    all_product_urls = []
    sitemap_urls = []
    
    # STEP 1: Get robots.txt and find sitemap
    robots_url = f"{CURR_URL}/robots.txt"
    robots_content = http_get(robots_url)
    
    sitemap_index_url = SITEMAP_INDEX  # Default
    
    if robots_content:
        for line in robots_content.split('\n'):
            if line.lower().startswith('sitemap:'):
                parts = line.split(':', 1)
                if len(parts) > 1:
                    potential_url = parts[1].strip()
                    if potential_url.startswith('http'):
                        sitemap_index_url = potential_url
                        log(f"✓ Found sitemap in robots.txt: {sitemap_index_url}")
                        break
    
    # STEP 2: Load sitemap index
    log(f"\n📂 Loading sitemap index: {sitemap_index_url}")
    index_content = http_get(sitemap_index_url)
    
    if not index_content:
        log("❌ Failed to load sitemap index")
        return []
    
    # Parse sitemap index
    try:
        root = ET.fromstring(index_content)
        
        # Try with namespace first
        ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        sitemap_elements = root.findall(".//ns:sitemap/ns:loc", ns)
        
        if not sitemap_elements:
            # Try without namespace
            sitemap_elements = root.findall(".//sitemap/loc")
        
        if not sitemap_elements:
            # Try direct loc tags
            sitemap_elements = root.findall(".//loc")
        
        for elem in sitemap_elements:
            if elem.text:
                sitemap_urls.append(elem.text)
        
        log(f"✓ Found {len(sitemap_urls)} sitemap URLs in index")
        
        # Print first few sitemaps
        for i, url in enumerate(sitemap_urls[:5]):
            log(f"  {i+1}. {url}")
        if len(sitemap_urls) > 5:
            log(f"  ... and {len(sitemap_urls)-5} more")
            
    except ET.ParseError as e:
        log(f"❌ XML Parse error: {e}")
        return []
    
    # STEP 3: Apply offset and limit
    if MAX_SITEMAPS > 0:
        sitemap_urls = sitemap_urls[SITEMAP_OFFSET:SITEMAP_OFFSET + MAX_SITEMAPS]
    elif SITEMAP_OFFSET > 0:
        sitemap_urls = sitemap_urls[SITEMAP_OFFSET:]
    
    log(f"\n📊 Processing {len(sitemap_urls)} sitemaps (offset: {SITEMAP_OFFSET})")
    
    # STEP 4: Process each sitemap recursively (handles nested XML sitemaps)
    total_product_urls = 0
    visited_sitemaps = set()
    
    for idx, sitemap_url in enumerate(sitemap_urls, 1):
        log(f"\n[{idx}/{len(sitemap_urls)}] Processing sitemap: {sitemap_url}")

        product_urls = collect_product_urls_from_sitemap(sitemap_url, visited_sitemaps, depth=0, max_depth=10)

        # Apply per-top-level-sitemap limit
        if MAX_URLS_PER_SITEMAP > 0 and len(product_urls) > MAX_URLS_PER_SITEMAP:
            product_urls = product_urls[:MAX_URLS_PER_SITEMAP]

        log(f"  ✓ Found {len(product_urls)} product URLs")
        all_product_urls.extend(product_urls)
        total_product_urls += len(product_urls)

        # Small delay between top-level sitemaps
        if idx < len(sitemap_urls):
            time.sleep(0.5)
    
    # Remove duplicates
    all_product_urls = list(set(all_product_urls))
    log(f"\n{'='*60}")
    log(f"✅ TOTAL: {len(all_product_urls)} unique product URLs found")
    log(f"{'='*60}")
    
    return all_product_urls

# ================= PRODUCT PARSER =================

def parse_product_page(html, url):
    """Extract product data from HTML"""
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        def abs_url(src):
            return urljoin(url, src) if src else ""
        
        product = {
            "product_id": "",
            "title": "",
            "price": "",
            "brand": "",
            "category": "",
            "sku": "",
            "mpn": "",
            "image": "",
            "availability": "Unknown",
        }
        
        # PRODUCT ID - from multiple sources
        # 1. From URL (fastest)
        url_match = re.search(r"/(\d+)\.htm", url)
        if url_match:
            product["product_id"] = url_match.group(1)
        
        # 2. From product-id-label span
        if not product["product_id"]:
            id_span = soup.find("span", class_="product-id-label")
            if id_span:
                product["product_id"] = id_span.get_text(strip=True)
        
        # 3. From JSON in script
        if not product["product_id"]:
            id_match = re.search(r'"productId":\s*"(\d+)"', html)
            if id_match:
                product["product_id"] = id_match.group(1)
        
        # TITLE
        h1 = soup.find("h1", itemprop="name")
        if h1:
            product["title"] = h1.get_text(strip=True)[:200]
        elif soup.title:
            product["title"] = soup.title.get_text(strip=True)[:200]
        
        # PRICE
        price_el = soup.find(id="product-main-price")
        if price_el:
            price_text = price_el.get_text()
            price_match = re.search(r"([\d,]+\.?\d*)", price_text.replace("$", "").replace(",", ""))
            if price_match:
                product["price"] = price_match.group(1)
        
        # BRAND
        brand_meta = soup.find("meta", itemprop="brand")
        if brand_meta:
            product["brand"] = brand_meta.get("content", "")[:100]
        
        if not product["brand"]:
            brand_match = re.search(r'"brandName":\s*"([^"]+)"', html)
            if brand_match:
                product["brand"] = brand_match.group(1)[:100]
        
        # CATEGORY
        crumbs = soup.select(".breadcrumb a")
        if len(crumbs) >= 3:
            product["category"] = crumbs[-2].get_text(strip=True)[:100]
        
        # SKU / MPN
        sku_match = re.search(r'"manufacturerPartNumbers":\s*\["([^"]+)"\]', html)
        if sku_match:
            product["sku"] = sku_match.group(1)[:100]
            product["mpn"] = product["sku"]
        
        # IMAGE
        main_img = soup.find("img", id="product-main-image")
        if main_img and main_img.get("src"):
            product["image"] = abs_url(main_img["src"])
        
        # AVAILABILITY
        if "Ships between" in html[:2000]:
            product["availability"] = "Available"
        
        return product
        
    except Exception as e:
        log(f"Parse error: {e}")
        return None

# ================= PRODUCT PROCESSING =================

csv_lock = threading.Lock()
success_count = 0
fail_count = 0

def process_product(url, writer, seen):
    """Process a single product and write to CSV"""
    global success_count, fail_count
    
    if url in seen:
        return
    
    with csv_lock:
        if url in seen:
            return
        seen.add(url)
    
    try:
        html = http_get(url)
        if not html:
            with csv_lock:
                fail_count += 1
            return
        
        product = parse_product_page(html, url)
        if not product or not product.get("product_id"):
            with csv_lock:
                fail_count += 1
            return
        
        # Write to CSV
        row = [
            url,                                    # Ref Product URL
            product["product_id"],                  # Ref Product ID
            product["product_id"],                  # Ref Variant ID
            product["category"],                    # Ref Category
            "",                                     # Ref Category URL
            product["brand"],                       # Ref Brand Name
            product["title"],                       # Ref Product Name
            product["sku"],                         # Ref SKU
            product["mpn"],                         # Ref MPN
            "",                                     # Ref GTIN
            product["price"],                       # Ref Price
            product["image"],                       # Ref Main Image
            1,                                      # Ref Quantity
            "Default",                             # Ref Group Attr 1
            "default",                             # Ref Group Attr 2
            product["availability"],               # Ref Status
            SCRAPED_DATE                           # Date Scraped
        ]
        
        with csv_lock:
            writer.writerow(row)
            success_count += 1
            
        if success_count % 25 == 0:
            log(f"✓ Processed: {success_count} | Failed: {fail_count} | Rate: {success_count/(time.time()-start_time if 'start_time' in globals() else 1):.1f}/s")
        
    except Exception as e:
        with csv_lock:
            fail_count += 1
        log(f"Error: {url[-50:]}... - {str(e)[:50]}")

# ================= MAIN =================

def main():
    global start_time, success_count, fail_count
    
    start_time = time.time()
    
    log("=" * 60)
    log("CYMAX SITEMAP TRAVERSAL SCRAPER")
    log("=" * 60)
    log(f"Delay: {REQUEST_DELAY_BASE}s | Workers: {MAX_WORKERS}")
    
    # Get all product URLs by traversing sitemap index
    all_product_urls = get_all_product_urls()
    
    if not all_product_urls:
        log("❌ No product URLs found")
        sys.exit(1)
    
    # Apply global product limit
    if MAX_PRODUCTS > 0:
        product_urls = all_product_urls[:MAX_PRODUCTS]
        log(f"\n🎯 Limited to first {MAX_PRODUCTS} products")
    else:
        product_urls = all_product_urls
    
    log(f"📋 Processing {len(product_urls)} products")
    
    # Create CSV file
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow([
            "Ref Product URL",
            "Ref Product ID",
            "Ref Variant ID",
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
            "Date Scraped"
        ])
        
        seen = set()
        
        # Process with thread pool
        log(f"\n🚀 Starting processing with {MAX_WORKERS} workers...")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            
            for url in product_urls:
                future = executor.submit(
                    process_product,
                    url, writer, seen
                )
                futures.append(future)
            
            # Monitor progress
            completed = 0
            total = len(futures)
            
            for future in as_completed(futures):
                completed += 1
                
                if completed % 25 == 0 or completed == total:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    eta = (total - completed) / rate if rate > 0 else 0
                    log(f"📊 Progress: {completed}/{total} ({completed/total*100:.1f}%) | Rate: {rate:.1f}/s | ETA: {eta:.0f}s")
                
                try:
                    future.result()
                except Exception as e:
                    log(f"Future error: {e}")
    
    # Final summary
    elapsed = time.time() - start_time
    
    log("\n" + "=" * 60)
    log("✅ SCRAPING COMPLETE")
    log("=" * 60)
    log(f"📁 Output: {OUTPUT_CSV}")
    log(f"✅ Success: {success_count}")
    log(f"❌ Failed: {fail_count}")
    log(f"⏱️  Time: {elapsed:.1f}s")
    log(f"⚡ Avg speed: {success_count/elapsed:.1f} products/sec")
    log(f"📊 Total requests: {request_manager.request_count}")
    
    # Show sample
    if success_count > 0:
        log("\n📋 Sample products:")
        try:
            with open(OUTPUT_CSV, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)
                for i, row in enumerate(rows[1:4], 1):
                    log(f"  {i}. ID: {row[1]} | {row[6][:50]}... | ${row[10]}")
        except:
            pass

if __name__ == "__main__":
    main()
