import os
import csv
import time
import sys
import random
import cloudscraper
from bs4 import BeautifulSoup
import re
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import gc
from urllib.parse import urljoin


# ================= CONFIG =================
CURR_URL = os.getenv("CURR_URL", "https://www.cymax.com").rstrip("/")
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_PRODUCTS = int(os.getenv("MAX_PRODUCTS", "100"))
MAX_WORKERS = max(int(os.getenv("MAX_WORKERS", "3")), 4)
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY", "3.0"))

OUTPUT_CSV = f"cymax_products_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ================= LOGGER =================
def log(msg: str):
    sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")
    sys.stderr.flush()

# ================= WORKING REQUEST MANAGER =================
class CymaxScraper:
    def __init__(self):
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            },
            delay=10
        )
        
        # Headers that work with Cymax
        # self.scraper.headers.update({
        #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        #     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        #     "Accept-Language": "en-US,en;q=0.5",
        #     # "Accept-Encoding": "gzip, deflate, br",
        #     "DNT": "1",
        #     "Connection": "keep-alive",
        #     "Upgrade-Insecure-Requests": "1",
        #     "Sec-Fetch-Dest": "document",
        #     "Sec-Fetch-Mode": "navigate",
        #     "Sec-Fetch-Site": "none",
        #     "Sec-Fetch-User": "?1",
        # })
        
        self.request_count = 0
        self.last_request = 0
        self.lock = threading.Lock()
    
    def _rate_limit(self):
        """Respect rate limits with random delays"""
        with self.lock:
            now = time.time()
            if self.request_count > 0:
                elapsed = now - self.last_request
                min_delay = REQUEST_DELAY_BASE * 0.8
                max_delay = REQUEST_DELAY_BASE * 1.5
                target_delay = random.uniform(min_delay, max_delay)
                
                if elapsed < target_delay:
                    time.sleep(0.5)
            
            self.last_request = time.time()
            self.request_count += 1
            
            # Longer pause every 10 requests
            if self.request_count % 10 == 0:
                long_pause = random.uniform(8, 15)
                log(f"Long pause after {self.request_count} requests: {long_pause:.1f}s")
                time.sleep(0.5)
    
    def get(self, url, max_retries=3):
        """Get URL with retries"""
        for retry in range(max_retries):
            try:
                self._rate_limit()
                response = self.scraper.get(url, timeout=45)
                
                if response.status_code == 200:
                    # Check for Cloudflare block
                    if "Cloudflare" in response.text and "Attention Required" in response.text:
                        log(f"Cloudflare block detected on {url}")
                        if retry < max_retries - 1:
                            delay = (retry + 1) * 5
                            log(f"Retry {retry + 1} in {delay}s...")
                            time.sleep(0.5)
                            continue
                        return None
                    return response.text
                elif response.status_code == 404:
                    log(f"404 Not Found: {url}")
                    return None
                else:
                    log(f"HTTP {response.status_code} for {url}")
                    if retry < max_retries - 1:
                        delay = (retry + 1) * 3
                        time.sleep(0.5)
                        continue
                    return None
                    
            except Exception as e:
                log(f"Request error for {url}: {e}")
                if retry < max_retries - 1:
                    delay = (retry + 1) * 2
                    time.sleep(0.5)
                    continue
        
        return None

# ================= PRODUCT PARSER =================
def parse_product_page(html, url):
    try:
        soup = BeautifulSoup(html, "html.parser")

        def abs_url(src):
            return urljoin(url, src) if src else ""

        product = {
            "url": url,
            "product_id": "",
            "title": "",
            "price": "",
            "original_price": "",
            "brand": "",
            "category": "",
            "category_full": "",
            "sku": "",
            "mpn": "",
            "description": "",
            "images": [],
            "dimensions": "",
            "weight": "",
            "availability": "Unknown",
            "shipping_info": "",
            "variants": [],
            "specifications": {},
        }

        # ---------- PRODUCT ID ----------
        id_span = soup.find("span", class_="product-id-label")
        if id_span:
            product["product_id"] = id_span.get_text(strip=True)

        if not product["product_id"]:
            m = re.search(r'"productId":\s*"(\d+)"', html)
            if m:
                product["product_id"] = m.group(1)

        if not product["product_id"]:
            m = re.search(r"Item:\s*(\d+)", html)
            if m:
                product["product_id"] = m.group(1)

        if not product["product_id"]:
            m = re.search(r"/(\d+)[-A-Z]*\.htm", url)
            if m:
                product["product_id"] = m.group(1)

        # ---------- TITLE ----------
        h1 = soup.find("h1", itemprop="name")
        if h1:
            product["title"] = h1.get_text(strip=True)
        elif soup.title:
            product["title"] = soup.title.get_text(strip=True)

        # ---------- PRICE ----------
        price_el = soup.find(id="product-main-price")
        if price_el:
            clean = price_el.get_text().replace("$", "").replace(",", "")
            m = re.search(r"\d+(\.\d+)?", clean)
            if m:
                product["price"] = m.group(0)

        # OG fallback
        if not product["price"]:
            og = soup.find("meta", property="og:price:amount")
            if og:
                product["price"] = og.get("content", "")

        # ---------- ORIGINAL PRICE ----------
        lp = soup.find("span", class_="list-price")
        if lp:
            clean = lp.get_text().replace("$", "").replace(",", "")
            m = re.search(r"\d+(\.\d+)?", clean)
            if m:
                product["original_price"] = m.group(0)

        # ---------- BRAND ----------
        meta_brand = soup.find("meta", itemprop="brand")
        if meta_brand:
            product["brand"] = meta_brand.get("content", "")

        if not product["brand"]:
            m = re.search(r'"brandName":\s*"([^"]+)"', html)
            if m:
                product["brand"] = m.group(1)

        # ---------- CATEGORY ----------
        crumbs = soup.select(".breadcrumb li a")
        cats = [c.get_text(strip=True) for c in crumbs if c.get_text(strip=True) != "Home"]
        if cats:
            product["category_full"] = " > ".join(cats)
            product["category"] = cats[-1]

        # ---------- SKU / MPN ----------
        m = re.search(r'"manufacturerPartNumbers":\s*\["([^"]+)"\]', html)
        if m:
            product["sku"] = m.group(1)
            product["mpn"] = m.group(1)

        # ---------- DESCRIPTION ----------
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            product["description"] = meta_desc.get("content", "")

        # ---------- IMAGES ----------
        main_img = soup.find("img", id="product-main-image")
        if main_img and main_img.get("src"):
            product["images"].append(abs_url(main_img["src"]))

        for img in soup.select("#gallery-slider-area img"):
            src = img.get("data-src") or img.get("src")
            if src:
                full = abs_url(src)
                if full not in product["images"]:
                    product["images"].append(full)

        # ---------- DIMENSIONS / WEIGHT ----------
        dims = soup.find(id="product-dimensions")
        if dims:
            text = dims.get_text(" ", strip=True)

            m = re.search(r"Dimensions:\s*([^P]+)", text)
            if m:
                product["dimensions"] = m.group(1).strip()

            m = re.search(r"Product Weight:\s*([^\s]+.*)", text)
            if m:
                product["weight"] = m.group(1).strip()

        # ---------- SHIPPING ----------
        ship = soup.find(id="product-shipping-info")
        if ship:
            txt = ship.get_text(" ", strip=True)
            product["shipping_info"] = txt
            if "Ships" in txt:
                product["availability"] = "Available"

        # ---------- VARIANTS ----------
        product["variants"] = [{
            "type": "default",
            "title": "Default",
            "price": product["price"],
            "url": url,
            "image": product["images"][0] if product["images"] else "",
            "dimensions": product["dimensions"],
        }]

        # ---------- SPECS ----------
        specs = {}
        table = soup.find("table", class_="table-striped")
        if table:
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    k = tds[0].get_text(strip=True)
                    v = tds[1].get_text(strip=True)
                    if k and v:
                        specs[k] = v

        product["specifications"] = specs

        return product

    except Exception as e:
        print(f"Parse error: {e}")
        return None

# ================= SITEMAP DISCOVERY =================
def discover_product_urls(scraper):
    """Discover product URLs from sitemaps and category pages"""
    log("Starting URL discovery...")
    all_urls = set()
    
    # 1. Try sitemap.xml
    sitemap_urls = [
        f"{CURR_URL}/sitemap.xml"
    ]
    
    for sitemap_url in sitemap_urls:
        log(f"Trying sitemap: {sitemap_url}")
        content = scraper.get(sitemap_url)
        if content:
            # Try to parse as XML
            urls = extract_urls_from_sitemap(content)
            if urls:
                log(f"Found {len(urls)} URLs in {sitemap_url}")
                all_urls.update(urls)
                break
    
    for sitemap_url in all_urls:
        log(f"Trying sitemap: {sitemap_url}")
        content = scraper.get(sitemap_url)
        if content:
            # Try to parse as XML
            urls = extract_urls_from_sitemap(content)
            if urls:
                log(f"Found {len(urls)} URLs in {sitemap_url}")
                all_urls.update(urls)
                break
    
    
    # Filter to product URLs (ending with .htm)
    product_urls = [url for url in all_urls if '.htm' in url and CURR_URL in url]
    
    # Remove non-product URLs
    product_urls = [url for url in product_urls if not any(x in url for x in ['--C', '--PC', 'robots', 'sitemap'])]
    
    log(f"Discovered {len(product_urls)} product URLs")
    return list(product_urls)

def extract_urls_from_sitemap(content):
    """Extract URLs from sitemap content"""
    urls = []
    
    # Method 1: XML sitemap
    try:
        from xml.etree import ElementTree as ET
        root = ET.fromstring(content)
        
        # Look for <loc> tags
        for elem in root.iter():
            if elem.tag.endswith('loc') and elem.text:
                urls.append(elem.text)
    except:
        pass
    
    # Method 2: Text extraction
    if not urls:
        url_patterns = [
            r'<loc>\s*(https?://[^<]+)\s*</loc>',
            r'https?://[^\s<>"]+\.htm',
        ]
        
        for pattern in url_patterns:
            matches = re.findall(pattern, content)
            urls.extend(matches)
    
    return list(set(urls))  # Remove duplicates

def get_category_urls(scraper):
    """Get main category URLs from homepage"""
    categories = []
    
    # Try to get homepage and extract categories
    content = scraper.get(CURR_URL)
    if content:
        soup = BeautifulSoup(content, 'html.parser')
        
        # Look for main menu links
        menu_links = soup.select('#main-menu a, .nav a')
        for link in menu_links:
            href = link.get('href', '')
            if href and '.htm' in href and CURR_URL in href and '--C' in href:
                categories.append(href)
    
    # Add common categories as fallback
    if not categories:
        categories = [
            f"{CURR_URL}/bathroom-vanities--C1107.htm",
            f"{CURR_URL}/Office-Chairs-More--PC401.htm",
            f"{CURR_URL}/desks--PC117.htm",
            f"{CURR_URL}/Tables--PC283.htm",
            f"{CURR_URL}/living-room--PC575.htm",
            f"{CURR_URL}/bedroom-furniture--PC330.htm",
            f"{CURR_URL}/kitchen-dining--PC588.htm",
            f"{CURR_URL}/Lighting--PC903.htm",
        ]
    
    return list(set(categories))

def extract_urls_from_category(scraper, category_url):
    """Extract product URLs from category page"""
    urls = []
    
    content = scraper.get(category_url)
    if not content:
        return urls
    
    soup = BeautifulSoup(content, 'html.parser')
    
    # Look for product links
    product_links = soup.find_all('a', href=re.compile(r'\.htm$'))
    for link in product_links:
        href = link.get('href', '')
        if href and '.htm' in href and CURR_URL in href:
            # Filter out category pages
            if not any(x in href for x in ['--C', '--PC']):
                urls.append(href)
    
    return list(set(urls))

# ================= PRODUCT PROCESSING =================
csv_lock = threading.Lock()

def process_product(url, writer, scraper, seen, results):
    """Process a single product and write to CSV"""
    if url in seen:
        return
    
    seen.add(url)
    
    try:
        html = scraper.get(url)
        if not html:
            log(f"Failed to fetch: {url}")
            return
        
        product = parse_product_page(html, url)
        if not product:
            return
        
        # Process each variant
        for idx, variant in enumerate(product['variants']):
            variant_id = f"{product['product_id']}_{idx+1}" if idx > 0 else product['product_id']
            
            # Prepare CSV row
            row = [
                variant['url'],  # Ref Product URL
                product['product_id'],  # Ref Product ID
                variant_id,  # Ref Variant ID
                product['category'],  # Ref Category
                "",  # Ref Category URL
                product['brand'],  # Ref Brand Name
                f"{product['title']} - {variant['title']}" if variant['title'] != 'Default' else product['title'],  # Ref Product Name
                product['sku'],  # Ref SKU
                product['mpn'],  # Ref MPN
                "",  # Ref GTIN
                variant['price'] or product['price'],  # Ref Price
                variant['image'] or (product['images'][0] if product['images'] else ""),  # Ref Main Image
                1,  # Ref Quantity (assume 1)
                variant['title'],  # Ref Group Attr 1
                variant['type'],  # Ref Group Attr 2
                product['availability'],  # Ref Status
                SCRAPED_DATE  # Date Scraped
            ]
            
            with csv_lock:
                writer.writerow(row)
        
        results.append(product['product_id'])
        log(f"✓ Processed {len(product['variants'])} variants for {product['product_id']}")
        
    except Exception as e:
        log(f"Error processing {url}: {e}")

# ================= MAIN =================
def main():
    log("=" * 60)
    log("CYMAX.COM PRODUCT SCRAPER")
    log("=" * 60)
    
    # Initialize scraper
    scraper = CymaxScraper()
    
    # Discover product URLs
    all_product_urls = discover_product_urls(scraper)
    
    if not all_product_urls:
        log("ERROR: No product URLs found")
        sys.exit(1)
    
    log(f"Total product URLs discovered: {len(all_product_urls)}")
    
    # Apply offset and limit
    if MAX_PRODUCTS > 0:
        product_urls = all_product_urls[SITEMAP_OFFSET:SITEMAP_OFFSET + MAX_PRODUCTS]
    else:
        product_urls = all_product_urls[SITEMAP_OFFSET:]
    
    log(f"Processing {len(product_urls)} URLs in this chunk")
    
    # Create CSV file
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write header matching your format
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
        results = []
        
        # Process products with thread pool
        log(f"Starting processing with {MAX_WORKERS} workers...")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            
            for url in product_urls:
                future = executor.submit(
                    process_product,
                    url, writer, scraper, seen, results
                )
                futures.append(future)
            
            # Monitor progress
            completed = 0
            total = len(futures)
            
            for future in as_completed(futures):
                completed += 1
                
                if completed % 5 == 0 or completed == total:
                    log(f"Progress: {completed}/{total} products processed")
                
                try:
                    future.result()
                except Exception as e:
                    log(f"Future error: {e}")
                
                # GC and pause occasionally
                if completed % 20 == 0:
                    gc.collect()
                    time.sleep(0.5)
    
    # Summary
    log("\n" + "=" * 60)
    log("SCRAPING COMPLETE")
    log("=" * 60)
    log(f"Output file: {OUTPUT_CSV}")
    log(f"Unique products processed: {len(results)}")
    log(f"Total requests made: {scraper.request_count}")
    
    # Show sample output
    if results:
        log("\nSample of scraped data:")
        try:
            with open(OUTPUT_CSV, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)
                
                if len(rows) > 1:
                    log("Headers: " + ", ".join(rows[0]))
                    for i, row in enumerate(rows[1:4], 1):  # First 3 data rows
                        log(f"Row {i}: ID={row[1]}, Product={row[6][:50]}..., Price=${row[10]}")
        except Exception as e:
            log(f"Could not read sample: {e}")

if __name__ == "__main__":
    main()
