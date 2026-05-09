import os
import re
import json
import cloudscraper
import subprocess
import sys
from datetime import datetime
from urllib.parse import urlparse
from bs4 import BeautifulSoup

class UnifiedScraper:
    def __init__(self, competitors_file="competitors.json"):
        self.competitors_file = competitors_file
        self.competitors = self.load_competitors()
        self.session = cloudscraper.create_scraper() # Use cloudscraper to bypass anti-bots
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "max-age=0",
            "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1"
        })

    def load_competitors(self):
        if not os.path.exists(self.competitors_file):
            return {}
        with open(self.competitors_file, 'r') as f:
            return json.load(f)

    def identify_competitor(self, url):
        parsed = urlparse(url)
        domain = re.sub(r"^www\.", "", parsed.hostname or "").lower()
        
        # Exact match
        for key, info in self.competitors.items():
            if info.get("domain", "").lower() == domain:
                return key, info
        
        # Partial match
        for key, info in self.competitors.items():
            if domain in info.get("domain", "").lower() or info.get("domain", "").lower() in domain:
                return key, info
                
        return None, None

    def standardize_data(self, data):
        """Standardizes internal data to the user requested 'Ref' fields."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return {
            "Ref Product URL": data.get("url", ""),
            "Ref Product ID": str(data.get("product_id", "")),
            "Ref Variant ID": str(data.get("variant_id", "")),
            "Ref Category URL": data.get("category_url", ""),
            "Ref Brand Name": data.get("brand", ""),
            "Ref Product Name": data.get("name", ""),
            "Ref SKU": data.get("sku", ""),
            "Ref MPN": data.get("mpn", data.get("sku", "")),
            "Ref GTIN": data.get("gtin", ""),
            "Ref Price": str(data.get("price", "")),
            "Ref Main Image": data.get("image", ""),
            "Ref Quantity": data.get("quantity", ""),
            "Ref Group Attr 1": data.get("attr1", ""),
            "Ref Group Attr 2": data.get("attr2", ""),
            "Ref Images": data.get("images", []),
            "Ref Dimensions": data.get("dimensions", ""),
            "Ref Status": data.get("status", "In Stock"),
            "Ref Highlights": data.get("highlights", ""),
            "Date Scrapped": now
        }

    def scrape_url(self, url):
        key, info = self.identify_competitor(url)
        
        raw_data = None
        if not key:
            if self.is_shopify(url):
                raw_data = self.scrape_shopify(url)
            else:
                raw_data = self.scrape_generic(url)
        else:
            scraper_path = info.get("scraper", "")
            if "shopify" in scraper_path.lower() or "shopify" in info.get("notes", "").lower():
                raw_data = self.scrape_shopify(url)
            elif key == "bisonoffice":
                raw_data = self.scrape_bison(url)
            elif key == "coleman-furniture":
                raw_data = self.scrape_coleman(url)
            elif key == "emma-mason":
                raw_data = self.scrape_emma_mason(url)
            elif key == "cymax" or key == "homesquare":
                raw_data = self.scrape_cymax(url)
            else:
                # Generic fallback for identified competitors without specialized handlers
                raw_data = self.scrape_generic(url, platform=f"custom ({key})")
        
        if raw_data and "error" not in raw_data:
            return self.standardize_data(raw_data)
        return raw_data

    def is_shopify(self, url):
        try:
            r = self.session.get(url, timeout=10)
            return "shopify" in r.text.lower() or "/cdn.shopify.com" in r.text
        except:
            return False

    def scrape_shopify(self, url):
        json_url = url.split("?")[0].rstrip("/") + ".js"
        try:
            r = self.session.get(json_url, timeout=10)
            if not r.ok:
                return {"error": f"Failed to fetch Shopify data from {json_url}"}
            
            data = r.json()
            variants = data.get("variants", [])
            primary_variant = variants[0] if variants else {}
            
            return {
                "platform": "shopify",
                "name": data.get("title"),
                "brand": data.get("vendor"),
                "product_id": data.get("id"),
                "variant_id": primary_variant.get("id"),
                "sku": primary_variant.get("sku"),
                "price": primary_variant.get("price", 0) / 100.0,
                "category": data.get("type"),
                "image": "https:" + data.get("featured_image") if data.get("featured_image") and data.get("featured_image").startswith("//") else data.get("featured_image"),
                "status": "In Stock" if primary_variant.get("available") else "Out of Stock",
                "url": url,
                "images": ["https:" + img if img.startswith("//") else img for img in data.get("images", [])]
            }
        except Exception as e:
            return {"error": f"Shopify scraping failed: {str(e)}"}

    def scrape_coleman(self, url):
        try:
            r = self.session.get(url, timeout=15)
            if not r.ok:
                return {"error": f"Coleman failed with status {r.status_code}"}
                
            soup = BeautifulSoup(r.text, 'html.parser')
            
            # Product Name
            name_elem = soup.find('h1') or soup.find('span', class_='base', itemprop='name')
            name = name_elem.get_text(strip=True) if name_elem else ""
            
            # SKU
            sku = ""
            sku_elem = soup.find('div', class_='product-sku') or soup.find('div', {'itemprop': 'sku'})
            if sku_elem:
                sku = sku_elem.get_text(strip=True).replace('SKU:', '').strip()
            else:
                sku_text = soup.find(string=re.compile(r'SKU:\s*(\S+)'))
                if sku_text:
                    match = re.search(r'SKU:\s*(\S+)', sku_text)
                    sku = match.group(1) if match else ""
                else:
                    all_text = soup.get_text()
                    match = re.search(r'SKU:\s*([A-Z0-9-]+)', all_text)
                    sku = match.group(1) if match else ""
            
            # Price
            price = ""
            price_elem = soup.find('span', class_='price') or soup.find('meta', {'itemprop': 'price'}) or soup.find('span', {'id': 'product-price'})
            if price_elem:
                if price_elem.name == 'meta':
                    price = price_elem.get('content')
                else:
                    price_text = price_elem.get_text(strip=True)
                    match = re.search(r'([\d,]+\.?\d*)', price_text)
                    price = match.group(1).replace(',', '') if match else ""
            
            if not price:
                all_text = soup.get_text()
                match = re.search(r'\$([\d,]+\.?\d*)', all_text)
                price = match.group(1).replace(',', '') if match else ""
            
            # Brand
            brand = ""
            brand_elem = soup.select_one('.by-manufacturer a') or soup.find('td', {'data-th': 'Manufacturer'})
            if brand_elem:
                brand = brand_elem.get_text(strip=True)
            else:
                spec_rows = soup.find_all('div', class_='product-info-table')
                for row in spec_rows:
                    if 'Manufacturer' in row.get_text():
                        val = row.find('div', class_='spec-value')
                        if val:
                            brand = val.get_text(strip=True)
                            break
            
            # Dimensions
            dimensions_list = []
            dim_rows = soup.select('.product-dimensions .product-info-table')
            for row in dim_rows:
                title = row.find('div', class_='spec-title')
                value = row.find('div', class_='spec-value')
                if title and value:
                    dimensions_list.append(f"{title.get_text(strip=True)}: {value.get_text(strip=True)}")
            dimensions = " | ".join(dimensions_list) if dimensions_list else ""
            
            # Highlights
            highlights = []
            # Visual Highlights
            vh_items = soup.select('.product-hightlights-items-item')
            for item in vh_items:
                title = item.find(class_='product-hightlights-items-item-title')
                desc = item.find(class_='product-hightlights-items-item-desc')
                if title and desc:
                    highlights.append(f"{title.get_text(strip=True)}: {desc.get_text(strip=True)}")
            
            # Textual Features
            feat_items = soup.select('ul.features li')
            for item in feat_items:
                highlights.append(item.get_text(strip=True))
            
            # Multiple Images
            images = []
            # Try to find images in the scripts (often Coleman uses a gallery script)
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and 'data-gallery-role=gallery-placeholder' in script.string:
                    # Find image URLs in JSON-like structure
                    urls = re.findall(r'"img":\s*"(https://.*?)"', script.string)
                    images.extend(urls)
                    urls_full = re.findall(r'"full":\s*"(https://.*?)"', script.string)
                    images.extend(urls_full)
            
            if not images:
                img_container = soup.find('div', class_='product-image-container') or soup.find('div', class_='gallery-placeholder')
                if img_container:
                    img_tags = img_container.find_all('img')
                    for img in img_tags:
                        src = img.get('src') or img.get('data-src') or img.get('data-lazy')
                        if src and src not in images and not src.endswith('.gif'):
                            if not src.startswith('http'):
                                src = "https:" + src if src.startswith('//') else src
                            images.append(src)
            
            # Remove duplicates and clean
            images = list(dict.fromkeys([img.replace('\\/', '/') for img in images]))
            
            # Main Image
            image = images[0] if images else ""
            if not image:
                meta_img = soup.find('meta', property='og:image')
                image = meta_img.get('content') if meta_img else ""
                
            return {
                "platform": "coleman-furniture",
                "name": name,
                "sku": sku,
                "price": price,
                "brand": brand,
                "dimensions": dimensions,
                "highlights": " | ".join(highlights),
                "images": images,
                "image": image,
                "url": url,
                "status": "In Stock" if price else "Out of Stock"
            }
        except Exception as e:
            return {"error": f"Coleman scraping failed: {str(e)}"}

    def scrape_emma_mason(self, url):
        try:
            r = self.session.get(url, timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')
            data = {"url": url, "platform": "emma-mason"}
            
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and 'dataLayer' in script.string:
                    match = re.search(r'dataLayer\.push\((\{.*?\})\)', script.string, re.DOTALL)
                    if match:
                        try:
                            dl = json.loads(match.group(1))
                            data['name'] = dl.get('product_name') or dl.get('name')
                            data['price'] = dl.get('product_price') or dl.get('price')
                            data['sku'] = dl.get('product_sku') or dl.get('sku')
                        except: pass
            
            if not data.get('name'):
                name_elem = soup.find('h1') or soup.find('span', itemprop='name')
                data['name'] = name_elem.get_text(strip=True) if name_elem else ""
            
            return data
        except Exception as e:
            return {"error": f"Emma Mason failed: {str(e)}"}

    def scrape_cymax(self, url):
        # Cymax has strong Cloudflare protection. 
        # Attempt standard fetch first, then fallback to Playwright
        try:
            r = self.session.get(url, timeout=10)
            if r.status_code == 403:
                return self.scrape_with_playwright(url, platform="cymax")
            
            if not r.ok:
                return {"error": f"Cymax failed with status {r.status_code}"}
                
            soup = BeautifulSoup(r.text, 'html.parser')
            return self._parse_cymax_html(soup, url)
        except Exception as e:
            return self.scrape_with_playwright(url, platform="cymax")

    def _parse_cymax_html(self, soup, url):
        data = {"url": url}
        # Brand
        brand_elem = soup.select_one('meta[itemprop="brand"]') or soup.select_one('.by-manufacturer a')
        if brand_elem:
            data['brand'] = brand_elem.get('content') if brand_elem.name == 'meta' else brand_elem.get_text(strip=True)
        
        # SKU
        sku_elem = soup.select_one('meta[itemprop="sku"]') or soup.select_one('.product-id-label')
        if sku_elem:
            data['sku'] = sku_elem.get('content') if sku_elem.name == 'meta' else sku_elem.get_text(strip=True)
        
        # Price
        price_elem = soup.select_one('meta[itemprop="price"]') or soup.select_one('#product-main-price')
        if price_elem:
            data['price'] = price_elem.get('content') if price_elem.name == 'meta' else price_elem.get_text(strip=True).replace('$', '').replace(',', '')

        # Fallback to generic JSON-LD if needed
        if not data.get('sku') or not data.get('price'):
            ld_data = self.scrape_generic_soup(soup, url)
            if ld_data: data.update(ld_data)
                
        return data

    def scrape_with_playwright(self, url, platform="generic"):
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page(user_agent=self.session.headers["User-Agent"])
                # Set a longer timeout for Cloudflare challenges
                page.goto(url, wait_until="networkidle", timeout=60000)
                
                # Wait for potential challenge to solve
                page.wait_for_timeout(2000) 
                
                content = page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                if "cymax" in platform:
                    data = self._parse_cymax_html(soup, url)
                else:
                    data = self.scrape_generic_soup(soup, url)
                
                browser.close()
                data["platform"] = f"{platform} (playwright-bypass)"
                return data
        except Exception as e:
            return {"error": f"Playwright bypass failed: {str(e)}"}

    def scrape_generic_soup(self, soup, url):
        data = {"url": url}
        json_ld_tags = soup.find_all('script', type='application/ld+json')
        for tag in json_ld_tags:
            try:
                ld = json.loads(tag.string)
                if isinstance(ld, list): ld = ld[0]
                if ld.get('@type') == 'Product' or (isinstance(ld.get('@type'), list) and 'Product' in ld.get('@type')):
                    data['name'] = data.get('name') or ld.get('name')
                    data['sku'] = data.get('sku') or ld.get('sku')
                    data['image'] = data.get('image') or ld.get('image')
                    offers = ld.get('offers')
                    if isinstance(offers, list): offers = offers[0]
                    if offers:
                        data['price'] = data.get('price') or offers.get('price')
            except: continue
        return data

    def scrape_bison(self, url):
        try:
            if "bo=0" not in url:
                url += ("&" if "?" in url else "?") + "bo=0"
            r = self.session.get(url, timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')
            name_elem = soup.find('div', {'itemprop': 'name'})
            price_elem = soup.find('div', {'itemprop': 'price'})
            brand_elem = soup.find('div', {'itemprop': 'brand'})
            
            return {
                "platform": "bisonoffice",
                "name": name_elem.get_text(strip=True) if name_elem else "",
                "price": price_elem.get("content") if price_elem else "",
                "brand": brand_elem.get_text(strip=True) if brand_elem else "",
                "url": url
            }
        except Exception as e:
            return {"error": f"Bison failed: {str(e)}"}

    def scrape_generic(self, url, platform="generic"):
        try:
            r = self.session.get(url, timeout=15)
            if not r.ok:
                return {"error": f"Generic failed with status {r.status_code}"}
                
            soup = BeautifulSoup(r.text, 'html.parser')
            data = {"url": url, "platform": platform}
            
            # Try JSON-LD
            json_ld_tags = soup.find_all('script', type='application/ld+json')
            for tag in json_ld_tags:
                try:
                    ld = json.loads(tag.string)
                    if isinstance(ld, list): ld = ld[0]
                    if ld.get('@type') == 'Product' or (isinstance(ld.get('@type'), list) and 'Product' in ld.get('@type')):
                        data['name'] = data.get('name') or ld.get('name')
                        data['sku'] = data.get('sku') or ld.get('sku')
                        data['image'] = data.get('image') or ld.get('image')
                        if isinstance(data['image'], list): data['image'] = data['image'][0]
                        offers = ld.get('offers')
                        if isinstance(offers, list): offers = offers[0]
                        if offers:
                            data['price'] = data.get('price') or offers.get('price')
                            data['currency'] = data.get('currency') or offers.get('priceCurrency')
                except: continue
            
            # Fallback to Meta Tags
            if not data.get('name'):
                meta_name = soup.find('meta', property='og:title') or soup.find('title')
                data['name'] = meta_name.get('content') if meta_name and meta_name.name == 'meta' else (meta_name.get_text(strip=True) if meta_name else "")
            
            return data
        except Exception as e:
            return {"error": f"Generic failed: {str(e)}"}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python src/unified_scraper.py <url>")
        sys.exit(1)
    url = sys.argv[1]
    scraper = UnifiedScraper()
    result = scraper.scrape_url(url)
    print(json.dumps(result, indent=2))
