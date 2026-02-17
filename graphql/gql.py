import os
import csv
import time
import sys
import gc
import threading
import requests
import re
import json
from typing import Optional
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "https://www.homedepot.com").rstrip("/")
SITEMAP_INDEX = os.getenv("SITEMAP_INDEX", "").strip()
GRAPHQL_URL = os.getenv("GRAPHQL_URL", "").strip()
STORE_ID = os.getenv("STORE_ID", "1710")
ZIP_CODE = os.getenv("ZIP_CODE", "96913")

SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.0"))

OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ================= LOGGER =================

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()


# ================= HTTP =================

session = requests.Session()
session.headers.update(
    {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
        "Origin": CURR_URL,
        "Referer": CURR_URL + "/",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    }
)


def get_sitemap_from_robots_txt() -> Optional[str]:
    try:
        robots_url = f"{CURR_URL}/robots.txt"
        response = requests.get(robots_url, timeout=20)
        response.raise_for_status()
        for line in response.text.split("\n"):
            if line.lower().startswith("sitemap:"):
                sitemap_url = line.split(":", 1)[1].strip()
                if sitemap_url:
                    log(f"Extracted sitemap index from robots.txt: {sitemap_url}")
                    return sitemap_url
    except Exception as e:
        log(f"Error fetching robots.txt: {e}", "WARNING")
    return None


def http_get(url: str) -> Optional[str]:
    for attempt in range(3):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 200:
                return r.text
            log(f"Status {r.status_code} for {url}", "WARNING")
        except Exception as e:
            log(f"GET attempt {attempt + 1} failed for {url}: {e}", "WARNING")
        time.sleep(1 + attempt)
    return None


def load_xml(url: str) -> Optional[ET.Element]:
    data = http_get(url)
    if not data:
        return None
    try:
        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML parsing failed for {url}: {e}", "ERROR")
        return None


# ================= GRAPHQL =================

GRAPHQL_QUERY = """
query productClientOnlyProduct(
  $itemId: String!,
  $storeId: String,
  $zipCode: String,
  $skipSpecificationGroup: Boolean = false,
  $isBrandPricingPolicyCompliant: Boolean = false
) {
  product(itemId: $itemId) {
    itemId
    identifiers {
      canonicalUrl
      brandName
      itemId
      modelNumber
      productLabel
      storeSkuNumber
      upcGtin13
      upc
    }
    specificationGroup @skip(if: $skipSpecificationGroup) {
      specTitle
      specifications {
        specName
        specValue
      }
    }
    availabilityType {
      status
      type
      buyable
      discontinued
    }
    details {
      highlights
      description
    }
    media {
      images {
        url
      }
    }
    pricing(storeId: $storeId, isBrandPricingPolicyCompliant: $isBrandPricingPolicyCompliant) {
      value
      original
      unitOfMeasure
    }
    taxonomy {
      breadCrumbs {
        label
        url
      }
    }
  }
}
"""


def extract_item_id_from_url(product_url: str) -> Optional[str]:
    # Home Depot PDP pattern usually ends with /<numeric_item_id>
    m = re.search(r"/(\d+)(?:[/?#]|$)", product_url)
    return m.group(1) if m else None


def fetch_product_graphql(product_url: str, item_id: str) -> Optional[dict]:
    parsed = urlparse(product_url)
    path = parsed.path if parsed.path else "/"

    payload = {
        "operationName": "productClientOnlyProduct",
        "variables": {
            "itemId": item_id,
            "storeId": STORE_ID,
            "zipCode": ZIP_CODE,
            "skipSpecificationGroup": False,
            "isBrandPricingPolicyCompliant": False,
        },
        "query": GRAPHQL_QUERY,
    }

    headers = {
        "x-current-url": path,
        "x-experience-name": "fusion-hdh-pip-desktop",
        "x-debug": "false",
    }

    for attempt in range(4):
        try:
            r = session.post(GRAPHQL_URL, headers=headers, json=payload, timeout=35)
            if r.status_code == 200:
                data = r.json()
                if data.get("errors"):
                    log(f"GraphQL errors for item {item_id}: {data['errors']}", "WARNING")
                return data
            log(f"GraphQL status {r.status_code} for item {item_id}", "WARNING")
        except Exception as e:
            log(f"GraphQL attempt {attempt + 1} failed for item {item_id}: {e}", "WARNING")
        time.sleep(1 + attempt)
    return None


def extract_product_data(gql_data: dict, product_url: str, item_id: str) -> Optional[dict]:
    product = (gql_data or {}).get("data", {}).get("product")
    if not product:
        return None

    identifiers = product.get("identifiers") or {}
    pricing = product.get("pricing") or {}
    taxonomy = product.get("taxonomy") or {}
    breadcrumbs = taxonomy.get("breadCrumbs") or []
    availability = product.get("availabilityType") or {}
    images = (product.get("media") or {}).get("images") or []

    brand = identifiers.get("brandName", "")
    name = identifiers.get("productLabel", "")
    sku = identifiers.get("storeSkuNumber", "")
    mpn = identifiers.get("modelNumber", "")
    gtin = identifiers.get("upcGtin13", "") or identifiers.get("upc", "") or ""
    price = pricing.get("value", "")
    main_image = images[0].get("url", "") if images else ""

    category = ""
    category_url = ""
    if breadcrumbs:
        non_home = [b for b in breadcrumbs if b.get("label", "").strip().lower() != "home"]
        if non_home:
            category = non_home[-1].get("label", "")
            category_url = non_home[-1].get("url", "")

    status_raw = str(availability.get("status", ""))
    status = "SELLABLE" if "active" in status_raw.lower() or availability.get("buyable") else "OUT_OF_STOCK"

    spec_groups = product.get("specificationGroup") or []
    additional_data = {
        "description": (product.get("details") or {}).get("description", ""),
        "highlights": (product.get("details") or {}).get("highlights", []),
        "original_price": pricing.get("original", ""),
        "unit_of_measure": pricing.get("unitOfMeasure", ""),
        "canonical_url": identifiers.get("canonicalUrl", ""),
        "specification_group": spec_groups,
    }

    return {
        "product_id": item_id,
        "variation_id": sku or item_id,
        "category": category,
        "category_url": category_url,
        "brand": brand,
        "name": name,
        "sku": sku,
        "mpn": mpn,
        "gtin": gtin,
        "price": price,
        "main_image": main_image,
        "quantity": 1,
        "group_attr_1": "",
        "group_attr_2": "",
        "status": status,
        "additional_data": json.dumps(additional_data, ensure_ascii=False),
        "product_url": product_url,
    }


# ================= PRODUCT PROCESSING =================

csv_lock = threading.Lock()
seen_lock = threading.Lock()
stats_lock = threading.Lock()


def process_product_data(product_url: str, writer, seen: set, stats: dict):
    item_id = extract_item_id_from_url(product_url)
    if not item_id:
        with stats_lock:
            stats["errors"] += 1
        log(f"Could not extract item ID from URL: {product_url}", "WARNING")
        return

    key = f"{item_id}|{product_url}"
    with seen_lock:
        if key in seen:
            return
        seen.add(key)

    gql_data = fetch_product_graphql(product_url, item_id)
    if not gql_data:
        with stats_lock:
            stats["errors"] += 1
        return

    product_info = extract_product_data(gql_data, product_url, item_id)
    if not product_info:
        with stats_lock:
            stats["errors"] += 1
        log(f"No product data for item {item_id}", "WARNING")
        return

    row = [
        product_info["product_url"],
        product_info["product_id"],
        product_info["variation_id"],
        product_info["category"],
        product_info["category_url"],
        product_info["brand"],
        product_info["name"],
        product_info["sku"],
        product_info["mpn"],
        product_info["gtin"],
        product_info["price"],
        product_info["main_image"],
        product_info["quantity"],
        product_info["group_attr_1"],
        product_info["group_attr_2"],
        product_info["status"],
        product_info["additional_data"],
        SCRAPED_DATE,
    ]

    with csv_lock:
        writer.writerow(row)

    with stats_lock:
        stats["products_fetched"] += 1
        stats["urls_processed"] += 1

    if REQUEST_DELAY > 0:
        time.sleep(REQUEST_DELAY)


# ================= MAIN =================

def main():
    if not GRAPHQL_URL:
        log("GRAPHQL_URL environment variable is required", "ERROR")
        sys.exit(1)

    sitemap = SITEMAP_INDEX or get_sitemap_from_robots_txt() or f"{CURR_URL}/sitemap.xml"

    log("=" * 60)
    log("Home Depot GraphQL Scraper")
    log(f"Timestamp: {SCRAPED_DATE}")
    log(f"Base URL: {CURR_URL}")
    log(f"GraphQL URL: {GRAPHQL_URL}")
    log(f"Sitemap Index: {sitemap}")
    log(f"Sitemap Offset: {SITEMAP_OFFSET}")
    log(f"Max Sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'All'}")
    log(f"Max URLs per Sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'All'}")
    log(f"Max Workers: {MAX_WORKERS}")
    log(f"Request Delay: {REQUEST_DELAY}s")
    log("=" * 60)

    index = load_xml(sitemap)
    if index is None:
        log("Failed to load sitemap index", "ERROR")
        sys.exit(1)

    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemaps = []
    for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
        elements = index.findall(path, ns) if "ns:" in path else index.findall(path)
        if elements:
            sitemaps = [e.text.strip() for e in elements if e.text and "sitemap" in e.text.lower()]
            if sitemaps:
                break

    if not sitemaps:
        log("No sitemaps found in index", "ERROR")
        sys.exit(1)

    if SITEMAP_OFFSET >= len(sitemaps):
        log(f"Offset {SITEMAP_OFFSET} exceeds total sitemaps ({len(sitemaps)})", "WARNING")
        sys.exit(0)

    end_index = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(sitemaps)
    sitemaps_to_process = sitemaps[SITEMAP_OFFSET:end_index]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
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
                "Date Scrapped",
            ]
        )

        seen = set()
        stats = {
            "sitemaps_processed": 0,
            "urls_processed": 0,
            "products_fetched": 0,
            "errors": 0,
        }

        for sitemap_url in sitemaps_to_process:
            stats["sitemaps_processed"] += 1
            log(f"Processing sitemap {stats['sitemaps_processed']}/{len(sitemaps_to_process)}: {sitemap_url}")

            xml = load_xml(sitemap_url)
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
                        and "/p/" in e.text
                        and extract_item_id_from_url(e.text)
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
                futures = [executor.submit(process_product_data, url, writer, seen, stats) for url in urls]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Error in thread execution: {e}", "ERROR")
                        with stats_lock:
                            stats["errors"] += 1

            gc.collect()

    log("=" * 60)
    log("SCRAPING STATISTICS")
    log("=" * 60)
    log(f"Sitemaps processed: {stats['sitemaps_processed']}")
    log(f"URLs processed: {stats['urls_processed']}")
    log(f"Products successfully fetched: {stats['products_fetched']}")
    log(f"Errors encountered: {stats['errors']}")
    if stats["urls_processed"] > 0:
        success_rate = (stats["products_fetched"] / stats["urls_processed"]) * 100
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
