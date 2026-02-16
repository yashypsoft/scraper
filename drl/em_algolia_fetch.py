import argparse
import csv
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests


ENDPOINT = (
    "https://ctz7lv7pje-2.algolianet.com/1/indexes/*/queries"
    "?x-algolia-agent=Algolia%20for%20JavaScript%20(4.24.0)%3B%20Browser%3B"
    "%20instantsearch.js%20(4.77.0)%3B%20Magento2%20integration%20(3.15.1)%3B"
    "%20JS%20Helper%20(3.23.0)"
)

INDEX_NAME = "magento2_emmamason_products"

PARAMS_TEMPLATE = (
    "facets=%5B%22brand%22%2C%22categories.level0%22%2C%22collection_style%22%2C"
    "%22color_finish%22%2C%22height%22%2C%22material%22%2C%22price.USD.default%22%2C"
    "%22style%22%2C%22type_of_product%22%2C%22width%22%5D&highlightPostTag=__%2Fais-"
    "highlight__&highlightPreTag=__ais-highlight__&hitsPerPage={hits_per_page}&"
    "maxValuesPerFacet=10&numericFilters=%5B%22visibility_search%3D1%22%5D&page={page}"
    "&query=%2A&ruleContexts=%5B%22magento_filters%22%5D"
)

HEADERS = {
    "accept": "*/*",
    "content-type": "application/x-www-form-urlencoded",
    "origin": "https://emmamason.com",
    "referer": "https://emmamason.com/",
    "x-algolia-api-key": (
        "YjQ3NzQ4MWY2NDMzZDljNzhkNDliYTYyYTAyZWJjMTg2NzhhNjU5ZjZhNmVhNTc5YmNiMzFmY"
        "jk2NjY1NjQzMXRhZ0ZpbHRlcnM9JnZhbGlkVW50aWw9MTc3MTMwNDg4Mg=="
    ),
    "x-algolia-application-id": "CTZ7LV7PJE",
    "user-agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
    ),
}

CSV_HEADERS = [
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


def normalize_multi_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ";".join(str(v).strip() for v in value if str(v).strip())
    text = str(value).strip()
    if not text:
        return ""
    if "," in text:
        return ";".join(part.strip() for part in text.split(",") if part.strip())
    return text


def normalize_category(value: Any) -> str:
    if isinstance(value, dict):
        ordered_keys = sorted(value.keys())
        parts: List[str] = []
        for key in ordered_keys:
            nested = value.get(key)
            if isinstance(nested, list):
                parts.extend(str(v).strip() for v in nested if str(v).strip())
            elif nested:
                parts.append(str(nested).strip())
        return " | ".join(dict.fromkeys(parts))
    if isinstance(value, list):
        return " | ".join(str(v).strip() for v in value if str(v).strip())
    return str(value or "").strip()


def extract_price(hit: Dict[str, Any]) -> str:
    price = hit.get("price", {})
    if isinstance(price, dict):
        usd = price.get("USD", {})
        if isinstance(usd, dict):
            for key in ("default_formated", "default"):
                if key in usd and usd.get(key) not in (None, ""):
                    return str(usd[key])
    if price not in (None, ""):
        return str(price)
    return ""


def fetch_page_once(page: int, hits_per_page: int, timeout: int) -> Dict[str, Any]:
    payload = {
        "requests": [
            {
                "indexName": INDEX_NAME,
                "params": PARAMS_TEMPLATE.format(page=page, hits_per_page=hits_per_page),
            }
        ]
    }
    response = requests.post(ENDPOINT, headers=HEADERS, json=payload, timeout=timeout)
    response.raise_for_status()
    body = response.json()
    results = body.get("results", [])
    if not results:
        raise ValueError(f"No results returned for page {page}")
    return results[0]


def fetch_page_with_retries(
    page: int,
    hits_per_page: int,
    timeout: int,
    retries: int,
    delay: float,
) -> Dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            result = fetch_page_once(page=page, hits_per_page=hits_per_page, timeout=timeout)
            print(f"Page {page} fetched: {len(result.get('hits', []))} hits")
            if delay > 0:
                time.sleep(delay)
            return result
        except Exception as exc:
            last_error = exc
            print(f"Page {page} failed on attempt {attempt}/{retries}: {exc}")
            if attempt < retries:
                time.sleep(1.5 * attempt)
    raise RuntimeError(f"Failed page {page} after {retries} attempts") from last_error


def hit_to_row(hit: Dict[str, Any], scraped_date: str) -> List[str]:
    url = str(hit.get("url", "")).strip()
    product_id = str(hit.get("objectID") or hit.get("id") or "").strip()
    sku = normalize_multi_value(hit.get("sku"))
    mpn = str(hit.get("item_number") or "").strip()
    if not mpn:
        mpn = sku.split(";")[0] if sku else ""
    category = normalize_category(hit.get("categories") or hit.get("categories_without_path"))
    quantity = "1" if str(hit.get("in_stock", "")).strip() in ("1", "true", "True") else "0"
    status = "SELLABLE" if quantity == "1" else "OUT_OF_STOCK"
    group_attr_1 = str(hit.get("type_of_product") or "").strip()
    group_attr_2 = str(hit.get("material") or hit.get("color_finish") or "").strip()

    return [
        url,
        product_id,
        "",
        category,
        "",
        str(hit.get("brand") or "").strip(),
        str(hit.get("name") or "").strip(),
        sku,
        mpn,
        "",
        extract_price(hit),
        str(hit.get("image_url") or hit.get("thumbnail_url") or "").strip(),
        quantity,
        group_attr_1,
        group_attr_2,
        status,
        json.dumps(hit, ensure_ascii=False),
        scraped_date,
    ]


def run(
    output_csv: str,
    output_json: str,
    page: int,
    hits_per_page: int,
    max_workers: int,
    delay: float,
    timeout: int,
    retries: int,
) -> None:
    scraped_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    requested_page = page

    if page < 0:
        raise ValueError("--page must be 0 or greater")
    if max_workers < 1:
        raise ValueError("--max-workers must be at least 1")

    first = fetch_page_with_retries(
        page=0 if page == 0 else page,
        hits_per_page=hits_per_page,
        timeout=timeout,
        retries=retries,
        delay=delay,
    )
    nb_pages = int(first.get("nbPages", 1))
    nb_hits = int(first.get("nbHits", len(first.get("hits", []))))

    if requested_page == 0:
        page_results: Dict[int, Dict[str, Any]] = {0: first}
        remaining_pages = list(range(1, nb_pages))
        if remaining_pages:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_page = {
                    executor.submit(
                        fetch_page_with_retries,
                        p,
                        hits_per_page,
                        timeout,
                        retries,
                        delay,
                    ): p
                    for p in remaining_pages
                }
                for future in as_completed(future_to_page):
                    p = future_to_page[future]
                    page_results[p] = future.result()
        selected_pages = sorted(page_results.keys())
        all_hits: List[Dict[str, Any]] = []
        for p in selected_pages:
            all_hits.extend(page_results[p].get("hits", []))
    else:
        if requested_page >= nb_pages:
            raise ValueError(
                f"Requested page {requested_page} is out of range. Valid pages are 0 to {max(nb_pages - 1, 0)}."
            )
        all_hits = first.get("hits", [])
        selected_pages = [requested_page]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        rows = list(executor.map(lambda h: hit_to_row(h, scraped_date), all_hits))

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)
        writer.writerows(rows)

    if output_json:
        payload = {
            "source": "emmamason_algolia",
            "endpoint": ENDPOINT,
            "indexName": INDEX_NAME,
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "requested_page": requested_page,
            "fetched_pages": selected_pages,
            "nbPages": nb_pages,
            "nbHits": nb_hits,
            "records_fetched": len(all_hits),
            "hits": all_hits,
        }
        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(rows)} rows to {output_csv}")
    if output_json:
        print(f"Saved JSON payload to {output_json}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Emma Mason Algolia hits and export CSV with em_scraper columns."
    )
    parser.add_argument(
        "--page",
        type=int,
        default=0,
        help="Page number to fetch. 0 means fetch all pages.",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="CSV output path. Default: products_chunk_<page>.csv",
    )
    parser.add_argument(
        "--output-json",
        default="drl/em_algolia_records.json",
        help="Optional JSON output path (empty string disables JSON output).",
    )
    parser.add_argument(
        "--hits-per-page",
        type=int,
        default=24,
        help="Algolia hitsPerPage value (default: 24).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Threadpool worker count (default: 8).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Delay in seconds per request (default: 0.1).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout seconds (default: 30).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Retry attempts per page (default: 3).",
    )
    args = parser.parse_args()

    output_csv = args.output_csv or f"products_chunk_{args.page}.csv"

    run(
        output_csv=output_csv,
        output_json=args.output_json,
        page=args.page,
        hits_per_page=args.hits_per_page,
        max_workers=args.max_workers,
        delay=args.delay,
        timeout=args.timeout,
        retries=args.retries,
    )


if __name__ == "__main__":
    main()
