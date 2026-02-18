import argparse
import os
import random
import shutil
import sys
import tempfile
import time
import traceback
from datetime import datetime

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

import gscrapperci as base  # noqa: E402


def _safe_quit(driver):
    if driver is None:
        return
    try:
        driver.quit()
    except Exception:
        pass


def _safe_rmtree(path: str | None):
    if not path:
        return
    try:
        shutil.rmtree(path, ignore_errors=True)
    except Exception:
        pass


def _make_profile_dir(prefix: str) -> str:
    runner_temp = os.getenv("RUNNER_TEMP")
    base_dir = runner_temp if runner_temp and os.path.isdir(runner_temp) else None
    return tempfile.mkdtemp(prefix=prefix, dir=base_dir)


def _new_driver(*, headless: bool, version_main: int | None):
    profile_dir = _make_profile_dir(prefix="gs_chrome_profile_")
    debug_port = random.randint(10000, 60000)

    driver = base.setup_driver(
        profile_dir=profile_dir,
        headless=headless,
        debug_port=debug_port,
        version_main=version_main,
    )
    return driver, profile_dir


def _should_retry_result(result: dict, retry_statuses: set[str]) -> bool:
    status = (result or {}).get("status") or ""
    return status in retry_statuses


def _is_success_result(result: dict) -> bool:
    return (result or {}).get("status") == "completed"


def scrape_product_with_browser_retries(
    *,
    driver,
    profile_dir: str | None,
    product_id,
    keyword,
    url,
    max_browser_attempts: int,
    headless: bool,
    version_main: int | None,
    retry_statuses: set[str],
):
    last_result = None

    for attempt in range(1, max_browser_attempts + 1):
        if driver is None:
            try:
                driver, profile_dir = _new_driver(headless=headless, version_main=version_main)
            except Exception as e:
                last_result = {
                    "product_id": product_id,
                    "keyword": keyword,
                    "url": url,
                    "last_response": f"Driver creation failed (attempt {attempt}/{max_browser_attempts}): {e}",
                    "status": "driver_create_failed",
                    "last_fetched_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "competitors": [],
                }
                time.sleep(random.uniform(2, 4))
                continue

        try:
            result = base.scrape_product(driver, product_id, keyword, url)
        except Exception as e:
            traceback.print_exc()
            result = {
                "product_id": product_id,
                "keyword": keyword,
                "url": url,
                "last_response": f"Unhandled exception during scrape: {e}",
                "status": "error",
                "last_fetched_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "competitors": [],
            }

        last_result = result

        if _is_success_result(result):
            return result, driver, profile_dir

        if _should_retry_result(result, retry_statuses):
            print(
                f"[retry] Product {product_id}: status='{result.get('status')}'. "
                f"Restarting Chrome (attempt {attempt}/{max_browser_attempts})..."
            )
            _safe_quit(driver)
            _safe_rmtree(profile_dir)
            driver = None
            profile_dir = None
            time.sleep(random.uniform(2, 4))
            continue

        return result, driver, profile_dir

    return last_result, driver, profile_dir


def process_chunk_with_browser_retries(
    chunk_file: str,
    chunk_id: int,
    *,
    max_browser_attempts: int,
    headless: bool,
    version_main: int | None,
):
    retry_statuses = {
        "captcha_failed",
        "container_not_found",
        "no_products",
        "no_offers_found",
        "error",
        "driver_create_failed",
    }

    df = pd.read_csv(chunk_file)
    print(f"Processing {len(df)} products from chunk {chunk_id}")

    product_results: list[dict] = []
    seller_results: list[dict] = []

    driver = None
    profile_dir = None
    created_profiles: list[str] = []

    try:
        for index, row in df.iterrows():
            product_id = row["product_id"]
            web_id = row["web_id"]
            keyword = row["keyword"]
            url = row["url"]
            osb_url = row["osb_url"]

            print(f"\nProcessing {index+1}/{len(df)}: Product ID {product_id}")

            result, driver, profile_dir = scrape_product_with_browser_retries(
                driver=driver,
                profile_dir=profile_dir,
                product_id=product_id,
                keyword=keyword,
                url=url,
                max_browser_attempts=max_browser_attempts,
                headless=headless,
                version_main=version_main,
                retry_statuses=retry_statuses,
            )

            if profile_dir and (not created_profiles or created_profiles[-1] != profile_dir):
                created_profiles.append(profile_dir)

            result["web_id"] = web_id
            result["osb_url"] = osb_url

            product_results.append(result)
            seller_results.extend(result.get("competitors") or [])

            if index < len(df) - 1:
                time.sleep(random.uniform(3, 6))

    finally:
        _safe_quit(driver)
        for p in created_profiles:
            _safe_rmtree(p)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    csv1_filename = f"product_info_chunk{chunk_id}_{timestamp}.csv"
    csv2_filename = f"seller_info_chunk{chunk_id}_{timestamp}.csv"
    csv1_path = os.path.join(output_dir, csv1_filename)
    csv2_path = os.path.join(output_dir, csv2_filename)

    csv1_data = []
    for result in product_results:
        csv1_data.append(
            {
                "product_id": result.get("product_id", ""),
                "web_id": result.get("web_id", ""),
                "keyword": result.get("keyword", ""),
                "url": result.get("url", ""),
                "osb_url": result.get("osb_url", ""),
                "last_response": result.get("last_response", ""),
                "product_url": result.get("product_url", ""),
                "seller": result.get("seller", ""),
                "product_name": result.get("product_name", ""),
                "cid": result.get("cid", ""),
                "pid": result.get("pid", ""),
                "last_fetched_date": result.get("last_fetched_date", ""),
                "osb_position": result.get("osb_position", 0),
                "osb_id": result.get("osb_id", ""),
                "seller_count": result.get("seller_count", 0),
                "status": result.get("status", "error"),
            }
        )

    csv2_data = []
    for seller in seller_results:
        csv2_data.append(
            {
                "product_id": seller.get("product_id", ""),
                "seller": seller.get("seller", ""),
                "seller_product_name": seller.get("seller_product_name", ""),
                "seller_url": seller.get("seller_url", ""),
                "seller_price": seller.get("seller_price", ""),
                "last_fetched_date": seller.get("last_fetched_date", ""),
            }
        )

    if csv1_data:
        pd.DataFrame(csv1_data).to_csv(csv1_path, index=False)
        print(f"✓ Saved product info: {csv1_filename}")

    if csv2_data:
        pd.DataFrame(csv2_data).to_csv(csv2_path, index=False)
        print(f"✓ Saved seller info: {csv2_filename}")

    print(f"\n✓ Chunk {chunk_id} processing completed")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Google Shopping Scraper (multi-instance Chrome retries)"
    )
    parser.add_argument("--chunk-id", type=int, required=True, help="Chunk ID (1-based)")
    parser.add_argument(
        "--total-chunks", type=int, required=True, help="Total number of chunks"
    )
    parser.add_argument(
        "--input-file", type=str, required=True, help="Input CSV filename on FTP"
    )
    parser.add_argument(
        "--browser-retries",
        type=int,
        default=3,
        help="Max Chrome restarts per product (default: 3)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Chrome in headless mode (recommended in CI)",
    )
    parser.add_argument(
        "--uc-version-main",
        type=int,
        default=None,
        help="Pin Chrome major version for undetected-chromedriver (default: auto)",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Google Shopping Scraper (multi-instance Chrome retries)")
    print(f"Chunk: {args.chunk_id} of {args.total_chunks}")
    print(f"Input file: {args.input_file}")
    print(f"Browser retries: {args.browser_retries}")
    print(f"Headless: {args.headless}")
    print(f"uc version_main: {args.uc_version_main}")
    print("=" * 60)

    ftp_host = os.getenv("FTP_HOST")
    ftp_user = os.getenv("FTP_USER")
    ftp_pass = os.getenv("FTP_PASS")
    ftp_path = os.getenv("FTP_PATH", "/scrap/")

    if not all([ftp_host, ftp_user, ftp_pass]):
        print("Error: FTP credentials not found in environment variables")
        print("Please set FTP_HOST, FTP_USER, FTP_PASS environment variables")
        sys.exit(1)

    input_csv = "input.csv"
    if not base.download_csv_from_ftp(
        ftp_host, ftp_user, ftp_pass, ftp_path, args.input_file, input_csv
    ):
        print("Failed to download input CSV")
        sys.exit(1)

    chunk_file = base.split_csv(input_csv, "chunks", args.chunk_id, args.total_chunks)
    if not chunk_file:
        print("Failed to split CSV")
        sys.exit(1)

    success = process_chunk_with_browser_retries(
        chunk_file,
        args.chunk_id,
        max_browser_attempts=max(1, args.browser_retries),
        headless=args.headless,
        version_main=args.uc_version_main,
    )

    try:
        os.remove(input_csv)
        os.remove(chunk_file)
        shutil.rmtree("chunks", ignore_errors=True)
    except Exception:
        pass

    if success:
        print("\n✓ Processing completed successfully")
        sys.exit(0)
    print("\n✗ Processing failed")
    sys.exit(1)


if __name__ == "__main__":
    main()

