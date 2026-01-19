import os
import csv
import time
import random
import socket
from ftplib import FTP, FTP_TLS, error_perm
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# --------------------------------------------------
# ENV
# --------------------------------------------------
load_dotenv()

LOCAL_INPUT = "input_urls.csv"
LOCAL_OUTPUT = "furniture_products.csv"
global_product_id = 1

# --------------------------------------------------
# FTP (PLAIN FIRST, FTPS FALLBACK)
# --------------------------------------------------
def get_ftp():
    host = os.getenv("FTP_HOST")
    port = int(os.getenv("FTP_PORT", 21))
    user = os.getenv("FTP_USER")
    password = os.getenv("FTP_PASSWORD")

    if not all([host, user, password]):
        raise RuntimeError("❌ Missing FTP credentials")

    socket.setdefaulttimeout(30)

    try:
        ftp = FTP()
        ftp.connect(host, port)
        ftp.set_pasv(True)
        ftp.login(user, password)
        print("✅ Connected using plain FTP")
        return ftp
    except error_perm:
        ftp = FTP_TLS()
        ftp.connect(host, port)
        ftp.login(user, password)
        ftp.prot_p()
        ftp.set_pasv(True)
        print("✅ Connected using FTPS")
        return ftp


def download_input_from_ftp():
    ftp = get_ftp()
    with open(LOCAL_INPUT, "wb") as f:
        ftp.retrbinary(f"RETR {os.getenv('FTP_INPUT_PATH')}", f.write)
    ftp.quit()
    print("✅ Input CSV downloaded")


def upload_output_to_ftp():
    ftp = get_ftp()
    with open(LOCAL_OUTPUT, "rb") as f:
        ftp.storbinary(f"STOR {os.getenv('FTP_OUTPUT_PATH')}", f)
    ftp.quit()
    print("✅ Output CSV uploaded")

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def text(el):
    return el.get_text(strip=True) if el else None

def attr(el, name):
    return el.get(name) if el else None

# --------------------------------------------------
# PLAYWRIGHT SCRAPER
# --------------------------------------------------
def scrape_product(page, url):
    global global_product_id

    try:
        page.goto(url, timeout=60000, wait_until="domcontentloaded")
        page.wait_for_selector("div.product-name h1", timeout=15000)

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        name = text(
            soup.select_one("div.product-name h1[itemprop='name']")
        )
        sku = attr(
            soup.select_one("meta[itemprop='sku']"),
            "content"
        )
        brand = text(
            soup.select_one("p.manufacturer a:nth-of-type(2)")
        )
        collection = text(
            soup.select_one("p.manufacturer a:nth-of-type(1)")
        )
        image = attr(
            soup.select_one("meta[itemprop='image']"),
            "content"
        )
        price = text(
            soup.select_one(".price-box .price")
        )
        if price:
            price = price.replace("$", "")

        row = {
            "product_id": global_product_id,
            "main_product_id": global_product_id,
            "product_type": "simple",
            "url": url,
            "name": name,
            "sku": sku,
            "brand": brand,
            "collection": collection,
            "image": image,
            "price": price,
        }

        global_product_id += 1
        return [row]

    except Exception as e:
        print(f"⛔ Scrape failed: {url} → {e}")
        return []

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    download_input_from_ftp()

    with open(LOCAL_INPUT) as f:
        urls = [row[0] for row in csv.reader(f) if row]

    columns = [
        "product_id",
        "main_product_id",
        "product_type",
        "url",
        "name",
        "sku",
        "brand",
        "collection",
        "image",
        "price",
    ]

    with open(LOCAL_OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )

            context = browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )

            page = context.new_page()

            for i, url in enumerate(urls, start=1):
                print(f"[{i}/{len(urls)}] Scraping")
                rows = scrape_product(page, url)
                for row in rows:
                    writer.writerow(row)

                time.sleep(random.uniform(2.0, 4.0))

            browser.close()

    upload_output_to_ftp()

# --------------------------------------------------
if __name__ == "__main__":
    main()