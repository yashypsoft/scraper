Here you go — ready-to-save README.md file.
Just copy-paste this into README.md.

# Parallel Sitemap Scraper (Python + GitHub Actions)

High-throughput product scraper that:
- Reads **XML sitemap indexes**
- Processes sitemaps **in parallel using GitHub Actions matrix**
- Automatically detects **Cloudflare vs normal sites**
- Extracts **Shopify + non-Shopify product data**
- Merges results and uploads final CSV to **FTP**

Built for **large e-commerce catalogs**.

---

## Features

- Parallel sitemap processing
- Auto Cloudflare detection (no forced bypass)
- Shopify `.json` endpoint first, HTML fallback
- JSON-LD product fallback
- Safe XML parsing (namespace + malformed XML tolerant)
- Deduplicated product URLs
- CSV chunk merge
- FTP upload support

---

## Project Structure

.
├── scrapercloud.py
├── .github/
│   └── workflows/
│       └── scraper.yml
├── README.md

---

## Requirements

### Local
- Python **3.10+**

```bash
pip install requests cloudscraper beautifulsoup4 lxml

GitHub Actions
	•	ubuntu-latest
	•	No Selenium / Chrome required

⸻

Environment Variables

Used by scrapercloud.py:

Variable	Description
CURR_URL	Base site URL
SITEMAP_OFFSET	Sitemap start index
MAX_SITEMAPS	Number of sitemaps to process
MAX_URLS_PER_SITEMAP	Max URLs per sitemap


⸻

GitHub Actions Inputs

Input	Description	Default
url	Target site	required
total_sitemaps	Force sitemap count (0 = auto)	0
sitemaps_per_job	Parallel sitemaps per job	2
urls_per_sitemap	Max URLs per sitemap	200


⸻

How It Works

1. Sitemap Detection
	•	Loads /sitemap.xml
	•	Counts <sitemap> entries automatically
	•	Manual override supported

2. Job Planning
	•	Splits sitemap list into chunks
	•	Builds GitHub Actions matrix dynamically

3. Scraping Logic

For each product URL:
	1.	Try {product}.json
	2.	Fallback to HTML scraping
	3.	Fallback to JSON-LD
	4.	Normalize images
	5.	Write one CSV row per variant

Cloudflare is detected automatically and bypassed only when required.

4. Merge
	•	Downloads CSV chunks
	•	Merges into one file
	•	Skips duplicate headers

5. Delivery
	•	Uploads final CSV to FTP
	•	Saves artifact in GitHub Actions

⸻

CSV Output Format

product_id
product_title
vendor
type
handle
variant_id
variant_title
sku
barcode
option_1_name
option_1_value
option_2_name
option_2_value
option_3_name
option_3_value
variant_price
available
variant_url
image_url


⸻

Run Locally

export CURR_URL=https://example.com
export SITEMAP_OFFSET=0
export MAX_SITEMAPS=1
export MAX_URLS_PER_SITEMAP=50

python scrapercloud.py


⸻

FTP Configuration (GitHub Secrets)

Secret	Description
FTP_HOST	FTP host
FTP_USER	FTP username
FTP_PASS	FTP password
FTP_OUTPUT_PATH	Target directory


⸻

Notes
	•	Lightweight request delays included
	•	No JavaScript execution
	•	Not intended to bypass aggressive bot protection
	•	Best suited for Shopify / static e-commerce

⸻

Recommended Settings
	•	sitemaps_per_job: 2–4
	•	urls_per_sitemap: 100–300
	•	Avoid running same site concurrently

⸻

License

Internal / Private Use

If you want, I can also give:
- `README.min.md` (short ops version)
- Architecture diagram
- Troubleshooting section
- Example output CSV

Just tell me.