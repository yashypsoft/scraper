import os
import sys
import json
import argparse
import pandas as pd
import subprocess
from pathlib import Path

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from unified_scraper import UnifiedScraper

# Configuration
COMPETITORS_FILE = "competitors.json"
OUTPUT_DIR = "data/exports"

def load_competitors():
    if not os.path.exists(COMPETITORS_FILE):
        print(f"Error: {COMPETITORS_FILE} not found.")
        sys.exit(1)
    with open(COMPETITORS_FILE, "r") as f:
        return json.load(f)

def list_competitors():
    competitors = load_competitors()
    print("\nAvailable Competitors:")
    print("-" * 50)
    for key, info in competitors.items():
        status = info.get("status", "pending")
        print(f"{key:25} | Status: {status:15} | Domain: {info.get('domain')}")
    print("-" * 50)

def scrape_single_url(url, output_file=None):
    scraper = UnifiedScraper()
    result = scraper.scrape_url(url)
    print(json.dumps(result, indent=2))
    
    if output_file and result and "error" not in result:
        df = pd.DataFrame([result])
        df.to_csv(output_file, index=False)
        print(f"\nSaved result to {output_file}")
    return result

def scrape_file(file_path):
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return
        
    with open(file_path, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]
        
    print(f"Scraping {len(urls)} URLs from {file_path}...")
    scraper = UnifiedScraper()
    results = []
    
    for i, url in enumerate(urls):
        print(f"[{i+1}/{len(urls)}] Scraping: {url}")
        res = scraper.scrape_url(url)
        if res and "error" not in res:
            results.append(res)
        else:
            print(f"  Failed: {res.get('error') if res else 'Unknown error'}")
            
    if results:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        filename = f"batch_scrape_{timestamp}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        df = pd.DataFrame(results)
        df.to_csv(filepath, index=False)
        print(f"\nSuccessfully scraped {len(results)}/{len(urls)} URLs.")
        print(f"Report saved to: {filepath}")
    else:
        print("\nNo data collected from the provided URLs.")

def run_competitor_scraper(key):
    # This maintains existing sitemap-based scraper functionality
    competitors = load_competitors()
    if key not in competitors:
        print(f"Error: Competitor {key} not found.")
        return
        
    info = competitors[key]
    scraper_script = info.get("scraper")
    if not scraper_script:
        print(f"Error: No scraper script defined for {key}.")
        return
        
    print(f"Launching scraper for {key}: {scraper_script}")
    try:
        subprocess.run(["python3", scraper_script], check=True)
    except Exception as e:
        print(f"Error running scraper: {e}")

def main():
    parser = argparse.ArgumentParser(description="Unified Scraper Interface")
    parser.add_argument("--list", action="store_true", help="List available competitors")
    parser.add_argument("--competitor", help="Run the full sitemap scraper for a specific competitor")
    parser.add_argument("--url", help="Scrape a single product URL immediately")
    parser.add_argument("--file", help="Scrape a list of URLs from a text file")
    parser.add_argument("--output", help="Optional output filename for single URL scrape")
    
    args = parser.parse_args()
    
    if args.list:
        list_competitors()
    elif args.competitor:
        run_competitor_scraper(args.competitor)
    elif args.url:
        scrape_single_url(args.url, args.output)
    elif args.file:
        scrape_file(args.file)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
