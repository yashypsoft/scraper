#!/usr/bin/env python3
"""
BBB SKU Extractor from OVS Variants
Extracts modelNumber from BBB API for each variant ID
"""

import pandas as pd
import requests
import asyncio
import aiohttp
import aiofiles
import json
import logging
import sys
import os
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from retrying import retry
import time
from fake_useragent import UserAgent
import numpy as np
import re
import csv
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= ENV =================
INPUT_CSV = os.getenv("INPUT_CSV", "products_chunk_0.csv")
CHUNK_ID = int(os.getenv("CHUNK_ID", "1"))
TOTAL_CHUNKS = int(os.getenv("TOTAL_CHUNKS", "1"))
BBB_API_BASE = os.getenv("BBB_API_BASE", "https://api.bedbathandbeyond.com/options")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.5"))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "bbb_output")

OUTPUT_CSV = f"bbb_products_chunk_{CHUNK_ID}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ================= LOGGER =================

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()

# ================= HTTP SESSION =================

session = requests.Session()
# Add default headers to session for all requests
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
})

def http_get(url: str) -> Optional[str]:
    """HTTP GET request for BBB API"""
    for attempt in range(3):
        try:
            r = session.get(url, timeout=15, verify=True)
            if r.status_code == 200:
                log(f"Success fetching {url}", "DEBUG")
                return r.text
            elif r.status_code == 404:
                log(f"404 Not Found for {url}", "WARNING")
                return None
            elif r.status_code == 429:  # Rate limited
                log(f"Rate limited (429) for {url}, attempt {attempt+1}", "WARNING")
                time.sleep(5)
            else:
                log(f"Status {r.status_code} for {url}", "WARNING")
                if r.status_code >= 500:
                    time.sleep(2)
        except requests.exceptions.Timeout:
            log(f"Timeout on attempt {attempt+1} for {url}", "WARNING")
            time.sleep(2)
        except Exception as e:
            log(f"Attempt {attempt+1} failed for {url}: {type(e).__name__}", "WARNING")
            time.sleep(1)
    return None

def fetch_json(url: str) -> Optional[dict]:
    """Fetch JSON data from BBB API"""
    try:
        data = http_get(url)
        if data:
            return json.loads(data)
        return None
    except json.JSONDecodeError as e:
        log(f"JSON decode error for {url}: {e}", "ERROR")
        return None
    except Exception as e:
        log(f"Error fetching JSON from {url}: {e}", "ERROR")
        return None

# ================= PRODUCT PROCESSING =================

def extract_bbb_data(variant_data: dict) -> Dict[str, Any]:
    """
    Extract data from BBB API response
    
    Args:
        variant_data: BBB API response data
        
    Returns:
        Dictionary with extracted fields
    """
    try:
        if not variant_data:
            return {}
        
        # Basic extraction
        result = {
            'BBB_SKU': variant_data.get('modelNumber'),
            'BBB_ModelNumber': variant_data.get('modelNumber'),
            'BBB_OptionId': variant_data.get('optionId'),
            'BBB_Description': variant_data.get('description'),
            'BBB_Dimensions': None,
            'BBB_Attributes': None,
            'BBB_Attributes_Count': 0,
            'BBB_AttributeIcons_Count': 0,
        }
        
        # Extract dimensions
        dims = variant_data.get('assembledDimensions', {})
        if dims:
            length = dims.get('length', '')
            width = dims.get('width', '')
            height = dims.get('height', '')
            result['BBB_Dimensions'] = f"{length}x{width}x{height}"
        
        # Extract attributes
        attributes = variant_data.get('attributes', [])
        if attributes:
            attr_list = []
            for attr in attributes:
                name = attr.get('name', '')
                value = attr.get('value', '')
                if name and value:
                    attr_list.append(f"{name}: {value}")
            if attr_list:
                result['BBB_Attributes'] = " | ".join(attr_list)
            result['BBB_Attributes_Count'] = len(attributes)
        
        # Count attribute icons
        icons = variant_data.get('attributeIcons', [])
        result['BBB_AttributeIcons_Count'] = len(icons)
        
        return result
        
    except Exception as e:
        log(f"Error extracting BBB data: {e}", "ERROR")
        return {}

def process_variant_data(variant_id: str, writer, stats: dict):
    """Process a single BBB variant ID"""
    if not variant_id or pd.isna(variant_id):
        stats['skipped'] += 1
        return
    
    # Clean variant ID (remove .0 if present)
    variant_id = str(variant_id).strip()
    variant_id = re.sub(r'\.0$', '', variant_id)
    
    # Validate variant ID is numeric
    if not re.match(r'^\d+$', variant_id):
        log(f"Invalid variant ID format: {variant_id}", "WARNING")
        stats['invalid'] += 1
        return
    
    log(f"Processing variant ID: {variant_id}", "DEBUG")
    
    # Try multiple possible API endpoints for BBB
    api_endpoints = [
        f"https://api.bedbathandbeyond.com/options/{variant_id}",
        # f"https://api.bedbathandbeyond.com/products/{variant_id}/options",
        # f"https://api.bedbathandbeyond.com/v1/options/{variant_id}",
    ]
    
    data = None
    for api_url in api_endpoints:
        log(f"Trying API endpoint: {api_url}", "DEBUG")
        data = fetch_json(api_url)
        if data:
            break
        time.sleep(0.5)
    
    if not data:
        log(f"No data found for variant {variant_id}", "WARNING")
        stats['errors'] += 1
        return
    
    # Extract data from response
    variant_info = extract_bbb_data(data)
    if not variant_info:
        stats['errors'] += 1
        return
    
    # Prepare row data
    row = [
        variant_id,
        variant_info.get('BBB_SKU', ''),
        variant_info.get('BBB_ModelNumber', ''),
        variant_info.get('BBB_OptionId', ''),
        variant_info.get('BBB_Description', ''),
        variant_info.get('BBB_Dimensions', ''),
        variant_info.get('BBB_Attributes', ''),
        variant_info.get('BBB_Attributes_Count', ''),
        variant_info.get('BBB_AttributeIcons_Count', ''),
        SCRAPED_DATE
    ]
    
    writer.writerow(row)
    stats['processed'] += 1
    log(f"Processed variant {variant_id}: {variant_info.get('BBB_SKU', 'N/A')}", "INFO")
    
    # Respect request delay
    time.sleep(REQUEST_DELAY)

# ================= MAIN =================

def main():
    log("=" * 60)
    log("BBB SKU Extractor")
    log(f"Timestamp: {SCRAPED_DATE}")
    log(f"Input CSV: {INPUT_CSV}")
    log(f"Chunk ID: {CHUNK_ID}/{TOTAL_CHUNKS}")
    log(f"BBB API Base: {BBB_API_BASE}")
    log(f"Max Workers: {MAX_WORKERS}")
    log(f"Request Delay: {REQUEST_DELAY}s")
    log(f"Output Directory: {OUTPUT_DIR}")
    log("=" * 60)
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, OUTPUT_CSV)
    
    # Read input CSV
    log(f"Loading input CSV: {INPUT_CSV}")
    try:
        df = pd.read_csv(INPUT_CSV, dtype={'Ref Varient ID': str})
    except Exception as e:
        log(f"Error reading CSV file: {e}", "ERROR")
        sys.exit(1)
    
    # Validate required columns
    if 'Ref Varient ID' not in df.columns:
        log("Missing required column: 'Ref Varient ID'", "ERROR")
        log(f"Available columns: {list(df.columns)}")
        sys.exit(1)
    
    # Clean variant IDs
    df['Ref Varient ID'] = df['Ref Varient ID'].astype(str).str.strip()
    df['Ref Varient ID'] = df['Ref Varient ID'].str.replace(r'\.0$', '', regex=True)
    
    # Filter valid numeric variant IDs
    valid_mask = df['Ref Varient ID'].str.match(r'^\d+$')
    df_valid = df[valid_mask].copy()
    
    log(f"Total rows in file: {len(df)}")
    log(f"Valid rows after cleaning: {len(df_valid)}")
    log(f"Unique variant IDs: {df_valid['Ref Varient ID'].nunique()}")
    
    if len(df_valid) == 0:
        log("No valid variant IDs to process", "WARNING")
        # Create empty output file
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Ref Varient ID',
                'BBB_SKU',
                'BBB_ModelNumber',
                'BBB_OptionId',
                'BBB_Description',
                'BBB_Dimensions',
                'BBB_Attributes',
                'BBB_Attributes_Count',
                'BBB_AttributeIcons_Count',
                'Date Scrapped'
            ])
        log(f"Empty output created: {output_file}")
        sys.exit(0)
    
    # Split into chunks if needed
    if TOTAL_CHUNKS > 1:
        chunk_size = len(df_valid) // TOTAL_CHUNKS
        if chunk_size == 0:
            chunk_size = 1
        
        start_idx = (CHUNK_ID - 1) * chunk_size
        end_idx = start_idx + chunk_size if CHUNK_ID < TOTAL_CHUNKS else len(df_valid)
        
        start_idx = min(start_idx, len(df_valid))
        end_idx = min(end_idx, len(df_valid))
        
        chunk_df = df_valid.iloc[start_idx:end_idx].copy()
        log(f"Processing chunk {CHUNK_ID}/{TOTAL_CHUNKS}: rows {start_idx}-{end_idx} ({len(chunk_df)} rows)")
    else:
        chunk_df = df_valid.copy()
        log(f"Processing all {len(chunk_df)} rows")
    
    variant_ids = chunk_df['Ref Varient ID'].unique().tolist()
    log(f"Total variant IDs to process: {len(variant_ids)}")
    log(f"Sample variant IDs: {variant_ids[:5] if len(variant_ids) > 5 else variant_ids}")
    
    # Initialize CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow([
            'Ref Varient ID',
            'BBB_SKU',
            'BBB_ModelNumber',
            'BBB_OptionId',
            'BBB_Description',
            'BBB_Dimensions',
            'BBB_Attributes',
            'BBB_Attributes_Count',
            'BBB_AttributeIcons_Count',
            'Date Scrapped'
        ])
        
        # Initialize tracking
        stats = {
            'processed': 0,
            'errors': 0,
            'skipped': 0,
            'invalid': 0
        }
        
        # Process variant IDs in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for variant_id in variant_ids:
                future = executor.submit(process_variant_data, variant_id, writer, stats)
                futures.append(future)
            
            # Wait for all tasks to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    log(f"Error in thread execution: {e}", "ERROR")
                    stats['errors'] += 1
        
        # Clean up memory
        gc.collect()
        
        # Print statistics
        log("=" * 60)
        log("EXTRACTION STATISTICS")
        log("=" * 60)
        log(f"Variant IDs processed: {stats['processed']}")
        log(f"Errors encountered: {stats['errors']}")
        log(f"Skipped (invalid): {stats['skipped'] + stats['invalid']}")
        if stats['processed'] > 0:
            success_rate = (stats['processed'] / len(variant_ids)) * 100
            log(f"Success rate: {success_rate:.1f}%")
        log("=" * 60)
        log(f"Completed: {output_file}")
        log("=" * 60)
        
        # Create summary JSON
        summary = {
            'chunk_id': CHUNK_ID,
            'total_chunks': TOTAL_CHUNKS,
            'input_file': INPUT_CSV,
            'output_file': output_file,
            'total_variant_ids': len(variant_ids),
            'processed': stats['processed'],
            'errors': stats['errors'],
            'skipped': stats['skipped'],
            'invalid': stats['invalid'],
            'api_base': BBB_API_BASE,
            'scraped_date': SCRAPED_DATE,
            'timestamp': datetime.now().isoformat()
        }
        
        summary_file = os.path.join(OUTPUT_DIR, f"summary_chunk_{CHUNK_ID}.json")
        with open(summary_file, 'w') as f_json:
            json.dump(summary, f_json, indent=2, default=str)
        
        log(f"Summary saved to: {summary_file}")

if __name__ == "__main__":
    # Suppress SSL warnings
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Validate environment variables
    if not os.path.exists(INPUT_CSV):
        log(f"Error: Input CSV file not found: {INPUT_CSV}", "ERROR")
        sys.exit(1)
    
    main()