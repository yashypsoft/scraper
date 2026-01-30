#!/usr/bin/env python3
"""
BBB SKU Extractor from OVS Variants
Extracts modelNumber from BBB API for each variant ID
"""

import pandas as pd
import requests
import json
import logging
import sys
import os
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import time
import re
import csv
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed, ThreadPoolExecutor
import urllib3
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, wait_fixed

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================= LOGGER =================

def setup_logging(chunk_id: int):
    """Setup logging configuration"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"bbb_extractor_chunk_{chunk_id}_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stderr)
        ]
    )
    return logging.getLogger(__name__)

# ================= HTTP SESSION =================

def create_session():
    """Create and configure HTTP session with better timeout settings for GitHub Actions"""
    session = requests.Session()
    
    # More realistic headers for BBB API
    session.headers.update({
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        # "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        # "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        # "Cache-Control": "no-cache",
        "Sec-Fetch-User": "?1",
    })
    
    # Configure adapter with larger timeouts for GitHub Actions
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=10,
        pool_maxsize=20,
        max_retries=3,
        pool_block=False
    )
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    
    return session

# Create a shared session
session = create_session()

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.ChunkedEncodingError,
        requests.exceptions.ReadTimeout
    ))
)
def fetch_json(url: str) -> Optional[dict]:
    """Fetch JSON data with proper headers and longer timeout for GitHub Actions"""
    try:
        # Headers specifically for JSON/API requests
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.bedbathandbeyond.com/",
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        }
        
        # Increased timeout for GitHub Actions (network may be slower)
        # Using connect timeout of 10s and read timeout of 30s
        r = session.get(url, headers=headers, timeout=(10, 30), verify=True)
        
        if r.status_code == 200:
            try:
                return r.json()
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for {url}: {e}")
                # Try to get text response for debugging
                logger.debug(f"Response text: {r.text[:500]}")
                return None
        elif r.status_code == 404:
            logger.warning(f"404 Not Found for {url}")
            return None
        elif r.status_code == 429:  # Rate limited
            logger.warning(f"Rate limited (429) for {url}, waiting 15 seconds")
            time.sleep(15)
            raise requests.exceptions.RetryError("Rate limited")
        elif r.status_code == 403:
            logger.warning(f"Access forbidden (403) for {url}")
            return None
        elif r.status_code >= 500:
            logger.warning(f"Server error {r.status_code} for {url}")
            time.sleep(3)
            raise requests.exceptions.RetryError(f"Server error {r.status_code}")
        else:
            logger.warning(f"HTTP {r.status_code} for {url}")
            return None
            
    except requests.exceptions.Timeout as e:
        logger.warning(f"Timeout for {url}: {e}")
        raise  # This will trigger retry
    except requests.exceptions.ConnectionError as e:
        logger.warning(f"Connection error for {url}: {e}")
        raise  # This will trigger retry
    except requests.exceptions.ReadTimeout as e:
        logger.warning(f"Read timeout for {url}: {e}")
        raise  # This will trigger retry
    except Exception as e:
        logger.error(f"Error fetching JSON from {url}: {type(e).__name__}: {str(e)[:200]}")
        return None

# ================= DATA PROCESSING =================

def extract_bbb_data(variant_data: dict) -> Dict[str, Any]:
    """
    Extract data from BBB API response based on actual data structure
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
            'BBB_AttributeIcons_URLs': None,
            'BBB_AttributeIcons_Names': None
        }
        
        # Extract dimensions with units
        dims = variant_data.get('assembledDimensions', {})
        if dims:
            length = dims.get('length', '')
            width = dims.get('width', '')
            height = dims.get('height', '')
            length_units = dims.get('lengthUnits', '')
            width_units = dims.get('widthUnits', '')
            height_units = dims.get('heightUnits', '')
            
            # Format: LxWxH with units
            if length and width:
                if height and height != 0:
                    result['BBB_Dimensions'] = f"{length}{length_units} x {width}{width_units} x {height}{height_units}"
                else:
                    result['BBB_Dimensions'] = f"{length}{length_units} x {width}{width_units}"
        
        # Extract attributes as string
        attributes = variant_data.get('attributes', [])
        if attributes:
            attr_list = []
            for attr in attributes:
                name = attr.get('name', '').strip()
                value = attr.get('value', '').strip()
                if name and value:
                    attr_list.append(f"{name}: {value}")
            
            if attr_list:
                result['BBB_Attributes'] = " | ".join(attr_list)
            result['BBB_Attributes_Count'] = len(attributes)
        
        # Extract attribute icons
        icons = variant_data.get('attributeIcons', [])
        result['BBB_AttributeIcons_Count'] = len(icons)
        
        # Extract icon URLs and names
        if icons:
            icon_urls = []
            icon_names = []
            for icon in icons:
                url = icon.get('url', '')
                attribute_name = icon.get('attributeName', '')
                attribute_value = icon.get('attributeValue', '')
                
                if url:
                    icon_urls.append(url)
                
                if attribute_name:
                    icon_names.append(f"{attribute_name}: {attribute_value}")
            
            if icon_urls:
                result['BBB_AttributeIcons_URLs'] = " | ".join(icon_urls)
            
            if icon_names:
                result['BBB_AttributeIcons_Names'] = " | ".join(icon_names)
        
        return result
        
    except Exception as e:
        logger.error(f"Error extracting BBB data: {e}")
        return {}

def process_variant_data(variant_id: str, stats: dict, request_delay: float = 1.0) -> Dict[str, Any]:
    """Process a single BBB variant ID"""
    try:
        if not variant_id or pd.isna(variant_id):
            stats['skipped'] += 1
            return None
        
        # Clean variant ID
        variant_id = str(variant_id).strip()
        variant_id = re.sub(r'\.0$', '', variant_id)
        
        # Validate variant ID is numeric
        if not re.match(r'^\d+$', variant_id):
            logger.warning(f"Invalid variant ID format: {variant_id}")
            stats['invalid'] += 1
            return None
        
        logger.info(f"Processing variant ID: {variant_id}")
        
        # Try different API endpoints in order
        api_endpoints = [
            f"https://api.bedbathandbeyond.com/options/{variant_id}",
            # f"https://api.bedbathandbeyond.com/v1/options/{variant_id}",
            # f"https://api.bedbathandbeyond.com/api/options/{variant_id}",
        ]
        
        data = None
        for api_url in api_endpoints:
            logger.debug(f"Trying API endpoint: {api_url}")
            data = fetch_json(api_url)
            if data:
                logger.debug(f"Successfully fetched data for variant {variant_id}")
                break
            # Small delay between endpoint attempts
            time.sleep(0.5)
        
        if not data:
            logger.warning(f"No data found for variant {variant_id}")
            stats['errors'] += 1
            
            # Return minimal result with error
            return {
                'Ref Varient ID': variant_id,
                'BBB_SKU': '',
                'BBB_ModelNumber': '',
                'BBB_OptionId': '',
                'BBB_Description': '',
                'BBB_Dimensions': '',
                'BBB_Attributes': '',
                'BBB_Attributes_Count': '',
                'BBB_AttributeIcons_Count': '',
                'BBB_AttributeIcons_URLs': '',
                'BBB_AttributeIcons_Names': '',
                'BBB_Error': 'No data found or timeout',
                'BBB_API_Response': ''
            }
        
        # Extract data from response
        variant_info = extract_bbb_data(data)
        
        # Prepare result with all fields
        result = {
            'Ref Varient ID': variant_id,
            'BBB_SKU': variant_info.get('BBB_SKU', ''),
            'BBB_ModelNumber': variant_info.get('BBB_ModelNumber', ''),
            'BBB_OptionId': variant_info.get('BBB_OptionId', ''),
            'BBB_Description': variant_info.get('BBB_Description', ''),
            'BBB_Dimensions': variant_info.get('BBB_Dimensions', ''),
            'BBB_Attributes': variant_info.get('BBB_Attributes', ''),
            'BBB_Attributes_Count': variant_info.get('BBB_Attributes_Count', 0),
            'BBB_AttributeIcons_Count': variant_info.get('BBB_AttributeIcons_Count', 0),
            'BBB_AttributeIcons_URLs': variant_info.get('BBB_AttributeIcons_URLs', ''),
            'BBB_AttributeIcons_Names': variant_info.get('BBB_AttributeIcons_Names', ''),
            'BBB_Error': '',
            'BBB_API_Response': json.dumps(data) if variant_info else ''
        }
        
        stats['processed'] += 1
        sku = variant_info.get('BBB_SKU', 'N/A')
        logger.info(f"Processed variant {variant_id}: SKU={sku}")
        
        # Respect request delay (increased for rate limiting)
        if request_delay > 0:
            time.sleep(request_delay)
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing variant {variant_id if 'variant_id' in locals() else 'UNKNOWN'}: {e}")
        stats['errors'] += 1
        return {
            'Ref Varient ID': variant_id if 'variant_id' in locals() else '',
            'BBB_SKU': '',
            'BBB_ModelNumber': '',
            'BBB_OptionId': '',
            'BBB_Description': '',
            'BBB_Dimensions': '',
            'BBB_Attributes': '',
            'BBB_Attributes_Count': '',
            'BBB_AttributeIcons_Count': '',
            'BBB_AttributeIcons_URLs': '',
            'BBB_AttributeIcons_Names': '',
            'BBB_Error': str(e)[:500],  # Limit error message length
            'BBB_API_Response': ''
        }

# ================= MAIN FUNCTION =================

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Extract BBB SKUs from OVS variants')
    parser.add_argument('--chunk-id', type=int, required=True, help='Chunk ID (1-indexed)')
    parser.add_argument('--total-chunks', type=int, required=True, help='Total number of chunks')
    parser.add_argument('--input-file', type=str, required=True, help='Input CSV file path')
    parser.add_argument('--api-url', type=str, default='https://api.bedbathandbeyond.com/options',
                       help='BBB API base URL (default: https://api.bedbathandbeyond.com/options)')
    parser.add_argument('--output-dir', type=str, default='output', help='Output directory')
    parser.add_argument('--max-workers', type=int, default=2, help='Maximum concurrent requests (reduced for rate limiting)')
    parser.add_argument('--request-delay', type=float, default=2.0, help='Delay between requests in seconds (increased for rate limiting)')
    parser.add_argument('--timeout', type=int, default=30, help='Request timeout in seconds')
    
    args = parser.parse_args()
    
    # Setup logging
    global logger
    logger = setup_logging(args.chunk_id)
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    logger.info("=" * 60)
    logger.info("BBB SKU Extractor")
    logger.info(f"Chunk ID: {args.chunk_id}/{args.total_chunks}")
    logger.info(f"Input file: {args.input_file}")
    logger.info(f"API URL: {args.api_url}")
    logger.info(f"Max workers: {args.max_workers} (reduced to avoid rate limiting)")
    logger.info(f"Request delay: {args.request_delay}s (increased to avoid rate limiting)")
    logger.info(f"Timeout: {args.timeout}s")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info("=" * 60)
    
    # Read input CSV
    logger.info(f"Loading input CSV: {args.input_file}")
    try:
        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        df = None
        for encoding in encodings:
            try:
                df = pd.read_csv(args.input_file, dtype={'Ref Varient ID': str}, encoding=encoding)
                logger.info(f"Successfully read with {encoding} encoding")
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            # Last resort: try without specifying encoding
            df = pd.read_csv(args.input_file, dtype={'Ref Varient ID': str})
            
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        sys.exit(1)
    
    # Check if required column exists
    variant_id_column = None
    possible_columns = ['Ref Varient ID', 'Ref Variant ID', 'variant_id', 'Variant ID', 'variantId', 
                        'variation_id', 'Variation ID', 'ID']
    
    for col in possible_columns:
        if col in df.columns:
            variant_id_column = col
            logger.info(f"Found variant ID column: {col}")
            break
    
    if not variant_id_column:
        logger.error(f"Missing variant ID column. Available columns: {list(df.columns)}")
        sys.exit(1)
    
    # Rename to standard column name for processing
    if variant_id_column != 'Ref Varient ID':
        df = df.rename(columns={variant_id_column: 'Ref Varient ID'})
        logger.info(f"Renamed column '{variant_id_column}' to 'Ref Varient ID'")
    
    # Clean variant IDs
    logger.info(f"Original data shape: {df.shape}")
    df['Ref Varient ID'] = df['Ref Varient ID'].astype(str).str.strip()
    df['Ref Varient ID'] = df['Ref Varient ID'].str.replace(r'\.0$', '', regex=True)
    
    # Remove any rows with empty variant IDs
    df = df[df['Ref Varient ID'].notna() & (df['Ref Varient ID'] != '')]
    
    # Filter valid numeric variant IDs
    valid_mask = df['Ref Varient ID'].str.match(r'^\d+$')
    df_valid = df[valid_mask].copy()
    
    invalid_count = len(df) - len(df_valid)
    if invalid_count > 0:
        logger.warning(f"Found {invalid_count} invalid variant IDs (non-numeric or empty)")
        # Show sample of invalid IDs
        invalid_samples = df[~valid_mask]['Ref Varient ID'].head(10).tolist()
        logger.warning(f"Sample invalid IDs: {invalid_samples}")
    
    logger.info(f"Valid rows after cleaning: {len(df_valid)}")
    logger.info(f"Unique variant IDs: {df_valid['Ref Varient ID'].nunique()}")
    
    if len(df_valid) == 0:
        logger.warning("No valid variant IDs to process")
        # Create empty output file with headers
        output_file = os.path.join(args.output_dir, f"bbb_output_chunk_{args.chunk_id}.csv")
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
                'BBB_AttributeIcons_URLs',
                'BBB_AttributeIcons_Names',
                'BBB_Error'
            ])
        logger.info(f"Empty output created: {output_file}")
        sys.exit(0)
    
    # Split into chunks
    if args.total_chunks > 1:
        chunk_size = len(df_valid) // args.total_chunks
        if chunk_size == 0:
            chunk_size = 1
        
        start_idx = (args.chunk_id - 1) * chunk_size
        end_idx = start_idx + chunk_size if args.chunk_id < args.total_chunks else len(df_valid)
        
        start_idx = min(start_idx, len(df_valid))
        end_idx = min(end_idx, len(df_valid))
        
        chunk_df = df_valid.iloc[start_idx:end_idx].copy()
        logger.info(f"Processing chunk {args.chunk_id}/{args.total_chunks}: rows {start_idx}-{end_idx} ({len(chunk_df)} rows)")
    else:
        chunk_df = df_valid.copy()
        logger.info(f"Processing all {len(chunk_df)} rows")
    
    # Get unique variant IDs
    variant_ids = chunk_df['Ref Varient ID'].unique().tolist()
    logger.info(f"Total variant IDs to process: {len(variant_ids)}")
    if len(variant_ids) > 0:
        logger.info(f"Sample variant IDs: {variant_ids[:10]}")
    
    # Initialize statistics
    stats = {
        'processed': 0,
        'errors': 0,
        'skipped': 0,
        'invalid': 0
    }
    
    # Process variant IDs with rate limiting
    results = []
    processed_ids = set()
    
    # Use sequential processing instead of ThreadPoolExecutor for better reliability
    logger.info(f"Processing {len(variant_ids)} variant IDs sequentially to avoid rate limiting")
    
    for i, variant_id in enumerate(variant_ids, 1):
        if variant_id in processed_ids:
            continue
            
        logger.info(f"Processing {i}/{len(variant_ids)}: variant {variant_id}")
        
        try:
            result = process_variant_data(variant_id, stats, args.request_delay)
            if result:
                results.append(result)
                processed_ids.add(variant_id)
        except Exception as e:
            logger.error(f"Unexpected error processing variant {variant_id}: {e}")
            stats['errors'] += 1
        
        # Progress update every 10 items
        if i % 10 == 0:
            logger.info(f"Progress: {i}/{len(variant_ids)} completed")
    
    # Alternative: Use ThreadPoolExecutor with very conservative settings
    # if you want to keep parallel processing
    # batch_size = min(10, args.max_workers)
    # for i in range(0, len(variant_ids), batch_size):
    #     batch = variant_ids[i:i + batch_size]
    #     logger.info(f"Processing batch {i//batch_size + 1}/{(len(variant_ids) + batch_size - 1)//batch_size} ({len(batch)} IDs)")
        
    #     with ThreadPoolExecutor(max_workers=min(1, len(batch))) as executor:  # Only 1 worker
    #         futures = []
    #         for variant_id in batch:
    #             if variant_id in processed_ids:
    #                 continue
                    
    #             future = executor.submit(
    #                 process_variant_data, 
    #                 variant_id, 
    #                 stats, 
    #                 args.request_delay
    #             )
    #             futures.append(future)
    #             processed_ids.add(variant_id)
            
    #         # Collect results
    #         for future in as_completed(futures):
    #             try:
    #                 result = future.result()
    #                 if result:
    #                     results.append(result)
    #             except Exception as e:
    #                 logger.error(f"Error in thread execution: {e}")
    #                 stats['errors'] += 1
        
    #     # Delay between batches
    #     if i + batch_size < len(variant_ids):
    #         logger.info(f"Batch completed, pausing for 3 seconds...")
    #         time.sleep(3)
    
    # Create results DataFrame
    if results:
        results_df = pd.DataFrame(results)
        
        # Merge with original data if we have additional columns
        if len(chunk_df.columns) > 1:  # More than just Ref Varient ID
            results_df = chunk_df.merge(results_df, on='Ref Varient ID', how='left')
    else:
        # Create empty results with original data
        results_df = chunk_df.copy()
        # Add BBB columns with empty values
        bbb_columns = [
            'BBB_SKU', 'BBB_ModelNumber', 'BBB_OptionId', 'BBB_Description',
            'BBB_Dimensions', 'BBB_Attributes', 'BBB_Attributes_Count',
            'BBB_AttributeIcons_Count', 'BBB_AttributeIcons_URLs',
            'BBB_AttributeIcons_Names', 'BBB_Error', 'BBB_API_Response'
        ]
        for col in bbb_columns:
            results_df[col] = ''
    
    # Ensure all required columns are present
    required_columns = [
        'Ref Varient ID',
        'BBB_SKU',
        'BBB_ModelNumber',
        'BBB_OptionId',
        'BBB_Description',
        'BBB_Dimensions',
        'BBB_Attributes',
        'BBB_Attributes_Count',
        'BBB_AttributeIcons_Count',
        'BBB_AttributeIcons_URLs',
        'BBB_AttributeIcons_Names',
        'BBB_Error',
        'BBB_API_Response'
    ]
    
    # Add any missing columns
    for col in required_columns:
        if col not in results_df.columns:
            results_df[col] = ''
    
    # Reorder columns
    other_columns = [col for col in results_df.columns if col not in required_columns]
    final_columns = ['Ref Varient ID'] + other_columns + [col for col in required_columns if col != 'Ref Varient ID']
    results_df = results_df[final_columns]
    
    # Save output
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(args.output_dir, f"bbb_output_chunk_{args.chunk_id}.csv")
    
    # Convert any non-string columns to string for CSV output
    for col in results_df.columns:
        if results_df[col].dtype == 'object':
            results_df[col] = results_df[col].astype(str)
    
    results_df.to_csv(output_file, index=False, encoding='utf-8')
    
    # Print statistics
    logger.info("=" * 60)
    logger.info("EXTRACTION STATISTICS")
    logger.info("=" * 60)
    logger.info(f"Total variant IDs: {len(variant_ids)}")
    logger.info(f"Successfully processed: {stats['processed']}")
    logger.info(f"Errors encountered: {stats['errors']}")
    logger.info(f"Skipped (invalid/empty): {stats['skipped'] + stats['invalid']}")
    
    if len(variant_ids) > 0:
        success_rate = (stats['processed'] / len(variant_ids)) * 100
        logger.info(f"Success rate: {success_rate:.1f}%")
    
    # Show sample of successful SKUs
    if stats['processed'] > 0:
        skus = results_df['BBB_SKU'].dropna().unique().tolist()
        if skus:
            logger.info(f"Sample SKUs found: {skus[:10]}")
    
    logger.info("=" * 60)
    logger.info(f"Output saved to: {output_file}")
    logger.info(f"Output shape: {results_df.shape}")
    logger.info(f"Output columns: {len(results_df.columns)}")
    logger.info("=" * 60)
    
    # Create summary JSON
    summary = {
        'chunk_id': args.chunk_id,
        'total_chunks': args.total_chunks,
        'input_file': args.input_file,
        'output_file': output_file,
        'total_variant_ids': len(variant_ids),
        'processed': stats['processed'],
        'errors': stats['errors'],
        'skipped': stats['skipped'],
        'invalid': stats['invalid'],
        'success_rate': f"{success_rate:.1f}%" if len(variant_ids) > 0 else "0%",
        'api_url': args.api_url,
        'max_workers': args.max_workers,
        'request_delay': args.request_delay,
        'timestamp': datetime.now().isoformat()
    }
    
    summary_file = os.path.join(args.output_dir, f"summary_chunk_{args.chunk_id}.json")
    with open(summary_file, 'w') as f_json:
        json.dump(summary, f_json, indent=2, default=str)
    
    logger.info(f"Summary saved to: {summary_file}")
    
    # Clean up
    session.close()
    gc.collect()
    
    logger.info("BBB Extractor finished successfully")

if __name__ == "__main__":
    # Install tenacity if not available
    try:
        import tenacity
    except ImportError:
        import subprocess
        import sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "tenacity"])
        import tenacity
    
    main()