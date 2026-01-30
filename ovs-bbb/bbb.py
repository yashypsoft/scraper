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
import random
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, wait_fixed, wait_random

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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Origin": "https://www.bedbathandbeyond.com",
        "Referer": "https://www.bedbathandbeyond.com/",
    })
    
    # Configure adapter with larger timeouts for GitHub Actions
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=5,
        pool_maxsize=10,
        max_retries=0,  # We'll handle retries manually with tenacity
        pool_block=False
    )
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    
    return session

# Create a shared session
session = create_session()

@retry(
    stop=stop_after_attempt(5),  # Increased to 5 attempts
    wait=wait_exponential(multiplier=2, min=5, max=60),  # Longer waits between retries
    retry=retry_if_exception_type((
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.ChunkedEncodingError,
        requests.exceptions.ReadTimeout,
        requests.exceptions.ConnectTimeout,
        requests.exceptions.ProxyError,
        requests.exceptions.SSLError,
    ))
)
def fetch_json_with_retry(url: str, attempt: int) -> Optional[dict]:
    """Fetch JSON data with retry logic and varying timeouts"""
    try:
        # Vary headers slightly to avoid blocking
        headers = {
            "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(110, 130)}.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.bedbathandbeyond.com/",
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        }
        
        # Increase timeout with each attempt
        timeout_multiplier = min(attempt, 3)  # Cap at 3x
        connect_timeout = 10 + (5 * timeout_multiplier)
        read_timeout = 30 + (15 * timeout_multiplier)
        
        logger.info(f"Attempt {attempt}: Fetching {url} with timeout ({connect_timeout}, {read_timeout})")
        
        r = session.get(
            url, 
            headers=headers, 
            timeout=(connect_timeout, read_timeout), 
            verify=False,
            allow_redirects=True
        )
        
        if r.status_code == 200:
            try:
                data = r.json()
                logger.debug(f"Successfully fetched JSON for attempt {attempt}")
                return data
            except json.JSONDecodeError as e:
                logger.warning(f"JSON decode error for {url}: {e}")
                # Check if we got HTML instead of JSON
                if '<html' in r.text.lower() or '<!doctype' in r.text.lower():
                    logger.warning(f"Got HTML response instead of JSON for {url}")
                return None
        elif r.status_code == 404:
            logger.warning(f"404 Not Found for {url}")
            return None
        elif r.status_code == 429:  # Rate limited
            wait_time = 30 * attempt  # Exponential backoff for rate limiting
            logger.warning(f"Rate limited (429) for {url}, waiting {wait_time} seconds")
            time.sleep(wait_time)
            raise requests.exceptions.RetryError(f"Rate limited - waiting {wait_time}s")
        elif r.status_code == 403:
            logger.warning(f"Access forbidden (403) for {url}")
            # Try with different headers
            if attempt < 3:
                logger.info(f"Will retry with different headers on attempt {attempt + 1}")
                raise requests.exceptions.RetryError("403 Forbidden - retrying")
            return None
        elif r.status_code >= 500:
            logger.warning(f"Server error {r.status_code} for {url}")
            wait_time = 10 * attempt
            logger.info(f"Server error, waiting {wait_time} seconds before retry")
            time.sleep(wait_time)
            raise requests.exceptions.RetryError(f"Server error {r.status_code}")
        else:
            logger.warning(f"HTTP {r.status_code} for {url}")
            if 400 <= r.status_code < 500:
                # Client error, don't retry
                return None
            # Server error, retry
            raise requests.exceptions.RetryError(f"HTTP {r.status_code}")
            
    except (requests.exceptions.Timeout, requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout) as e:
        logger.warning(f"Timeout for {url} on attempt {attempt}: {type(e).__name__}")
        raise  # This will trigger retry
    except requests.exceptions.ConnectionError as e:
        logger.warning(f"Connection error for {url} on attempt {attempt}: {e}")
        # Add a delay before retry
        time.sleep(10)
        raise  # This will trigger retry
    except Exception as e:
        logger.error(f"Unexpected error fetching JSON from {url} on attempt {attempt}: {type(e).__name__}: {str(e)[:200]}")
        if attempt < 3:  # Only retry unexpected errors for first few attempts
            raise
        return None

def fetch_json(url: str) -> Optional[dict]:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()

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

def process_variant_data(variant_id: str, stats: dict, request_delay: float = 3.0) -> Dict[str, Any]:
    """Process a single BBB variant ID with multiple fallback strategies"""
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
        
        # Try different API endpoints and strategies
        api_endpoints = [
            f"https://api.bedbathandbeyond.com/options/{variant_id}",
        #     f"https://api.bedbathandbeyond.com/v1/options/{variant_id}",
        #     f"https://api.bedbathandbeyond.com/api/options/{variant_id}",
        #     f"https://api.bedbathandbeyond.com/product/{variant_id}",
        ]
        
        data = None
        endpoint_used = None
        
        for api_url in api_endpoints:
            logger.debug(f"Trying API endpoint: {api_url}")
            try:
                data = fetch_json(api_url)
                if data:
                    endpoint_used = api_url
                    logger.info(f"Successfully fetched data from {endpoint_used}")
                    break
            except Exception as e:
                logger.debug(f"Failed to fetch from {api_url}: {e}")
                continue
            
            # Add delay between endpoint attempts
            time.sleep(1)
        
        if not data:
            # Try one more strategy: check if endpoint exists without variant ID pattern
            logger.warning(f"All API endpoints failed for variant {variant_id}")
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
                'BBB_Error': 'All API endpoints failed or timed out',
                'BBB_API_Endpoint': '',
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
            'BBB_API_Endpoint': endpoint_used or '',
            'BBB_API_Response': json.dumps(data) if variant_info else ''
        }
        
        stats['processed'] += 1
        sku = variant_info.get('BBB_SKU', 'N/A')
        logger.info(f"Successfully processed variant {variant_id}: SKU={sku}")
        
        # Add jitter to request delay to avoid pattern detection
        jitter = random.uniform(0.5, 1.5)
        actual_delay = request_delay * jitter
        if actual_delay > 0:
            logger.debug(f"Sleeping for {actual_delay:.2f} seconds (jitter: {jitter:.2f})")
            time.sleep(actual_delay)
        
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
            'BBB_Error': str(e)[:500],
            'BBB_API_Endpoint': '',
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
    parser.add_argument('--max-workers', type=int, default=1, help='Maximum concurrent requests (1 for reliability)')
    parser.add_argument('--request-delay', type=float, default=5.0, help='Delay between requests in seconds (increased)')
    parser.add_argument('--timeout', type=int, default=45, help='Request timeout in seconds')
    parser.add_argument('--skip-errors', action='store_true', help='Continue processing even if some variants fail')
    parser.add_argument('--max-retries', type=int, default=5, help='Maximum retry attempts per request')
    
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
    logger.info(f"Max workers: {args.max_workers} (1 for reliability)")
    logger.info(f"Request delay: {args.request_delay}s (with jitter)")
    logger.info(f"Timeout: {args.timeout}s")
    logger.info(f"Max retries: {args.max_retries}")
    logger.info(f"Skip errors: {args.skip_errors}")
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
                'BBB_Error',
                'BBB_API_Endpoint'
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
        'invalid': 0,
        'retries': 0
    }
    
    # Process variant IDs sequentially with conservative settings
    results = []
    
    logger.info(f"Processing {len(variant_ids)} variant IDs sequentially")
    
    for i, variant_id in enumerate(variant_ids, 1):
        logger.info(f"Processing {i}/{len(variant_ids)}: variant {variant_id}")
        
        try:
            # Add initial delay for first request
            if i == 1:
                logger.info("Initial delay of 3 seconds before first request...")
                time.sleep(3)
            
            result = process_variant_data(variant_id, stats, args.request_delay)
            if result:
                results.append(result)
            
            # Progress update
            if i % 5 == 0 or i == len(variant_ids):
                success_rate = (stats['processed'] / i) * 100 if i > 0 else 0
                logger.info(f"Progress: {i}/{len(variant_ids)} | Success: {stats['processed']} | Errors: {stats['errors']} | Rate: {success_rate:.1f}%")
                
                # If we're having many errors, increase delay
                if stats['errors'] > 0 and stats['errors'] > stats['processed']:
                    logger.warning("High error rate detected. Increasing delay...")
                    args.request_delay = min(args.request_delay * 1.5, 30.0)  # Cap at 30 seconds
                    logger.info(f"New request delay: {args.request_delay:.2f}s")
            
        except KeyboardInterrupt:
            logger.info("Interrupted by user. Saving partial results...")
            break
        except Exception as e:
            logger.error(f"Fatal error processing variant {variant_id}: {e}")
            if not args.skip_errors:
                logger.error("Stopping due to fatal error (use --skip-errors to continue)")
                break
            stats['errors'] += 1
    
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
            'BBB_AttributeIcons_Names', 'BBB_Error', 'BBB_API_Endpoint', 'BBB_API_Response'
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
        'BBB_API_Endpoint',
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
        error_rate = (stats['errors'] / len(variant_ids)) * 100
        logger.info(f"Success rate: {success_rate:.1f}%")
        logger.info(f"Error rate: {error_rate:.1f}%")
    
    # Show sample of successful SKUs
    if stats['processed'] > 0:
        skus = results_df['BBB_SKU'].dropna().unique().tolist()
        if skus:
            logger.info(f"Sample SKUs found: {skus[:10]}")
    
    logger.info("=" * 60)
    logger.info(f"Output saved to: {output_file}")
    logger.info(f"Output shape: {results_df.shape}")
    logger.info(f"Output columns: {len(results_df.columns)}")
    
    # List output files
    output_files = [f for f in os.listdir(args.output_dir) if f.endswith('.csv') or f.endswith('.json') or f.endswith('.log')]
    logger.info(f"Output files created: {output_files}")
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
        'error_rate': f"{error_rate:.1f}%" if len(variant_ids) > 0 else "0%",
        'api_url': args.api_url,
        'max_workers': args.max_workers,
        'request_delay': args.request_delay,
        'max_retries': args.max_retries,
        'skip_errors': args.skip_errors,
        'timestamp': datetime.now().isoformat(),
        'output_files': output_files
    }
    
    summary_file = os.path.join(args.output_dir, f"summary_chunk_{args.chunk_id}.json")
    with open(summary_file, 'w') as f_json:
        json.dump(summary, f_json, indent=2, default=str)
    
    logger.info(f"Summary saved to: {summary_file}")
    
    # Clean up
    session.close()
    gc.collect()
    
    # Final status
    if stats['errors'] == 0 or (args.skip_errors and stats['processed'] > 0):
        logger.info("BBB Extractor finished successfully")
        return 0
    else:
        logger.warning("BBB Extractor finished with errors")
        return 1

if __name__ == "__main__":
    # Install required packages if not available
    try:
        import tenacity
    except ImportError:
        import subprocess
        import sys
        logger = logging.getLogger(__name__)
        logger.info("Installing tenacity...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "tenacity"])
        import tenacity
    
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)