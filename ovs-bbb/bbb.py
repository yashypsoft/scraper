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
from datetime import datetime
from typing import List, Dict, Any, Optional
from retrying import retry
import time
from fake_useragent import UserAgent
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BBBExtractor:
    def __init__(self, api_url: str, max_concurrent: int = 10):
        """
        Initialize BBB Extractor
        
        Args:
            api_url: Base API URL (e.g., https://api.bedbathandbeyond.com/options)
            max_concurrent: Maximum concurrent requests
        """
        self.api_url = api_url.rstrip('/')
        self.max_concurrent = max_concurrent
        self.ua = UserAgent()
        self.session = None
        
    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        self.session = aiohttp.ClientSession(connector=connector)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    async def fetch_variant_data(self, variant_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch variant data from BBB API
        
        Args:
            variant_id: Variant ID to fetch
            
        Returns:
            Dictionary containing variant data or None if failed
        """
        # Handle NaN/None variant IDs
        if pd.isna(variant_id) or variant_id in ['nan', 'NaN', 'None', '']:
            logger.warning(f"Skipping invalid variant ID: {variant_id}")
            return None
            
        url = f"{self.api_url}/{variant_id}"
        headers = {
            'User-Agent': self.ua.random,
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        
        try:
            async with self.session.get(url, headers=headers, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Successfully fetched variant {variant_id}")
                    return data
                elif response.status == 404:
                    logger.warning(f"Variant {variant_id} not found (404)")
                    return None
                else:
                    logger.warning(f"HTTP {response.status} for variant {variant_id}")
                    return None
                    
        except asyncio.TimeoutError:
            logger.warning(f"Timeout fetching variant {variant_id}")
            return None
        except aiohttp.ClientError as e:
            logger.warning(f"Client error fetching variant {variant_id}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Error fetching variant {variant_id}: {e}")
            return None
    
    def extract_sku_from_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract relevant data from API response
        
        Args:
            data: API response data
            
        Returns:
            Dictionary with extracted fields
        """
        result = {
            'BBB_SKU': None,
            'BBB_ModelNumber': None,
            'BBB_OptionId': None,
            'BBB_Description': None,
            'BBB_Dimensions': None,
            'BBB_Attributes': None,
            'BBB_Attributes_Count': 0,
            'BBB_AttributeIcons_Count': 0,
        }
        
        if not data:
            return result
        
        try:
            # Extract modelNumber (SKU)
            result['BBB_SKU'] = data.get('modelNumber')
            result['BBB_ModelNumber'] = data.get('modelNumber')
            result['BBB_OptionId'] = data.get('optionId')
            result['BBB_Description'] = data.get('description')
            
            # Extract dimensions
            dims = data.get('assembledDimensions', {})
            if dims:
                length = dims.get('length', '')
                width = dims.get('width', '')
                height = dims.get('height', '')
                result['BBB_Dimensions'] = f"{length}x{width}x{height}"
            
            # Extract attributes as string
            attributes = data.get('attributes', [])
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
            icons = data.get('attributeIcons', [])
            result['BBB_AttributeIcons_Count'] = len(icons)
            
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
        
        return result
    
    async def process_batch(self, variant_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Process a batch of variant IDs concurrently
        
        Args:
            variant_ids: List of variant IDs to process
            
        Returns:
            List of dictionaries with variant data
        """
        tasks = []
        for variant_id in variant_ids:
            task = self.fetch_variant_data(str(variant_id))
            tasks.append(task)
        
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        results = []
        for variant_id, response in zip(variant_ids, responses):
            if isinstance(response, Exception):
                logger.error(f"Exception for variant {variant_id}: {response}")
                results.append({
                    'Ref Varient ID': variant_id,
                    'BBB_SKU': None,
                    'BBB_ModelNumber': None,
                    'BBB_OptionId': None,
                    'BBB_Description': None,
                    'BBB_Dimensions': None,
                    'BBB_Attributes': None,
                    'BBB_Attributes_Count': 0,
                    'BBB_AttributeIcons_Count': 0,
                    'BBB_Error': str(response)
                })
            else:
                extracted = self.extract_sku_from_data(response)
                results.append({
                    'Ref Varient ID': variant_id,
                    **extracted,
                    'BBB_Error': None
                })
        
        return results


def clean_variant_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate variant IDs in the DataFrame
    
    Args:
        df: Input DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    # Convert to string and clean
    df_clean['Ref Varient ID'] = df_clean['Ref Varient ID'].astype(str).str.strip()
    
    # Remove decimal points from float-like strings
    df_clean['Ref Varient ID'] = df_clean['Ref Varient ID'].str.replace(r'\.0$', '', regex=True)
    
    # Filter out invalid variant IDs
    invalid_mask = (
        df_clean['Ref Varient ID'].isna() |
        (df_clean['Ref Varient ID'] == 'nan') |
        (df_clean['Ref Varient ID'] == 'NaN') |
        (df_clean['Ref Varient ID'] == 'None') |
        (df_clean['Ref Varient ID'] == '') |
        (~df_clean['Ref Varient ID'].str.match(r'^\d+$'))
    )
    
    invalid_count = invalid_mask.sum()
    if invalid_count > 0:
        logger.warning(f"Found {invalid_count} invalid variant IDs:")
        invalid_ids = df_clean.loc[invalid_mask, 'Ref Varient ID'].unique()[:10]
        for vid in invalid_ids:
            logger.warning(f"  - {vid}")
        if invalid_count > 10:
            logger.warning(f"  ... and {invalid_count - 10} more")
    
    # Keep only valid rows for processing
    df_valid = df_clean[~invalid_mask].copy()
    
    return df_valid


async def process_chunk(input_file: str, chunk_id: int, total_chunks: int, api_url: str) -> pd.DataFrame:
    """
    Process a chunk of the input CSV
    
    Args:
        input_file: Path to input CSV file
        chunk_id: Current chunk ID (1-indexed)
        total_chunks: Total number of chunks
        api_url: BBB API URL
        
    Returns:
        DataFrame with results
    """
    # Read input CSV
    logger.info(f"Reading input file: {input_file}")
    try:
        df = pd.read_csv(input_file, dtype={'Ref Varient ID': str})
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise
    
    # Validate required columns
    required_columns = ['Ref Varient ID']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        logger.info(f"Available columns: {list(df.columns)}")
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Clean variant IDs
    df_clean = clean_variant_ids(df)
    
    logger.info(f"Total rows in file: {len(df)}")
    logger.info(f"Valid rows after cleaning: {len(df_clean)}")
    logger.info(f"Unique variant IDs: {df_clean['Ref Varient ID'].nunique()}")
    
    if len(df_clean) == 0:
        logger.warning("No valid variant IDs to process")
        # Return empty DataFrame with expected columns
        return pd.DataFrame()
    
    # Split into chunks
    chunk_size = len(df_clean) // total_chunks
    if chunk_size == 0:
        chunk_size = 1
    
    start_idx = (chunk_id - 1) * chunk_size
    end_idx = start_idx + chunk_size if chunk_id < total_chunks else len(df_clean)
    
    # Ensure we don't go out of bounds
    start_idx = min(start_idx, len(df_clean))
    end_idx = min(end_idx, len(df_clean))
    
    chunk_df = df_clean.iloc[start_idx:end_idx].copy()
    chunk_variant_ids = chunk_df['Ref Varient ID'].tolist()
    
    logger.info(f"Processing chunk {chunk_id}/{total_chunks}: rows {start_idx}-{end_idx} ({len(chunk_df)} rows)")
    logger.info(f"Sample variant IDs: {chunk_variant_ids[:5] if len(chunk_variant_ids) > 5 else chunk_variant_ids}")
    
    if len(chunk_variant_ids) == 0:
        logger.warning("No variant IDs to process in this chunk")
        return pd.DataFrame()
    
    # Process variant IDs
    async with BBBExtractor(api_url, max_concurrent=5) as extractor:
        results = await extractor.process_batch(chunk_variant_ids)
    
    # Create results DataFrame
    results_df = pd.DataFrame(results)
    
    # Merge with original chunk data
    if len(results_df) > 0 and len(chunk_df) > 0:
        # Ensure Ref Varient ID is string for merge
        results_df['Ref Varient ID'] = results_df['Ref Varient ID'].astype(str)
        chunk_df['Ref Varient ID'] = chunk_df['Ref Varient ID'].astype(str)
        
        merged_df = chunk_df.merge(results_df, on='Ref Varient ID', how='left')
    else:
        merged_df = chunk_df.copy()
        # Add BBB columns with NaN values
        bbb_columns = [
            'BBB_SKU', 'BBB_ModelNumber', 'BBB_OptionId', 'BBB_Description',
            'BBB_Dimensions', 'BBB_Attributes', 'BBB_Attributes_Count',
            'BBB_AttributeIcons_Count', 'BBB_Error'
        ]
        for col in bbb_columns:
            merged_df[col] = None
    
    # Reorder columns to have BBB columns at the end
    original_columns = [col for col in merged_df.columns if not col.startswith('BBB_')]
    bbb_columns = [col for col in merged_df.columns if col.startswith('BBB_')]
    
    # Ensure all columns are present
    final_columns = original_columns + bbb_columns
    missing_in_final = [col for col in merged_df.columns if col not in final_columns]
    if missing_in_final:
        final_columns.extend(missing_in_final)
    
    final_df = merged_df[final_columns]
    
    # Log statistics
    successful = final_df['BBB_SKU'].notna().sum()
    failed = final_df['BBB_SKU'].isna().sum()
    
    logger.info(f"Chunk {chunk_id} results: {successful} successful, {failed} failed")
    
    if successful > 0:
        sample_skus = final_df['BBB_SKU'].dropna().head(3).tolist()
        logger.info(f"Sample SKUs: {sample_skus}")
    
    return final_df


def convert_to_serializable(obj):
    """Convert numpy/pandas types to Python native types for JSON serialization"""
    if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
        return float(obj) if not np.isnan(obj) else None
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif pd.isna(obj):
        return None
    return obj


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Extract BBB SKUs from OVS variants')
    parser.add_argument('--chunk-id', type=int, required=True, help='Chunk ID (1-indexed)')
    parser.add_argument('--total-chunks', type=int, required=True, help='Total number of chunks')
    parser.add_argument('--input-file', type=str, required=True, help='Input CSV file path')
    parser.add_argument('--api-url', type=str, required=True, 
                       help='BBB API URL (e.g., https://api.bedbathandbeyond.com/options)')
    parser.add_argument('--output-dir', type=str, default='output', help='Output directory')
    parser.add_argument('--max-concurrent', type=int, default=5, help='Maximum concurrent requests')
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Configure file logging
    log_file = os.path.join(args.output_dir, f'bbb_extractor_chunk_{args.chunk_id}.log')
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    
    logger.info(f"Starting BBB SKU Extractor")
    logger.info(f"Chunk ID: {args.chunk_id}/{args.total_chunks}")
    logger.info(f"Input file: {args.input_file}")
    logger.info(f"API URL: {args.api_url}")
    logger.info(f"Max concurrent requests: {args.max_concurrent}")
    
    try:
        # Process chunk
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result_df = loop.run_until_complete(
            process_chunk(args.input_file, args.chunk_id, args.total_chunks, args.api_url)
        )
        
        # Save output
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(
            args.output_dir, 
            f"bbb_output_chunk_{args.chunk_id}_{timestamp}.csv"
        )
        
        # Convert numpy types to Python types before saving CSV
        for col in result_df.columns:
            if result_df[col].dtype in [np.int64, np.float64]:
                result_df[col] = result_df[col].astype(object).where(pd.notna(result_df[col]), None)
        
        result_df.to_csv(output_file, index=False)
        
        logger.info(f"Output saved to: {output_file}")
        logger.info(f"Output shape: {result_df.shape}")
        
        if len(result_df) > 0:
            logger.info(f"Columns: {', '.join(result_df.columns)}")
        
        # Create summary with serializable types
        summary = {
            'chunk_id': int(args.chunk_id),
            'total_rows': int(len(result_df)),
            'successful': int(result_df['BBB_SKU'].notna().sum()) if len(result_df) > 0 else 0,
            'failed': int(result_df['BBB_SKU'].isna().sum()) if len(result_df) > 0 else 0,
            'output_file': str(output_file),
            'timestamp': str(timestamp),
            'api_url': str(args.api_url),
            'input_file': str(args.input_file)
        }
        
        # Convert all values to serializable types
        summary = {k: convert_to_serializable(v) for k, v in summary.items()}
        
        summary_file = os.path.join(args.output_dir, f"summary_chunk_{args.chunk_id}.json")
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"Summary saved to: {summary_file}")
        logger.info(f"Summary: {summary}")
        
        logger.info("BBB SKU extraction completed")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()