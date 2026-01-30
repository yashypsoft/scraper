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
        url = f"{self.api_url}/{variant_id}"
        headers = {
            'User-Agent': self.ua.random,
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        
        try:
            async with self.session.get(url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Successfully fetched variant {variant_id}")
                    return data
                elif response.status == 404:
                    logger.warning(f"Variant {variant_id} not found (404)")
                    return None
                else:
                    logger.error(f"Error fetching variant {variant_id}: HTTP {response.status}")
                    response.raise_for_status()
                    
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching variant {variant_id}")
            return None
        except aiohttp.ClientError as e:
            logger.error(f"Client error fetching variant {variant_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching variant {variant_id}: {e}")
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
                result['BBB_Dimensions'] = f"{dims.get('length', '')}x{dims.get('width', '')}x{dims.get('height', '')}"
            
            # Extract attributes as string
            attributes = data.get('attributes', [])
            if attributes:
                attr_list = []
                for attr in attributes:
                    name = attr.get('name', '')
                    value = attr.get('value', '')
                    attr_list.append(f"{name}: {value}")
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
            task = self.fetch_variant_data(variant_id)
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
        df = pd.read_csv(input_file)
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise
    
    # Validate required columns
    required_columns = ['Ref Varient ID', 'Ref Product URL', 'Ref Product ID', 'Ref SKU', 'Ref MPN']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        logger.info(f"Available columns: {list(df.columns)}")
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Clean and validate variant IDs
    df['Ref Varient ID'] = df['Ref Varient ID'].astype(str).str.strip()
    variant_ids = df['Ref Varient ID'].tolist()
    
    logger.info(f"Total rows: {len(df)}")
    logger.info(f"Unique variant IDs: {df['Ref Varient ID'].nunique()}")
    
    # Split into chunks
    chunk_size = len(df) // total_chunks
    start_idx = (chunk_id - 1) * chunk_size
    end_idx = start_idx + chunk_size if chunk_id < total_chunks else len(df)
    
    chunk_df = df.iloc[start_idx:end_idx].copy()
    chunk_variant_ids = chunk_df['Ref Varient ID'].tolist()
    
    logger.info(f"Processing chunk {chunk_id}/{total_chunks}: rows {start_idx}-{end_idx} ({len(chunk_df)} rows)")
    
    # Process variant IDs
    async with BBBExtractor(api_url, max_concurrent=10) as extractor:
        results = await extractor.process_batch(chunk_variant_ids)
    
    # Create results DataFrame
    results_df = pd.DataFrame(results)
    
    # Merge with original chunk data
    merged_df = chunk_df.merge(results_df, on='Ref Varient ID', how='left')
    
    # Reorder columns to have BBB columns at the end
    original_columns = [col for col in merged_df.columns if not col.startswith('BBB_')]
    bbb_columns = [col for col in merged_df.columns if col.startswith('BBB_')]
    
    final_df = merged_df[original_columns + bbb_columns]
    
    # Log statistics
    successful = final_df['BBB_SKU'].notna().sum()
    failed = final_df['BBB_SKU'].isna().sum()
    
    logger.info(f"Chunk {chunk_id} results: {successful} successful, {failed} failed")
    
    if successful > 0:
        logger.info(f"Sample SKUs: {final_df['BBB_SKU'].dropna().head(5).tolist()}")
    
    return final_df


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Extract BBB SKUs from OVS variants')
    parser.add_argument('--chunk-id', type=int, required=True, help='Chunk ID (1-indexed)')
    parser.add_argument('--total-chunks', type=int, required=True, help='Total number of chunks')
    parser.add_argument('--input-file', type=str, required=True, help='Input CSV file path')
    parser.add_argument('--api-url', type=str, required=True, 
                       help='BBB API URL (e.g., https://api.bedbathandbeyond.com/options)')
    parser.add_argument('--output-dir', type=str, default='output', help='Output directory')
    
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
        
        result_df.to_csv(output_file, index=False)
        
        logger.info(f"Output saved to: {output_file}")
        logger.info(f"Output shape: {result_df.shape}")
        logger.info("BBB SKU extraction completed successfully")
        
        # Also save a summary
        summary = {
            'chunk_id': args.chunk_id,
            'total_rows': len(result_df),
            'successful': result_df['BBB_SKU'].notna().sum(),
            'failed': result_df['BBB_SKU'].isna().sum(),
            'output_file': output_file,
            'timestamp': timestamp
        }
        
        summary_file = os.path.join(args.output_dir, f"summary_chunk_{args.chunk_id}.json")
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Summary saved to: {summary_file}")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()