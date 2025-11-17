#!/usr/bin/env python3
import csv
import json
import logging
import os
import sys
import time
import requests
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ============================================================================
# CONFIGURATION - Update these values
# ============================================================================
BEARER_TOKEN_ASSET_INFO = os.getenv("BEARER_TOKEN_ASSET_INFO")
BEARER_TOKEN_PRICING = os.getenv("BEARER_TOKEN_PRICING")
CSV_FILE = "backed_list.csv"
DRY_RUN = False  # Set to True to test without making changes

# EVM chains to process
# EVM_CHAINS = ["evm_1", "evm_42161", "evm_56"]
EVM_CHAINS = ["evm_56"]

# Logging configuration
LOG_DIR = "logs"
LOG_FILE = f"{LOG_DIR}/mark_not_spam_evm_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
# ============================================================================


def setup_logging():
    """Set up logging configuration."""
    # Create logs directory if it doesn't exist
    os.makedirs(LOG_DIR, exist_ok=True)

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return logging.getLogger(__name__)


def get_asset_info(evm_address: str, chain: str, bearer_token: str, logger: logging.Logger) -> Optional[dict]:
    """Get asset info for a given EVM address and chain."""
    url = "https://api.fordefi.com/api/v1/assets/asset-infos"

    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "asset_identifier": {
            "type": "evm",
            "details": {
                "type": "erc20",
                "token": {
                    "chain": chain,
                    "hex_repr": evm_address
                }
            }
        }
    }

    try:
        logger.debug(f"Requesting asset info for {evm_address} on {chain}")
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        logger.debug(f"Successfully retrieved asset info for {evm_address} on {chain}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting asset info for {evm_address} on {chain}: {e}")
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            logger.error(f"Response: {e.response.text}")
        return None


def mark_asset_not_spam(asset_id: str, bearer_token: str, logger: logging.Logger) -> bool:
    """Mark an asset as not spam."""
    url = "https://api.fordefi.com/csm/assets/assets_mark_as_spam"

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    data = {
        "asset_id": asset_id,
        "spam": "false"
    }

    try:
        logger.debug(f"Marking asset {asset_id} as not spam")
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()

        if response.status_code == 200:
            logger.info(f"✅ Successfully marked asset {asset_id} as not spam (200 OK)")
            return True
        else:
            logger.warning(f"⚠️  Unexpected status code for asset {asset_id}: {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error marking asset {asset_id} as not spam: {e}")
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            logger.error(f"Response: {e.response.text}")
        return False


def process_csv(csv_file: str, bearer_token_asset_info: str, bearer_token_pricing: str, dry_run: bool, logger: logging.Logger):
    """Process the CSV file and mark assets as not spam."""
    stats = {
        "total_rows": 0,
        "skipped": 0,
        "processed_chains": 0,
        "failed_chains": 0,
        "by_chain": {chain: {"success": 0, "failed": 0, "not_found": 0} for chain in EVM_CHAINS}
    }

    logger.info(f"Starting to process CSV file: {csv_file}")

    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row_num, row in enumerate(reader, start=2):  # Start at 2 (after header)
            product_name = row.get('Product name', '').strip()
            token_symbol = row.get('Token Symbol', '').strip()
            evm_address = row.get('EVM address', '').strip()

            # Skip empty rows
            if not evm_address or not product_name:
                continue

            stats["total_rows"] += 1

            logger.info(f"\n{'='*80}")
            logger.info(f"Processing row {row_num}: {product_name} ({token_symbol})")
            logger.info(f"{'='*80}")
            logger.info(f"EVM Address: {evm_address}")

            if dry_run:
                logger.info(f"🧪 DRY RUN MODE - No changes will be made")

            # Process each chain
            for chain in EVM_CHAINS:
                logger.info(f"\n🔗 Processing chain: {chain}")

                # Step 1: Get asset info
                logger.info(f"   📡 Getting asset info...")
                asset_info = get_asset_info(evm_address, chain, bearer_token_asset_info, logger)

                if not asset_info:
                    logger.warning(f"   ⚠️  Asset not found on {chain}")
                    stats["by_chain"][chain]["not_found"] += 1
                    continue

                asset_id = asset_info.get('id')
                if not asset_id:
                    logger.error(f"   ❌ No asset ID in response")
                    stats["by_chain"][chain]["failed"] += 1
                    stats["failed_chains"] += 1
                    continue

                logger.info(f"   ✅ Got asset ID: {asset_id}")
                logger.info(f"      Asset Name: {asset_info.get('name', 'N/A')}")
                logger.info(f"      Symbol: {asset_info.get('symbol', 'N/A')}")

                # Step 2: Mark as not spam
                if not dry_run:
                    logger.info(f"   📡 Marking asset as not spam...")
                    success = mark_asset_not_spam(asset_id, bearer_token_pricing, logger)

                    if success:
                        stats["by_chain"][chain]["success"] += 1
                        stats["processed_chains"] += 1
                    else:
                        stats["by_chain"][chain]["failed"] += 1
                        stats["failed_chains"] += 1
                else:
                    logger.info(f"   🧪 DRY RUN: Would mark asset {asset_id} as not spam")
                    stats["by_chain"][chain]["success"] += 1
                    stats["processed_chains"] += 1

                # Small pause between chains
                time.sleep(1)

            # Pause between assets to respect rate limits
            logger.info(f"\n⏸️  Pausing for 0 seconds to respect rate limits...")
            time.sleep(0)

    # Summary
    logger.info(f"\n{'='*80}")
    logger.info(f"SUMMARY")
    logger.info(f"{'='*80}")
    logger.info(f"📊 Total rows processed: {stats['total_rows']}")
    logger.info(f"✅ Chains successfully processed: {stats['processed_chains']}")
    logger.info(f"❌ Chains failed: {stats['failed_chains']}")
    logger.info(f"⚠️  Rows skipped: {stats['skipped']}")

    logger.info(f"\n{'='*80}")
    logger.info(f"BY CHAIN BREAKDOWN")
    logger.info(f"{'='*80}")
    for chain in EVM_CHAINS:
        chain_stats = stats["by_chain"][chain]
        logger.info(f"\n{chain}:")
        logger.info(f"  ✅ Success: {chain_stats['success']}")
        logger.info(f"  ❌ Failed: {chain_stats['failed']}")
        logger.info(f"  ⚠️  Not found: {chain_stats['not_found']}")


def main():
    """Main entry point."""
    # Setup logging first
    logger = setup_logging()

    logger.info(f"Log file created: {LOG_FILE}")

    # Validate configuration
    if not BEARER_TOKEN_ASSET_INFO:
        logger.error("Error: BEARER_TOKEN_ASSET_INFO environment variable not set")
        logger.error("Please set it in your .env file or environment")
        sys.exit(1)

    if not BEARER_TOKEN_PRICING:
        logger.error("Error: BEARER_TOKEN_PRICING environment variable not set")
        logger.error("Please set it in your .env file or environment")
        sys.exit(1)

    if not os.path.exists(CSV_FILE):
        logger.error(f"Error: CSV file not found: {CSV_FILE}")
        sys.exit(1)

    logger.info(f"{'='*80}")
    logger.info(f"BACKED ASSETS - MARK AS NOT SPAM (EVM)")
    logger.info(f"{'='*80}")
    logger.info(f"CSV file: {CSV_FILE}")
    logger.info(f"Chains to process: {', '.join(EVM_CHAINS)}")
    if DRY_RUN:
        logger.warning("⚠️  Running in DRY-RUN mode - no changes will be made")
    logger.info("")

    process_csv(CSV_FILE, BEARER_TOKEN_ASSET_INFO, BEARER_TOKEN_PRICING, DRY_RUN, logger)

    logger.info(f"\n{'='*80}")
    logger.info(f"Script completed. Log saved to: {LOG_FILE}")
    logger.info(f"{'='*80}")


if __name__ == "__main__":
    main()
