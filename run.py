#!/usr/bin/env python3
import csv
import json
import os
import sys
import time
import requests
from typing import Optional
from dotenv import load_dotenv


# ============================================================================
# CONFIGURATION - Update these values
# ============================================================================
BEARER_TOKEN_ASSET_INFO = os.getenv("BEARER_TOKEN_ASSET_INFO")
BEARER_TOKEN_PRICING = os.getenv("BEARER_TOKEN_PRICING")
CSV_FILE = "asset_list.csv" 
DRY_RUN = False  # Set to True to test without making changes
# ============================================================================


def get_asset_info(bsc_address: str, bearer_token: str) -> Optional[dict]:
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
                    "chain": "evm_56",
                    "hex_repr": bsc_address
                }
            }
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error getting asset info for {bsc_address}: {e}")
        if hasattr(e.response, 'text'):
            print(f"Response: {e.response.text}")
        return None


def update_asset_price(asset_id: str, coingecko_id: str, bearer_token: str, dry_run: bool = False) -> Optional[dict]:
    url = "https://api.fordefi.com/csm/pricing/update_price"
    
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    data = {
        "asset_id": asset_id,
        "price": "",  # Empty as shown in the example
        "coingecko_id": coingecko_id,
        "dry_run": str(dry_run).lower()
    }
    
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error updating price for asset {asset_id}: {e}")
        if hasattr(e.response, 'text'):
            print(f"Response: {e.response.text}")
        return None


def process_csv(csv_file: str, bearer_token_asset_info: str, bearer_token_pricing: str, dry_run: bool = False):
    successful = 0
    failed = 0
    skipped = 0
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row_num, row in enumerate(reader, start=2):  # Start at 2 (after header)
            name = row.get('Name', '')
            bsc_address = row.get('BSC Deployed Address', '').strip()
            coingecko_id = row.get('CoinGecko API ID', '').strip()
            
            print(f"\n{'='*80}")
            print(f"Processing row {row_num}: {name}")
            print(f"{'='*80}")
            
            # Skip if either field is empty
            if not bsc_address or not coingecko_id:
                print(f"⚠️  Skipping - Missing BSC address or CoinGecko ID")
                skipped += 1
                continue
            
            print(f"BSC Address: {bsc_address}")
            print(f"CoinGecko ID: {coingecko_id}")
            
            # Step 1: Get asset info
            print(f"\n📡 Step 1: Getting asset info...")
            asset_info = get_asset_info(bsc_address, bearer_token_asset_info)
            
            if not asset_info:
                print(f"❌ Failed to get asset info")
                failed += 1
                continue
            
            asset_id = asset_info.get('id')
            if not asset_id:
                print(f"❌ No asset ID in response")
                failed += 1
                continue
            
            print(f"✅ Got asset ID: {asset_id}")
            print(f"   Asset Name: {asset_info.get('name', 'N/A')}")
            print(f"   Symbol: {asset_info.get('symbol', 'N/A')}")
            
            # Step 2: Update price
            print(f"\n📡 Step 2: Updating price with CoinGecko ID...")
            price_response = update_asset_price(asset_id, coingecko_id, bearer_token_pricing, dry_run)
            
            if not price_response:
                print(f"❌ Failed to update price")
                failed += 1
                continue
            
            # Check for stdout in response
            stdout = price_response.get('stdout', '')
            if stdout:
                print(f"✅ Price updated successfully!")
                print(f"   stdout present: {len(stdout)} characters")
                # Print first 600 chars of stdout
                print(f"   Preview: {stdout[:600]}...")
                successful += 1
            else:
                print(f"⚠️  Warning: No stdout in response")
                print(f"   Response: {json.dumps(price_response, indent=2)}")
                failed += 1
            
            # Pause to respect rate limits
            print(f"\n⏸️  Pausing for 5 seconds to respect rate limits...")
            time.sleep(5)
    
    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"⚠️  Skipped: {skipped}")
    print(f"📊 Total processed: {successful + failed + skipped}")


def main():
    """Main entry point."""
    # Validate configuration
    if not BEARER_TOKEN_ASSET_INFO or BEARER_TOKEN_ASSET_INFO == "eyJ":
        print("Error: Please update BEARER_TOKEN_ASSET_INFO in the script configuration section")
        sys.exit(1)
    
    if not BEARER_TOKEN_PRICING or BEARER_TOKEN_PRICING == "eyJ":
        print("Error: Please update BEARER_TOKEN_PRICING in the script configuration section")
        sys.exit(1)
    
    if not CSV_FILE:
        print("Error: Please update CSV_FILE in the script configuration section")
        sys.exit(1)
    
    if not os.path.exists(CSV_FILE):
        print(f"Error: CSV file not found: {CSV_FILE}")
        sys.exit(1)
    
    print(f"Processing CSV file: {CSV_FILE}")
    if DRY_RUN:
        print("Running in DRY-RUN mode")
    print()
    
    process_csv(CSV_FILE, BEARER_TOKEN_ASSET_INFO, BEARER_TOKEN_PRICING, DRY_RUN)


if __name__ == "__main__":
    main()

