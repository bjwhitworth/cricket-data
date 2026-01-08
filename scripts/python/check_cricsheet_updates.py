#!/usr/bin/env python3
"""
Check for new cricket match data on Cricsheet and optionally download updates.

The script downloads and inspects the all_json.zip file from Cricsheet to determine
which files are new compared to your local data directory. It can then selectively
extract only the new files you need.

Usage:
    python scripts/python/check_cricsheet_updates.py              # Check only
    python scripts/python/check_cricsheet_updates.py --download   # Download new files
    python scripts/python/check_cricsheet_updates.py --download --limit 50  # Limit downloads
"""

import argparse
import sys
from pathlib import Path
import requests
import zipfile
import io


CRICSHEET_ZIP_URL = "https://cricsheet.org/downloads/all_json.zip"
LOCAL_DATA_DIR = Path("data/raw/all_json")


def get_local_files():
    """Get list of JSON files already downloaded."""
    if not LOCAL_DATA_DIR.exists():
        LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
        return set()
    
    return {f.name for f in LOCAL_DATA_DIR.glob("*.json")}


def get_cricsheet_files():
    """Download and inspect Cricsheet zip file to get list of available files."""
    print("   Downloading zip file metadata...", end=' ')
    
    try:
        # Download the zip file
        response = requests.get(CRICSHEET_ZIP_URL, timeout=60)
        response.raise_for_status()
        print("✓")
    except requests.RequestException as e:
        print("✗")
        print(f"❌ Failed to fetch Cricsheet zip file: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Read zip contents without extracting
    try:
        zip_data = io.BytesIO(response.content)
        with zipfile.ZipFile(zip_data, 'r') as zip_ref:
            # Get all JSON filenames in the zip
            json_files = {
                name for name in zip_ref.namelist() 
                if name.endswith('.json') and not name.startswith('__MACOSX')
            }
            return json_files, zip_data
    except zipfile.BadZipFile as e:
        print(f"❌ Failed to read zip file: {e}", file=sys.stderr)
        sys.exit(1)


def extract_files(zip_data, files_to_extract, output_dir):
    """Extract specific files from zip to output directory."""
    zip_data.seek(0)  # Reset to beginning
    
    with zipfile.ZipFile(zip_data, 'r') as zip_ref:
        success_count = 0
        for i, filename in enumerate(files_to_extract, 1):
            try:
                print(f"   [{i}/{len(files_to_extract)}] {filename}...", end=' ')
                zip_ref.extract(filename, output_dir)
                
                # Move file if it's in a subdirectory
                extracted_path = output_dir / filename
                if extracted_path.parent != output_dir:
                    target_path = output_dir / extracted_path.name
                    extracted_path.rename(target_path)
                    # Clean up empty directories
                    try:
                        extracted_path.parent.rmdir()
                    except:
                        pass
                
                print("✓")
                success_count += 1
            except Exception as e:
                print(f"✗ ({e})")
        
        return success_count


def main():
    parser = argparse.ArgumentParser(
        description="Check for new cricket match data on Cricsheet"
    )
    parser.add_argument(
        '--download', '-d',
        action='store_true',
        help='Download new files (default is check only)'
    )
    parser.add_argument(
        '--limit', '-l',
        type=int,
        default=None,
        help='Limit number of files to download'
    )
    
    args = parser.parse_args()
    
    print("🔍 Checking for updates on Cricsheet...")
    print(f"   Source: {CRICSHEET_ZIP_URL}")
    print(f"   Local:  {LOCAL_DATA_DIR.absolute()}\n")
    
    # Get file lists
    local_files = get_local_files()
    cricsheet_files, zip_data = get_cricsheet_files()
    
    # Validate scraping worked
    if len(cricsheet_files) == 0:
        print("❌ Error: Found 0 files on Cricsheet. This likely means scraping failed.", file=sys.stderr)
        print("   The website structure may have changed or the page is unavailable.", file=sys.stderr)
        sys.exit(1)
    
    # Compare
    new_files = cricsheet_files - local_files
    removed_files = local_files - cricsheet_files
    
    # Report
    print(f"📊 Summary:")
    print(f"   Local files:      {len(local_files):,}")
    print(f"   Cricsheet files:  {len(cricsheet_files):,}")
    print(f"   New files:        {len(new_files):,}")
    print(f"   Removed files:    {len(removed_files):,}\n")
    
    if not new_files and not removed_files:
        print("✅ No changes detected. You're up to date!")
        return
    
    if not new_files and removed_files:
        print("⚠️  No new files, but some local files are no longer on Cricsheet.")
        print("    This may indicate files were removed or the scraping failed.")
        print()
    
    # Show sample of new files
    print(f"🆕 New files available ({len(new_files)} total):")
    sample_size = min(10, len(new_files))
    for filename in sorted(new_files)[:sample_size]:
        print(f"   - {filename}")
    
    if len(new_files) > sample_size:
        print(f"   ... and {len(new_files) - sample_size} more")
    
    print()
    
    if removed_files:
        print(f"⚠️  Files removed from Cricsheet ({len(removed_files)} total):")
        sample_size = min(5, len(removed_files))
        for filename in sorted(removed_files)[:sample_size]:
            print(f"   - {filename}")
        if len(removed_files) > sample_size:
            print(f"   ... and {len(removed_files) - sample_size} more")
        print()
    
    # Download if requested
    if args.download:
        files_to_download = list(new_files)
        if args.limit:
            files_to_download = files_to_download[:args.limit]
        
        print(f"⬇️  Extracting {len(files_to_download)} files from zip...")
        
        success_count = extract_files(zip_data, files_to_download, LOCAL_DATA_DIR)
        
        print(f"\n✅ Extracted {success_count}/{len(files_to_download)} files successfully")
        
        if success_count > 0:
            print("\n💡 Next steps:")
            print("   1. Run: dbt run")
            print("   2. Run: dbt test")
            print("   3. Verify new matches in the database")
    else:
        print("💡 To download new files, run:")
        print(f"   python {sys.argv[0]} --download")
        if len(new_files) > 100:
            print(f"   Or limit downloads: python {sys.argv[0]} --download --limit 100")


if __name__ == '__main__':
    main()
