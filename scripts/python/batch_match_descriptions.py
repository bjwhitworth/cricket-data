#!/usr/bin/env python3
"""
Batch generate and store cricket match descriptions using local threading.

This uses concurrent threads to generate descriptions for multiple matches,
with results stored as raw JSON in the database. dbt models then parse,
validate, and handle versioning. For large scale (20k+ matches), use
batch_match_descriptions_api.py instead for better cost and performance.

Usage:
    python scripts/python/batch_match_descriptions.py [--type brief|full] [--workers 4] [--limit 100]

Options:
    --type      brief or full (default: brief)
    --workers   number of concurrent workers (default: 4)
    --limit     process only first N matches (useful for testing)
"""

import sys
import os
import json
import duckdb
from generate_match_narrative import (
    generate_narrative, 
    DescriptionConfig,
    create_narrative_json_blob,
    store_narrative_json
)
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Configuration
DB_PATH = os.getenv('CRICKET_DB_PATH', 'data/duckdb/dev.duckdb')


def get_all_match_ids() -> list[str]:
    """Fetch all match IDs from the database."""
    with duckdb.connect(DB_PATH, read_only=True) as conn:
        cursor = conn.execute("SELECT DISTINCT match_id FROM stg_cricket__matches ORDER BY match_id")
        return [row[0] for row in cursor.fetchall()]


def process_match_with_storage(match_id: str, desc_type: str = 'brief', api_key: str = None, 
                               db_path: str = None) -> tuple[str, bool, str | None]:
    """Process a single match and store result as raw JSON.
    
    Returns:
        Tuple of (match_id, success, error_message or None)
    """
    try:
        description = generate_narrative(match_id, desc_type=desc_type, api_key=api_key)
        narrative_json = create_narrative_json_blob(match_id, desc_type, description, source='batch')
        store_narrative_json(narrative_json, db_path or DB_PATH)
        
        return (match_id, True, None)
    except Exception as e:
        return (match_id, False, str(e))


def batch_generate_and_store(desc_type: str = 'brief', workers: int = 4, limit: int = None) -> None:
    """Generate descriptions for all matches and store as raw JSON.
    
    Args:
        desc_type: Type of description ('brief' or 'full')
        workers: Number of concurrent worker threads
        limit: Optional limit on number of matches to process
    """
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set")
    
    # Create table if needed
    with duckdb.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_narratives (
                raw_narrative_id UUID DEFAULT gen_random_uuid(),
                narrative_json JSON,
                loaded_at TIMESTAMP DEFAULT now()
            )
        """)
    
    match_ids = get_all_match_ids()
    if limit:
        match_ids = match_ids[:limit]
    
    total = len(match_ids)
    print(f"Generating {desc_type} descriptions for {total} matches ({workers} workers)...")
    print(f"Results will be stored in match_narratives table\n")
    
    start_time = datetime.now()
    processed = 0
    succeeded = 0
    failed = 0
    error_details = []
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(process_match_with_storage, match_id, desc_type, api_key): match_id 
            for match_id in match_ids
        }
        
        # Process results as they complete
        for future in as_completed(futures):
            match_id, success, error = future.result()
            processed += 1
            
            if success:
                succeeded += 1
                status = "✓"
            else:
                failed += 1
                status = "✗"
                error_details.append(f"{match_id}: {error}")
            
            # Print progress
            if processed % 10 == 0 or processed == total:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = processed / elapsed if elapsed > 0 else 0
                pct = (processed / total) * 100
                eta_secs = (total - processed) / rate if rate > 0 else 0
                eta_mins = int(eta_secs / 60)
                print(f"[{processed:5d}/{total}] ({pct:5.1f}%) {status} | Rate: {rate:.1f}/s | ETA: {eta_mins}m")
    
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n{'='*80}")
    print(f"Batch Complete in {elapsed:.1f}s")
    print(f"  Succeeded: {succeeded}")
    print(f"  Failed:    {failed}")
    if error_details:
        print(f"\nFirst 10 errors:")
        for err in error_details[:10]:
            print(f"  {err}")
    print(f"{'='*80}")
    
    # Show database status
    with duckdb.connect(DB_PATH) as conn:
        cursor = conn.execute("""
            SELECT COUNT(*) FROM raw_narratives 
            WHERE narrative_json->>'description_type' = ?
        """, [desc_type])
        count = cursor.fetchone()[0]
        print(f"\nRaw narratives inserted: {count} ({desc_type} type)")
        print("Note: dbt models will parse, validate, and version these rows")


if __name__ == "__main__":
    # Parse arguments
    desc_type = 'brief'
    workers = 4
    limit = None
    
    for arg in sys.argv[1:]:
        if arg.startswith('--type='):
            desc_type = arg.split('=')[1]
        elif arg.startswith('--workers='):
            workers = int(arg.split('=')[1])
        elif arg.startswith('--limit='):
            limit = int(arg.split('=')[1])
    
    if desc_type not in ('brief', 'full'):
        print(f"Error: --type must be 'brief' or 'full', got '{desc_type}'")
        sys.exit(1)
    
    try:
        batch_generate_and_store(desc_type=desc_type, workers=workers, limit=limit)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
