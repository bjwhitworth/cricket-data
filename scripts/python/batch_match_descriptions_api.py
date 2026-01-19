#!/usr/bin/env python3
"""
Batch generate match descriptions using the official Gemini Batch API.

This uses Google's native batch processing infrastructure for large-scale generation.
Much more efficient than threading for 20k+ matches.

Directory Structure:
    batches/
    ├── requests/          # JSONL files prepared for submission
    ├── results/           # JSONL files downloaded from completed jobs
    └── metadata/          # Job tracking files (job_name.txt)

Usage:
    # Step 1: Prepare batch file (defaults to batches/requests/batch_requests_<type>.jsonl)
    python scripts/python/batch_match_descriptions_api.py prepare --type brief
    
    # Filter options for prepare command:
    #   --limit N              Process only first N matches (for testing, e.g. --limit=5)
    #   --match-ids-file FILE  Path to file with newline-separated match IDs
    #   --event NAME           Filter by event name (case-insensitive)
    #   --season SEASON        Filter by season (e.g. 2025 matches 2025/26)
    #   --start-date YYYY-MM-DD Filter by start date or later
    #   --end-date YYYY-MM-DD  Filter by end date or earlier
    #   --output FILE          Custom output path (default: batches/requests/batch_requests_<type>.jsonl)
    
    # Examples:
    python scripts/python/batch_match_descriptions_api.py prepare --type brief --limit=5
    python scripts/python/batch_match_descriptions_api.py prepare --type brief --event "T20" --limit=100
    python scripts/python/batch_match_descriptions_api.py prepare --type brief --season 2024 --start-date 2024-01-01 --end-date 2024-06-30
    python scripts/python/batch_match_descriptions_api.py prepare --type brief --match-ids-file my_match_ids.txt
    
    # Step 2: Submit batch job (defaults to reading from batches/requests/)
    python scripts/python/batch_match_descriptions_api.py submit --input batches/requests/batch_requests_brief.jsonl --job-name cricket-brief
    
    # Step 3: Check status
    python scripts/python/batch_match_descriptions_api.py status --job-name cricket-brief
    
    # Step 4: Download results when complete (defaults to batches/results/)
    python scripts/python/batch_match_descriptions_api.py download --job-name cricket-brief
    
    # Step 5: Store in database
    python scripts/python/batch_match_descriptions_api.py store --input batches/results/batch_results_cricket-brief.jsonl --type brief
"""

import sys
import os
import json
import duckdb
import google.genai as genai
from generate_match_narrative import (
    fetch_match_data,
    format_match_prompt,
    DescriptionConfig,
    create_narrative_json_blob,
    store_narrative_json
)
from datetime import datetime
from typing import Literal
import time


def _parse_cli_args(arg_list: list[str]) -> dict:
    """Parse CLI args supporting both --key=value and --key value forms."""
    args = {}
    i = 0
    while i < len(arg_list):
        arg = arg_list[i]
        if arg.startswith('--'):
            keyval = arg.lstrip('-')
            if '=' in keyval:
                key, value = keyval.split('=', 1)
                args[key] = value
            else:
                # Look ahead for a value; otherwise treat as boolean flag
                if i + 1 < len(arg_list) and not arg_list[i + 1].startswith('--'):
                    args[keyval] = arg_list[i + 1]
                    i += 1
                else:
                    args[keyval] = True
        i += 1
    return args

# Database path configuration with environment variable override
DB_PATH = os.getenv('CRICKET_DB_PATH', 'data/duckdb/dev.duckdb')

# Batch files directory configuration with environment variable override
BATCH_DIR = os.getenv('CRICKET_BATCH_DIR', 'batches')

# Ensure batch subdirectories exist
for subdir in ['requests', 'results', 'metadata']:
    os.makedirs(os.path.join(BATCH_DIR, subdir), exist_ok=True)


def get_filtered_match_ids(match_ids_file: str = None, event_name: str = None, 
                          season: str = None, start_date: str = None, 
                          end_date: str = None, limit: int = None) -> list[str]:
    """
    Get match IDs with optional filtering.
    
    Args:
        match_ids_file: Path to file with newline-separated match IDs
        event_name: Filter by event name (case-insensitive partial match)
        season: Filter by season
        start_date: Filter by start date (YYYY-MM-DD or later)
        end_date: Filter by end date (YYYY-MM-DD or earlier)
        limit: Limit number of results
    
    Returns:
        List of match IDs
    """
    if match_ids_file:
        # Load from file
        with open(match_ids_file, 'r') as f:
            match_ids = [line.strip() for line in f if line.strip()]
        print(f"Loaded {len(match_ids)} match IDs from {match_ids_file}")
        return match_ids[:limit] if limit else match_ids
    
    # Query database with filters
    with duckdb.connect(DB_PATH, read_only=True) as conn:
        query = "SELECT DISTINCT match_id FROM stg_cricket__matches WHERE 1=1"
        params = []
        
        if event_name:
            query += " AND event_name ILIKE ?"
            params.append(f"%{event_name}%")
        
        if season:
            query += " AND season ilike ?"
            params.append(f"%{season}%")
        
        if start_date:
            query += " AND match_start_date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND match_start_date <= ?"
            params.append(end_date)
        
        query += " ORDER BY match_id"
        
        if limit:
            query += f" LIMIT {limit}"
        
        print(query, params)  # Debug print to verify query and params

        cursor = conn.execute(query, params)
        match_ids = [row[0] for row in cursor.fetchall()]

        print(match_ids)  # Debug print to verify fetched match IDs
    
    return match_ids


def prepare_batch_file(desc_type: Literal['brief', 'full'] = 'brief', 
                       output_file: str = None,
                       match_ids_file: str = None,
                       event_name: str = None,
                       season: str = None,
                       start_date: str = None,
                       end_date: str = None,
                       limit: int = None) -> None:
    """
    Prepare JSONL file with batch requests for Gemini Batch API.
    
    Format per docs: {"key": "request-1", "request": {"contents": [{"parts": [{"text": "..."}]}]}}
    
    Args:
        desc_type: Type of description to generate
        output_file: Path to output JSONL file (defaults to batches/requests/batch_requests_<type>.jsonl)
        match_ids_file: Optional path to file with match IDs (one per line)
        event_name: Optional event name filter (case-insensitive)
        season: Optional season filter
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        limit: Optional limit on number of matches (for testing)
    """
    if output_file is None:
        output_file = os.path.join(BATCH_DIR, 'requests', f'batch_requests_{desc_type}.jsonl')
    
    match_ids = get_filtered_match_ids(
        match_ids_file=match_ids_file,
        event_name=event_name,
        season=season,
        start_date=start_date,
        end_date=end_date,
        limit=limit
    )
    
    config = DescriptionConfig.get_config(desc_type)
    
    print(f"Preparing batch file for {len(match_ids)} matches...")
    print(f"Output: {output_file}")
    print(f"Description type: {desc_type}\n")
    
    with open(output_file, 'w') as f:
        for idx, match_id in enumerate(match_ids, 1):
            try:
                data = fetch_match_data(match_id)
                prompt = format_match_prompt(data, config)
                
                # Format according to Gemini Batch API spec
                request = {
                    "key": match_id,  # User-defined key to match results
                    "request": {
                        "contents": [{
                            "parts": [{"text": prompt}],
                            "role": "user"
                        }],
                        "generation_config": {
                            "maxOutputTokens": config.max_tokens,
                            "temperature": config.temperature,
                        }
                    }
                }
                
                f.write(json.dumps(request) + '\n')
                
                if idx % 100 == 0:
                    print(f"  Prepared {idx}/{len(match_ids)} matches...")
                    
            except Exception as e:
                print(f"  Warning: Failed to prepare {match_id}: {e}")
    
    print(f"\n✓ Batch file created: {output_file}")
    print(f"  Ready to submit to Gemini Batch API")


def submit_batch_job(input_file: str, job_name: str, api_key: str = None) -> str:
    """
    Submit batch job to Gemini Batch API.
    
    Args:
        input_file: Path to JSONL file with requests
        job_name: Display name for this batch job
        api_key: Optional API key
        
    Returns:
        Job ID for tracking
    """
    api_key = api_key or os.getenv('GEMINI_API_KEY')
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set")
    
    from google.genai import types
    client = genai.Client(api_key=api_key)
    
    print(f"Submitting batch job: {job_name}")
    print(f"Input file: {input_file}\n")
    
    # Step 1: Upload the JSONL file using File API
    print(f"Uploading batch file to File API...")
    uploaded_file = client.files.upload(
        file=input_file,
        config=types.UploadFileConfig(
            display_name=f'{job_name}-requests',
            mime_type='jsonl'
        )
    )
    print(f"✓ Uploaded: {uploaded_file.name}")
    
    # Step 2: Create batch job with the uploaded file
    print(f"\nCreating batch job...")
    batch_job = client.batches.create(
        model='gemini-2.5-flash-lite',
        src=uploaded_file.name,
        config={
            'display_name': job_name
        },
    )
    
    job_id = batch_job.name
    print(f"\n✓ Batch job submitted successfully!")
    print(f"  Job ID: {job_id}")
    print(f"  Status: {batch_job.state.name if hasattr(batch_job.state, 'name') else batch_job.state}")
    print(f"\nUse this command to check status:")
    print(f"  python scripts/python/batch_match_descriptions_api.py status --job-name {job_name}")
    
    # Save job ID for later reference
    metadata_file = os.path.join(BATCH_DIR, 'metadata', f'{job_name}.txt')
    with open(metadata_file, 'w') as f:
        f.write(job_id)
    print(f"  Job ID saved to: {metadata_file}")
    
    return job_id


def check_batch_status(job_id: str = None, job_name: str = None, api_key: str = None) -> dict:
    """
    Check status of a batch job.
    
    Args:
        job_id: Direct job ID
        job_name: Job name (will load ID from file)
        api_key: Optional API key
        
    Returns:
        Status dict
    """
    if not job_id and job_name:
        try:
            metadata_file = os.path.join(BATCH_DIR, 'metadata', f'{job_name}.txt')
            with open(metadata_file, 'r') as f:
                job_id = f.read().strip()
        except FileNotFoundError:
            raise ValueError(f"No saved job ID found for {job_name} in {os.path.join(BATCH_DIR, 'metadata')}")
    
    if not job_id:
        raise ValueError("Must provide either job_id or job_name")
    
    api_key = api_key or os.getenv('GEMINI_API_KEY')
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set")
    
    client = genai.Client(api_key=api_key)
    
    batch_job = client.batches.get(name=job_id)
    
    state = batch_job.state.name if hasattr(batch_job.state, 'name') else str(batch_job.state)
    
    print(f"Batch Job Status: {job_id}")
    print(f"  State: {state}")
    print(f"  Created: {batch_job.create_time if hasattr(batch_job, 'create_time') else 'N/A'}")
    
    if hasattr(batch_job, 'completion_time') and batch_job.completion_time:
        print(f"  Completed: {batch_job.completion_time}")
    
    if hasattr(batch_job, 'request_counts'):
        print(f"  Request Counts: {batch_job.request_counts}")
    
    if hasattr(batch_job, 'error') and batch_job.error:
        print(f"  Error: {batch_job.error}")
    
    completed_states = {'JOB_STATE_SUCCEEDED', 'JOB_STATE_COMPLETED', 'JOB_STATE_FAILED', 
                       'JOB_STATE_CANCELLED', 'JOB_STATE_EXPIRED'}
    
    return {
        'job_id': job_id,
        'state': state,
        'completed': state in completed_states
    }


def download_batch_results(job_id: str = None, job_name: str = None, 
                           output_file: str = None,
                           api_key: str = None) -> None:
    """
    Download results from completed batch job.
    
    Args:
        job_id: Direct job ID
        job_name: Job name (will load ID from file)
        output_file: Path to save results (defaults to batches/results/batch_results_<job_name>.jsonl)
        api_key: Optional API key
    """
    if not job_id and job_name:
        try:
            metadata_file = os.path.join(BATCH_DIR, 'metadata', f'{job_name}.txt')
            with open(metadata_file, 'r') as f:
                job_id = f.read().strip()
        except FileNotFoundError:
            raise ValueError(f"No saved job ID found for {job_name} in {os.path.join(BATCH_DIR, 'metadata')}")
    
    if output_file is None and job_name:
        output_file = os.path.join(BATCH_DIR, 'results', f'batch_results_{job_name}.jsonl')
    elif output_file is None:
        output_file = os.path.join(BATCH_DIR, 'results', 'batch_results.jsonl')
    
    api_key = api_key or os.getenv('GEMINI_API_KEY')
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set")
    
    client = genai.Client(api_key=api_key)
    
    print(f"Downloading results from job: {job_id}")
    
    batch_job = client.batches.get(name=job_id)
    
    state = batch_job.state.name if hasattr(batch_job.state, 'name') else str(batch_job.state)
    
    if state not in ('JOB_STATE_SUCCEEDED', 'JOB_STATE_COMPLETED'):
        print(f"⚠️  Job not complete yet. Current state: {state}")
        return
    
    # Results are in a file (for file-based batches)
    if hasattr(batch_job.dest, 'file_name') and batch_job.dest.file_name:
        result_file_name = batch_job.dest.file_name
        print(f"  Results file: {result_file_name}")
        print(f"  Downloading...")
        
        file_content_bytes = client.files.download(file=result_file_name)
        
        with open(output_file, 'wb') as f:
            f.write(file_content_bytes)
        
        print(f"✓ Results downloaded: {output_file}")
    else:
        print(f"✗ No results file found in batch job")
        if hasattr(batch_job, 'error') and batch_job.error:
            print(f"  Error: {batch_job.error}")


def store_batch_results(input_file: str, desc_type: str, 
                       db_path: str = None) -> None:
    """
    Parse batch results JSONL and store as raw JSON in database.
    
    Each line format: {"key": "match_id", "response": {"candidates": [{"content": {"parts": [{"text": "..."}]}}]}}
    dbt models will handle parsing, validation, and versioning.
    
    Args:
        input_file: Path to JSONL file with results from Gemini
        desc_type: Description type ('brief' or 'full')
        db_path: Path to DuckDB database (defaults to DB_PATH constant)
    """
    db_path = db_path or DB_PATH
    print(f"Storing batch results from: {input_file}")
    print(f"Description type: {desc_type}\n")
    
    with duckdb.connect(db_path) as conn:
        # Create table if needed
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_narratives (
                raw_narrative_id UUID DEFAULT gen_random_uuid(),
                narrative_json JSON,
                loaded_at TIMESTAMP DEFAULT now()
            )
        """)
        
        success_count = 0
        error_count = 0
        
        with open(input_file, 'r') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    result = json.loads(line)
                    match_id = result.get('key')
                    
                    if not match_id:
                        print(f"  Warning: Line {line_num} missing 'key' field")
                        error_count += 1
                        continue
                    
                    # Extract generated text from Gemini response structure
                    description = None
                    
                    if 'response' in result:
                        response = result['response']
                        if 'candidates' in response and len(response['candidates']) > 0:
                            candidate = response['candidates'][0]
                            if 'content' in candidate and 'parts' in candidate['content']:
                                parts = candidate['content']['parts']
                                if len(parts) > 0 and 'text' in parts[0]:
                                    description = parts[0]['text']
                    
                    if not description:
                        print(f"  Warning: Could not extract text from result for {match_id}")
                        error_count += 1
                        continue
                    
                    # Create and store JSON blob
                    narrative_json = create_narrative_json_blob(match_id, desc_type, description, source='batch_api')
                    store_narrative_json(narrative_json, db_path)
                    
                    success_count += 1
                    
                    if success_count % 100 == 0:
                        print(f"  Stored {success_count} results...")
                        
                except Exception as e:
                    print(f"  Error on line {line_num}: {e}")
                    error_count += 1
        
        print(f"\n✓ Storage complete")
        print(f"  Succeeded: {success_count}")
        print(f"  Errors: {error_count}")
        
        # Show final count
        cursor = conn.execute(
            "SELECT COUNT(*) FROM raw_narratives WHERE narrative_json->>'description_type' = ?", 
            [desc_type]
        )
        total = cursor.fetchone()[0]
        print(f"  Total {desc_type} raw narratives in DB: {total}")
        print("  Note: dbt models will parse, validate, and version these rows")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    command = sys.argv[1]
    
    # Parse common arguments (supports --key=value and --key value)
    args = _parse_cli_args(sys.argv[2:])
    
    try:
        if command == 'prepare':
            prepare_batch_file(
                desc_type=args.get('type', 'brief'),
                output_file=args.get('output'),
                match_ids_file=args.get('match-ids-file'),
                event_name=args.get('event'),
                season=args.get('season'),
                start_date=args.get('start-date'),
                end_date=args.get('end-date'),
                limit=int(args['limit']) if 'limit' in args else None
            )
        
        elif command == 'submit':
            if 'input' not in args or 'job-name' not in args:
                print("Error: --input and --job-name required")
                sys.exit(1)
            submit_batch_job(
                input_file=args['input'],
                job_name=args['job-name']
            )
        
        elif command == 'status':
            check_batch_status(
                job_id=args.get('job-id'),
                job_name=args.get('job-name')
            )
        
        elif command == 'download':
            download_batch_results(
                job_id=args.get('job-id'),
                job_name=args.get('job-name'),
                output_file=args.get('output')
            )
        
        elif command == 'store':
            if 'input' not in args or 'type' not in args:
                print("Error: --input and --type required")
                sys.exit(1)
            store_batch_results(
                input_file=args['input'],
                desc_type=args['type']
            )
        
        else:
            print(f"Unknown command: {command}")
            print(__doc__)
            sys.exit(1)
            
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
