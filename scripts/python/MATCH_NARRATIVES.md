# Match Narrative Generation

Scripts for generating natural language descriptions of cricket matches using Gemini LLM (gemini-2.5-flash-lite). Results stored as raw JSON and processed by dbt.

## Overview

Three Python scripts + dbt models:

1. **`generate_match_narrative.py`** - Core engine: single match generation with flexible output types
2. **`batch_match_descriptions.py`** - Local threading: batch processing (good for <1000 matches)
3. **`batch_match_descriptions_api.py`** - Google Batch API: large-scale processing (recommended for 1000+ matches like your 20k)
4. **dbt models** - Parse, validate, version, and enrich narratives

## Quick Start

### Setup

```bash
export GEMINI_API_KEY="your-api-key"
```

### Single Match Examples

```bash
# Generate brief summary (5-10 words)
python scripts/python/generate_match_narrative.py 1000851 --type brief

# Generate full narrative (2-3 paragraphs)
python scripts/python/generate_match_narrative.py 1000851 --type full

# View the prompt being sent to Gemini
python scripts/python/generate_match_narrative.py 1000851 --type full --prompt

# Test prompt without making API call
python scripts/python/generate_match_narrative.py 1000851 --type full --prompt-only
```

### Batch Processing - Which Approach?

| Scenario | Command | Cost | Speed |
|----------|---------|------|-------|
| **Testing (10-100 matches)** | `batch_match_descriptions.py --type=brief --limit=100` | 100% API | Real-time |
| **Production (1000+ matches)** | `batch_match_descriptions_api.py prepare/submit/download/store` | **50% API** | ~24h turnaround |

**RECOMMENDATION**: Use the Batch API for your 20k matches (50% cost savings, higher quotas).

---

## Batch Processing: Detailed Guide

### Option 1: Local Threading (batch_match_descriptions.py)

**Best for:** <1000 matches, immediate results needed, testing

```bash
# Test with 100 matches
python scripts/python/batch_match_descriptions.py --type=brief --limit=100 --workers=4

# Full run (all matches)
python scripts/python/batch_match_descriptions.py --type=brief --workers=4

# Using more workers for faster processing (if quota allows)
python scripts/python/batch_match_descriptions.py --type=brief --workers=8
```

**How it works:**
- Creates `raw_narratives` table (if needed)
- Fetches all match IDs from database
- Spawns worker threads (default: 4)
- Each worker: generates narrative → creates JSON blob → inserts into raw table
- Real-time progress with ETA
- dbt transforms and versions the data

**Concurrency Strategy:**
- 4 workers: Safe for most API quotas (~15-60 requests/min)
- 8 workers: For higher quota limits
- 1 worker: For debugging or restricted quotas

### Option 2: Google Batch API (batch_match_descriptions_api.py) - RECOMMENDED ✓

**Best for:** 1000+ matches, non-urgent, want 50% cost savings, processing 20k matches

**5-Step Workflow:**

```bash
# Step 1: Prepare batch file (creates JSONL with all requests)
python scripts/python/batch_match_descriptions_api.py prepare --type=brief --output=requests.jsonl

# Step 2: Submit to Google's batch processing
python scripts/python/batch_match_descriptions_api.py submit --input=requests.jsonl --job-name=cricket-prod

# Step 3: Check status (run periodically while waiting)
python scripts/python/batch_match_descriptions_api.py status --job-name=cricket-prod

# Step 4: Download results (once DONE - typically 4-24 hours)
python scripts/python/batch_match_descriptions_api.py download --job-name=cricket-prod --output=results.jsonl

# Step 5: Store in database (as raw JSON)**
python scripts/python/batch_match_descriptions_api.py store --input=results.jsonl --type=brief
```

**Then run dbt to transform:**

```bash
dbt run --select stg_cricket__narratives int_cricket__narratives
```

**Characteristics:**
- **Cost**: 50% of standard API pricing
- **Quotas**: Much higher limits than standard API
- **Turnaround**: 4-24 hours (mostly waiting, not dependent on your machine)
- **Resume**: Automatic retry on failures
- **Infrastructure**: Runs on Google's servers, not your machine

**Full 20k Batch Example:**

```bash
python scripts/python/batch_match_descriptions_api.py prepare --type=brief --output=all_matches.jsonl
python scripts/python/batch_match_descriptions_api.py submit --input=all_matches.jsonl --job-name=cricket-full
# Wait...
python scripts/python/batch_match_descriptions_api.py download --job-name=cricket-full --output=results.jsonl
python scripts/python/batch_match_descriptions_api.py store --input=results.jsonl --type=brief
```

**Total time**: ~4-8 hours (mostly waiting) + ~5 min dbt run
**Your involvement**: 5 commands + occasional status checks + 1 dbt command

---

## Architecture: Python + dbt Pipeline

```
┌──────────────────────────────────────────────────────────────┐
│           Python: Generate Narratives (Gemini API)            │
│  generate_match_narrative.py / batch_*.py scripts             │
│                                                               │
│  Insert raw JSON:                                            │
│  {match_id, description_type, description, generated_at, ...}│
└──────────────────────────────────────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────────┐
│            DuckDB: raw_narratives (raw JSON)                  │
│  Single table: raw_narrative_id UUID, narrative_json JSON     │
└──────────────────────────────────────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────────┐
│     dbt: Parse & Validate (stg_cricket__narratives)           │
│  - Extract JSON fields                                        │
│  - Validate description_type IN ('brief', 'full')             │
│  - Mark most recent per match_id + type                       │
│  - Returns: typed columns, ready for transformation           │
└──────────────────────────────────────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────────┐
│     dbt: Version & Enrich (int_cricket__narratives)           │
│  - Row number by match_id + description_type                  │
│  - is_most_recent boolean flag (v1 = most recent)             │
│  - Version history for audit trail                            │
│  - Join-ready for facts tables                                │
└──────────────────────────────────────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────────┐
│     Query: Match Narratives for Analysis                      │
│  SELECT * FROM int_cricket__narratives                        │
│  WHERE is_most_recent AND description_type = 'brief'          │
└──────────────────────────────────────────────────────────────┘
```

**Key benefits of this architecture:**

- **Separation of concerns**: Python handles generation, dbt handles transformation
- **Schema flexibility**: Add new fields in dbt without Python changes
- **Versioning**: Full audit trail of all generations
- **Testability**: dbt tests for data quality validation
- **Git versioning**: Schema evolution in `schema.yml`
- **Consistency**: Matches your existing staging → intermediate → marts pattern

| Type | Max Tokens | Temperature | Example |
|------|-----------|------------|---------|
| `brief` | 50 | 0.5 | "England won by 5 runs" |
| `full` | 1500 | 0.7 | Multi-paragraph detailed narrative |

**Extensibility**: To add a new type:
1. Add config to `DescriptionConfig.get_config(type_name)` in Python
2. Extend `format_match_prompt()` with template
3. Update `schema.yml` accepted_values for description_type
4. All scripts and dbt models automatically support it

---

## Database Schema

### Raw Layer (Python writes)

```sql
CREATE TABLE raw_narratives (
    raw_narrative_id UUID DEFAULT gen_random_uuid(),
    narrative_json JSON,    -- {match_id, description_type, description, generated_at, model}
    loaded_at TIMESTAMP DEFAULT now()
)
```

Example JSON blob:
```json
{
  "match_id": "1000851",
  "description_type": "brief",
  "description": "England won by 5 runs",
  "generated_at": "2026-01-16T10:00:00",
  "model": "gemini-2.0-flash-lite"
}
```

### Staging Layer (dbt parses)

Model: `stg_cricket__narratives` - Flattens JSON, validates, marks most recent

Columns: `raw_narrative_id`, `match_id`, `description_type`, `description`, `generated_at`, `model`, `loaded_at`, `row_num`

### Intermediate Layer (dbt versions)

Model: `int_cricket__narratives` - Adds versioning, `is_most_recent` flag

Columns: All from staging + `version_num`, `is_most_recent`, `created_row_id`

**Query current descriptions:**
```sql
SELECT * FROM int_cricket__narratives 
WHERE is_most_recent AND description_type = 'brief'
```

**Query version history for a match:**
```sql
SELECT * FROM int_cricket__narratives 
WHERE match_id = '1000851' 
ORDER BY generated_at DESC
```

---

## Running the Full Pipeline

```bash
# 1. Generate (using Batch API)
python scripts/python/batch_match_descriptions_api.py prepare --type=brief --output=requests.jsonl
python scripts/python/batch_match_descriptions_api.py submit --input=requests.jsonl --job-name=prod
# Wait for completion...
python scripts/python/batch_match_descriptions_api.py download --job-name=prod --output=results.jsonl
python scripts/python/batch_match_descriptions_api.py store --input=results.jsonl --type=brief

# 2. Transform (dbt)
dbt run --select stg_cricket__narratives int_cricket__narratives
dbt test --select stg_cricket__narratives int_cricket__narratives

# 3. Query results
duckdb 'SELECT COUNT(*) FROM int_cricket__narratives WHERE is_most_recent'
```

---

## Troubleshooting

**"GEMINI_API_KEY environment variable not set"**
```bash
export GEMINI_API_KEY="your-key"
```

**Rate limiting / quota exceeded (threading only)**
- Reduce `--workers` (try 2 or 1)
- Pause and retry later

**Batch API says "processing" for >24 hours**
- Normal during high demand
- Check status periodically: `batch_match_descriptions_api.py status --job-name=...`
- Files are auto-retried on failure

**dbt test failures for narratives**
- Check raw table: `SELECT COUNT(*) FROM raw_narratives`
- Check staging: `SELECT COUNT(*) FROM stg_cricket__narratives WHERE row_num = 1`
- Review error messages in `dbt test` output

**Some matches missing from int_cricket__narratives**
- Check if narratives are in raw table
- Verify description_type is 'brief' or 'full' (not typos)
- Confirm generated_at is valid timestamp

---

## Performance Notes

- **Gemini Flash Lite**: Cost-effective, fast for batching
- **API Quotas**: Threading limited by per-minute limits; Batch API by daily limits
- **Cost Example**: 20k matches with Batch API ≈ $50-100 (50% vs threading)
- **Network**: Batch API independent of your connection; threading requires stable network
- **dbt Run**: Parsing 20k JSON narratives in dbt typically <5 minutes
- **Storage**: 20k narratives ≈ 50-100 MB raw JSON

---

## Migration: From Threading to Batch API

If you've already processed some matches with threading:

```bash
# Both approaches write to the same raw_narratives table
# Existing rows get processed by dbt

# Start Batch API for remaining matches
python scripts/python/batch_match_descriptions_api.py prepare --type=brief --output=remaining.jsonl
python scripts/python/batch_match_descriptions_api.py submit --input=remaining.jsonl --job-name=phase-2
# ... wait ...
python scripts/python/batch_match_descriptions_api.py download --job-name=phase-2 --output=results.jsonl
python scripts/python/batch_match_descriptions_api.py store --input=results.jsonl --type=brief

# Run dbt to consolidate
dbt run --select stg_cricket__narratives int_cricket__narratives

# Result: is_most_recent flag auto-updates in intermediate layer
```
