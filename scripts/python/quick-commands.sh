#!/bin/bash
# Quick reference commands for match narrative generation

# Test with a single match (brief 5-10 word summary)
python scripts/python/generate_match_narrative.py 1000851 --type brief

# Test with a single match (full narrative)
python scripts/python/generate_match_narrative.py 1000851 --type full

# See the LLM prompt
python scripts/python/generate_match_narrative.py 1000851 --type full --prompt

# Test batch with first 100 matches (no database storage)
python scripts/python/generate_match_narrative.py --batch --type brief --limit 100 --workers 4

# Generate brief summaries for all ~20k matches and store in DB
python scripts/python/batch_match_descriptions.py --type brief --workers 4

# Generate full narratives for all matches (will take hours)
python scripts/python/batch_match_descriptions.py --type full --workers 4

# Test batch with database storage first
python scripts/python/batch_match_descriptions.py --type brief --limit 100 --workers 4

# Query database
duckdb 'SELECT COUNT(*) FROM match_narratives WHERE description_type = "brief"'
duckdb 'SELECT * FROM match_narratives WHERE match_id = "1000851"'
