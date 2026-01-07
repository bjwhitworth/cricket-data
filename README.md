# Cricket Data Pipeline

A comprehensive dbt + DuckDB data pipeline for analyzing cricket match data from JSON sources.

## Overview

This project builds a DuckDB implementation of **[Cricsheet](https://cricsheet.org)** cricket match data. It ingests cricket match data (metadata, innings, deliveries, wickets, etc.) from JSON files, transforms it through layered models (raw → intermediate → staging → marts), and exposes fact/dimension tables for analytics.

## Data Source

Data is sourced from **[Cricsheet](https://cricsheet.org)**, an open-source collection of cricket match data. All JSON match files are available in a single download at [https://cricsheet.org/downloads/all_json.zip](https://cricsheet.org/downloads/all_json.zip).

**Note**: The raw JSON data (`data/raw/all_json/`) and generated DuckDB databases (`data/duckdb/`) are **not committed to Git**. They are gitignored to avoid bloating the repository. To use this pipeline:
1. Download and extract match JSON files from [cricsheet.org/downloads/all_json.zip](https://cricsheet.org/downloads/all_json.zip), or use the `check_cricsheet_updates.py` script
2. Place them in `data/raw/all_json/`
3. Run `dbt run` to generate the DuckDB database and materialized views

## Project Structure

```
cricket-data/
├── dbt_project.yml           # dbt configuration
├── models/
│   ├── staging/              # Cleaned views (stg_cricket_*)
│   ├── intermediate/         # Flattened arrays (int_cricket_*)
│   └── marts/                # Fact & dimension tables (fct_*, dim_*)
├── scripts/
│   ├── python/               # Python utility scripts
│   │   ├── generate_match_narrative.py  # LLM-powered match summaries
│   │   └── test_gemini_connection.py    # Gemini API connectivity test
│   └── r/                    # R data analysis scripts
├── data/
│   ├── raw/all_json/         # Cricket match JSON files
│   ├── duckdb/               # DuckDB databases (gitignored)
│   └── scratch/              # Temporary analysis files
├── macros/                   # dbt macros & utilities
├── tests/                    # dbt test definitions
├── pyproject.toml            # Python dependencies
└── renv.lock                 # R environment lockfile
```

## Setup

### Prerequisites
- Python 3.13+
- R 4.3+ (optional, for analysis scripts)
- dbt 1.10+ (installed via pyproject.toml)

### Installation

1. Clone the repo and install dependencies:
```bash
uv sync              # Python dependencies
renv::restore()      # R dependencies (in R console, if using R)
```

2. Configure your shell for API access:
```bash
export GEMINI_API_KEY='your-key-here'
```

### Running the Pipeline

Compile and run dbt models:
```bash
dbt compile
dbt run
dbt test
```

Or selectively:
```bash
dbt run --select int_cricket__deliveries_flattened
dbt run --select tag:batting
```

## Data Model

### Raw Ingestion (`stg_cricket__raw_json`)
- Reads JSON files via DuckDB's `read_json_auto()`
- Preserves all metadata: `meta`, `info`, `innings`
- Includes JSON clones (`*_json`) for optional fields

### Intermediate Flattening
- **`int_cricket__matches_flattened`**: Match metadata (teams, toss, result, officials, players, registry)
- **`int_cricket__innings_flattened`**: Innings-level data (team, super_over flag, innings JSON payload)
- **`int_cricket__overs_flattened`**: Over-level data (over number, deliveries)
- **`int_cricket__deliveries_flattened`**: Ball-by-ball data (batter, bowler, runs, extras, wickets)

### Staging Views
- **`stg_cricket__matches`**: Cleaned match summaries
- **`stg_cricket__innings`**: Innings with aggregated stats (runs, wickets, overs)
- **`stg_cricket__deliveries`**: Individual deliveries with flattened extras/wickets

### Marts (Analytics)
- **`fct_batter_innings`**: Batting performance per innings (runs, balls, fours, sixes, strike rate)
- **`fct_bowler_innings`**: Bowling performance per innings (wickets, runs conceded, economy)
- **`dim_players`**: Player registry with match participation

## Data Quality

- **Union by name**: DuckDB's `union_by_name=true` ensures optional JSON fields are preserved across files
- **Name cleaning**: Quotes stripped from player/team names at the source
- **Nullable fields**: Optional fields (super_over, powerplays, eliminator, method, etc.) are coalesced to sensible defaults

## Utilities

### Check for New Data

Check Cricsheet for new match files by comparing the official `all_json.zip` against your local data:

```bash
python scripts/python/check_cricsheet_updates.py
```

The script downloads the zip file metadata (without saving the full archive), compares the contents against your local `data/raw/all_json/` directory, and reports what's new or removed.

Download new files automatically:

```bash
python scripts/python/check_cricsheet_updates.py --download
```

Limit downloads (useful for testing or large updates):

```bash
python scripts/python/check_cricsheet_updates.py --download --limit 50
```

After downloading, run `dbt run` to process the new data.

### Generate Match Narratives

Use Google Gemini 2.5 Flash Lite to generate natural language summaries:

```bash
python scripts/python/generate_match_narrative.py 1485939
```

Show the prompt used for generation:

```bash
python scripts/python/generate_match_narrative.py 1485939 --prompt
```

Outputs a 3-4 paragraph narrative covering match result, key performances, and pivotal moments.

### Test Gemini API

Verify Gemini connectivity and quota status:

```bash
python scripts/python/test_gemini_connection.py
```

## Testing

### Run Unit Tests

Run all Python tests:
```bash
uv run pytest
```

Run specific test file:
```bash
uv run pytest tests/test_generate_match_narrative.py
```

Run with verbose output:
```bash
uv run pytest -v
```

Run with coverage report:
```bash
uv run pytest --cov=scripts/python --cov-report=html
open htmlcov/index.html  # View coverage report
```

### dbt Tests

Run dbt data quality tests:
```bash
dbt test
```

Run tests for specific models:
```bash
dbt test --select stg_cricket__deliveries
```

## Development Notes

- **Local DB**: DuckDB files in `data/duckdb/` are gitignored (rebuild with `dbt run`)
- **Python venv**: Use `uv` for dependency management; virtualenv is gitignored
- **R integration**: R scripts live in `scripts/r/`; use `renv` for reproducibility
- **Free Tier Limits**: Gemini API has per-minute and per-day quotas; use Flash Lite for lower usage

## References

- [dbt Documentation](https://docs.getdbt.com/)
- [DuckDB Documentation](https://duckdb.org/)
- [Google Gemini API](https://ai.google.dev/)
- Cricket JSON schema: See [CricketData](https://cricketdata.org/) or ESPNcricinfo

## License
MIT License. See LICENSE file for details.

