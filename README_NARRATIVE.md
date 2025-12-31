# Match Narrative Generator

Generate natural language narratives of cricket matches using LLM.

## Setup

1. Install dependencies:
```bash
uv sync
```

2. Set your Anthropic API key:
```bash
export ANTHROPIC_API_KEY='your-key-here'
```

3. Ensure your dbt models are materialized:
```bash
dbt run
```

## Usage

Generate a narrative for a specific match:

```bash
python scripts/python/generate_match_narrative.py 1485939
```

## What it does

The script:
1. Queries DuckDB for match metadata, innings summaries, top performances, and key wickets
2. Formats the data into a structured prompt
3. Sends it to Claude (Sonnet 3.5) for narrative generation
4. Returns a 3-4 paragraph match summary

## Customization

Edit `scripts/python/generate_match_narrative.py` to:
- Change the LLM model or provider
- Adjust the narrative length/style in the prompt
- Include additional statistics (partnerships, run rates, etc.)
- Filter/prioritize different match moments
- Generate multiple narrative styles (short tweets, detailed reports, etc.)

## Example Output

```
MATCH NARRATIVE: 1485939
================================================================================

The Scotland T20 Tri-Series encounter between Netherlands and Nepal at Titwood, 
Glasgow turned into an absolute thriller, culminating in a dramatic super over 
finish. After winning the toss, Nepal elected to field first...

[3-4 paragraphs of generated narrative]

================================================================================
```

## Future Enhancements

- Batch generation for multiple matches
- Export to markdown/HTML reports
- Integrate match context from external sources
- Fine-tune prompts based on match type (Test, ODI, T20)
- Add visualization generation alongside narrative
