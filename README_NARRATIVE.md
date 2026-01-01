# Match Narrative Generator

Generate natural language narratives of cricket matches using Google Gemini LLM.

## Setup

1. Install dependencies:
```bash
uv sync
```

2. Set your Gemini API key:
```bash
export GEMINI_API_KEY='your-key-here'
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

View the prompt used for generation:

```bash
python scripts/python/generate_match_narrative.py 1485939 --prompt
```

Alternative prompt flags:
- `--prompt=true` or `prompt=true` - Show prompt before generating
- `--prompt` or `-p` - Short form

## What it does

The script:
1. Queries DuckDB for match metadata, innings summaries, top batting/bowling performances, and key wickets
2. Formats the data into a structured prompt with:
   - Match details (event, venue, teams, toss, result)
   - Innings summaries with runs/wickets
   - Top 6 batters with strike rates
   - Top 6 bowlers with economy rates
   - Key dismissals with fielder information
3. Sends the prompt to Google Gemini 2.5 Flash Lite for narrative generation
4. Returns a 3-4 paragraph match summary

## Data Sources

The generator queries the following dbt models:
- `stg_cricket__matches` - Match-level metadata
- `stg_cricket__innings` - Innings summaries
- `stg_cricket__deliveries` - Ball-by-ball data (for aggregating top performers and wickets)

## Customization

Edit `scripts/python/generate_match_narrative.py` to:
- Change the LLM model in the `generate_narrative()` function
- Adjust the narrative length/style in the prompt template (`format_match_prompt()`)
- Include additional statistics (partnerships, run rates, powerplay performance, etc.)
- Filter/prioritize different match moments
- Generate multiple narrative styles (short tweets, detailed reports, etc.)
- Change temperature/token limits in the `GenerateContentConfig`

## Testing

Unit tests are available in `tests/test_generate_match_narrative.py`. Run them with:

```bash
uv run pytest tests/test_generate_match_narrative.py -v
```

Tests cover:
- Row-to-dictionary conversion
- Prompt formatting with various match scenarios
- Data fetching from DuckDB
- Narrative generation with Gemini API

## Example Output

```bash
python scripts/python/generate_match_narrative.py 1485939 --prompt=true
```

```
================================================================================
PROMPT USED:
================================================================================

Write a short, punchy, pithy narrative of this cricket match in two or three paragraphs.  Don't be flowery; be direct, briefing-like. Start with the most important information. Pay attention to the match structure, including innings number for batting sequence. Focus on the key moments, standout performances, and the flow of the game. Pick out the important turning points of the game.

Match Details:
- Event: Scotland T20 Tri-Series
- Venue: Titwood, Glasgow, Glasgow
- Date: 2025-06-16
- Teams: Netherlands vs Nepal
- Toss: Nepal won and chose to field
- Result: tie_then_super_over(s)
- Winner: Netherlands
- Margin: Super over (3 rounds)
- Player(s) of the Match: ['ZB Lion-Cachet']

Innings Summaries:

Innings 1: Netherlands scored 152/7 in 20 overs

Innings 2: Nepal scored 152/8 in 20 overs

Innings 3 (Super Over): Nepal scored 19/1 in 1 overs

Innings 4 (Super Over): Netherlands scored 19/0 in 1 overs

Innings 5 (Super Over): Netherlands scored 17/1 in 1 overs

Innings 6 (Super Over): Nepal scored 17/0 in 1 overs

Innings 7 (Super Over): Nepal scored 0/2 in 1 overs

Innings 8 (Super Over): Netherlands scored 6/0 in 1 overs

Top Batting Performances:
- Innings 1: AT Nidamanuru - 35 runs (37 balls, 0 fours, 1 sixes, SR: 94.6)
- Innings 1: Vikramjit Singh - 30 runs (29 balls, 0 fours, 2 sixes, SR: 103.4)
- Innings 1: Saqib Zulfiqar - 25 runs (14 balls, 1 fours, 2 sixes, SR: 178.6)
- Innings 1: M Levitt - 20 runs (17 balls, 1 fours, 1 sixes, SR: 117.6)
- Innings 2: RK Paudel - 48 runs (35 balls, 3 fours, 2 sixes, SR: 137.1)
- Innings 2: K Bhurtel - 34 runs (28 balls, 5 fours, 0 sixes, SR: 121.4)

Top Bowling Performances:
- Innings 1: S Lamichhane - 3/18 (4.0 overs)
- Innings 1: NK Yadav - 2/20 (2.5 overs)
- Innings 1: LN Rajbanshi - 1/22 (4.0 overs)
- Innings 1: K Bhurtel - 1/25 (2.1 overs)
- Innings 2: DT Doram - 3/14 (4.0 overs)
- Innings 2: Vikramjit Singh - 2/30 (4.0 overs)

Key Wickets: 19 total dismissals
- Over 3.1: MP O'Dowd caught b LN Rajbanshi (c Karan KC)
- Over 6.2: M Levitt bowled b S Lamichhane
- Over 6.6: SA Edwards caught b S Lamichhane (c Rupesh Singh)
- Over 11.3: Vikramjit Singh caught b NK Yadav (c Lokesh Bam)
- Over 12.4: NRJ Croes caught b S Lamichhane (c NK Yadav)
- Over 15.6: ZB Lion-Cachet caught b K Bhurtel (c NK Yadav)
- Over 19.3: AT Nidamanuru bowled b NK Yadav
- Over 1.1: Lokesh Bam caught b BE Fletcher (c SA Edwards)

Write the narrative now:

================================================================================

================================================================================
MATCH NARRATIVE: 1485939
================================================================================

Netherlands secured a nail-biting victory against Nepal in a tied encounter, decided only in a dramatic third round of Super Overs. Nepal won the toss and elected to field, restricting the Netherlands to 152/7 in their 20 overs, with AT Nidamanuru top-scoring with 35. Nepal responded in kind, also finishing on 152/8, thanks to RK Paudel's 48 and K Bhurtel's 34. Sandeep Lamichhane was the pick of the bowlers for Nepal in the first innings, taking 3 wickets.

The match went into a Super Over after both teams failed to separate. Nepal set a target of 19 in the first Super Over, but the Netherlands matched it. The tension escalated through two more Super Overs, with the scores tied at 17-17 in the second. In the decisive third Super Over, Nepal collapsed to 0/2, a collapse that Netherlands' ZB Lion-Cachet, who was instrumental in the Super Over, capitalized on to seal the win with a score of 6/0.

================================================================================
```

## Troubleshooting

### "Match not found" error
- Ensure the match_id exists in your DuckDB database
- Run `dbt run` to rebuild the models

### "GEMINI_API_KEY environment variable not set"
- Set your API key: `export GEMINI_API_KEY='your-key-here'`
- Verify it's set: `echo $GEMINI_API_KEY`

### Quota errors (429 RESOURCE_EXHAUSTED)
- Gemini API has free-tier quotas
- Flash Lite model has higher free limits than full models
- Consider adding rate limiting if batch processing

### Empty or incorrect narrative
- Run with `--prompt` flag to inspect the data being sent
- Check that dbt models are materialized with `dbt run`
- Verify temperature and token settings in the API config

## Future Enhancements

- **Batch LLM processing**: Efficient parallel generation for multiple matches using asyncio or concurrent.futures
- **Distributed processing with PySpark**: Scale narrative generation across Spark clusters for large-scale match datasets
- **Caching layer**: Cache prompts and narratives to avoid re-querying identical match data
- **Export formats**: Markdown, HTML, and JSON report generation for integration with downstream systems
- **Multi-LLM support**: Switch between Gemini, Claude, GPT, and local models for comparison/fallback
- **Context enrichment**: Integrate player form, head-to-head records, weather data, and pitch conditions
- **Match-type customization**: Fine-tune prompts and narrative style based on Test vs ODI vs T20 vs other formats
- **Visualization generation**: Auto-generate charts (run progression, wicket timeline, etc.) alongside narratives
- **Streaming narratives**: Stream LLM output to users in real-time as it generates
- **Feedback loop**: Collect user ratings on narrative quality to improve prompt design
- **Multi-language support**: Generate narratives in multiple languages
- **Real-time match commentary**: Hook into live match APIs for instant narrative generation during matches


