#!/usr/bin/env python3
"""
Generate natural language descriptions of cricket matches using LLM.
Usage: python scripts/python/generate_match_narrative.py <match_id> [--type brief|full] [--prompt] [--prompt-only] [--no-store]
"""

import sys
import os
import json
import duckdb
import google.genai as genai
from typing import Literal
from dataclasses import dataclass
from datetime import datetime

# Configuration
DB_PATH = os.getenv('CRICKET_DB_PATH', 'data/duckdb/dev.duckdb')

@dataclass
class DescriptionConfig:
    """Configuration for description generation type."""
    type: Literal['brief', 'full']
    max_tokens: int
    temperature: float
    
    @classmethod
    def get_config(cls, desc_type: Literal['brief', 'full'] = 'full') -> 'DescriptionConfig':
        """Get config for description type."""
        if desc_type == 'brief':
            return cls(type='brief', max_tokens=200, temperature=0.5)
        else:
            return cls(type='full', max_tokens=1500, temperature=0.7)

def create_narrative_json_blob(match_id: str, desc_type: str, description: str, source: str = 'cli') -> dict:
    """Create standardized narrative JSON structure.
    
    Args:
        match_id: Cricket match identifier
        desc_type: Type of narrative ('brief', 'full', etc.)
        description: Generated narrative text
        source: Origin of generation ('cli', 'batch', 'batch_api', etc.)
    
    Returns:
        Dictionary with narrative metadata (ready to JSON serialize)
    """
    return {
        "match_id": match_id,
        "description_type": desc_type,
        "description": description,
        "generated_at": datetime.now().isoformat(),
        "model": "gemini-2.5-flash-lite",
        "source": source
    }

def store_narrative_json(narrative_json: dict, db_path: str = None) -> None:
    """Insert narrative JSON blob into raw_narratives table (creates table if missing)."""
    db_path = db_path or DB_PATH
    with duckdb.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_narratives (
                raw_narrative_id UUID DEFAULT gen_random_uuid(),
                narrative_json JSON,
                loaded_at TIMESTAMP DEFAULT now()
            )
            """
        )
        conn.execute(
            "INSERT INTO raw_narratives (narrative_json) VALUES (?)",
            [json.dumps(narrative_json)]
        )

def _row_to_dict(cursor, row):
    """Convert a database row tuple to a dictionary using cursor column names."""
    if row is None:
        return None
    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))

def fetch_match_data(match_id: str) -> dict:
    """Query DuckDB for match summary data."""
    with duckdb.connect(DB_PATH, read_only=True) as conn:
        
        # Match metadata
        cursor = conn.execute("""
            SELECT 
                match_id,
                event_name,
                city,
                venue,
                match_start_date,
                team_1,
                team_2,
                toss_winner,
                toss_decision,
                winner,
                result_type,
                result_description,
                winner_after_eliminator,
                outcome_method,
                players_of_match
            FROM stg_cricket__matches
            WHERE match_id = ?
        """, [match_id])
        
        match_info = _row_to_dict(cursor, cursor.fetchone())
        
        if not match_info:
            raise ValueError(f"Match {match_id} not found")
        
        # Innings summaries
        cursor = conn.execute("""
            SELECT 
                innings_number,
                batting_team,
                is_super_over,
                runs_total,
                wickets_fallen,
                recorded_over_count
            FROM stg_cricket__innings
            WHERE match_id = ?
            ORDER BY innings_number
        """, [match_id])
        innings = [_row_to_dict(cursor, row) for row in cursor.fetchall()]
        
        # Top batters per innings
        cursor = conn.execute("""
            with innings_numbers AS (

            SELECT 
            innings_number
            , batter
            , sum(runs_batter) as runs
            , count(*) as balls_faced
            , count_if(runs_batter = 4) as fours
            , count_if(runs_batter = 6) as sixes
            FROM stg_cricket__deliveries
            WHERE match_id::TEXT = ?
            GROUP BY batter, innings_number

            )

            select 
            batter
            , sum(runs) as runs_in_match
            , listagg('Innings ' || innings_number || ': ' || runs, ', ' order by innings_number asc) as innings_scores
            , sum(balls_faced) as balls_faced_in_match
            , sum(fours) as fours_in_match
            , sum(sixes) as sixes_in_match
            from innings_numbers
            group by batter
            order by runs_in_match DESC
        """, [match_id])
        top_batters = [_row_to_dict(cursor, row) for row in cursor.fetchall()]
        
        # Top bowlers across all innings
        cursor = conn.execute("""
            WITH innings_numbers AS (
                SELECT 
                    innings_number,
                    bowler,
                    count(*) as balls_bowled,
                    sum(runs_total) as runs_conceded,
                    count_if(is_wicket) as wickets
                FROM stg_cricket__deliveries
                WHERE match_id::TEXT = ?
                GROUP BY innings_number, bowler
            )
            SELECT 
                bowler
                , sum(wickets) as wickets_in_match
                , sum(runs_conceded) as runs_conceded_in_match
                , sum(balls_bowled) as balls_bowled_in_match
                , round(runs_conceded_in_match / nullif(balls_bowled_in_match, 0), 2) as match_economy_per_ball
                , listagg(
                    'Innings ' || innings_number || ': ' || 
                    wickets || '/' || runs_conceded || 
                    ' (' || (balls_bowled // 6) || '.' || (balls_bowled % 6) || ' overs)',
                    ', '
                    ORDER BY innings_number ASC
                ) as innings_details
            FROM innings_numbers
            GROUP BY bowler
            ORDER BY wickets_in_match DESC, match_economy_per_ball ASC
        """, [match_id])
        top_bowlers = [_row_to_dict(cursor, row) for row in cursor.fetchall()]
        
        # Key wickets
        cursor = conn.execute("""
            SELECT 
                innings_number,
                over_number,
                ball_in_over,
                wicket_player_out,
                wicket_kind,
                bowler,
                wicket_fielder_1,
                wicket_fielder_2
            FROM stg_cricket__deliveries
            WHERE match_id = ? AND is_wicket = true
            ORDER BY innings_number, over_number, ball_in_over
        """, [match_id])
        key_wickets = [_row_to_dict(cursor, row) for row in cursor.fetchall()]
        
        return {
            'match_info': match_info,
            'innings': innings,
            'top_batters': top_batters,
            'top_bowlers': top_bowlers,
            'key_wickets': key_wickets
        }

def format_match_prompt(data: dict, config: DescriptionConfig) -> str:
    """Format match data into an LLM prompt.
    
    Args:
        data: Match data dictionary
        config: Description configuration (controls detail level)
    """
    info = data['match_info']

    match_details = f"""
    
        Match Details:
        - Event: {info['event_name']}
        - Venue: {info['venue']}, {info['city']}
        - Date: {info['match_start_date']}
        - Teams: {info['team_1']} vs {info['team_2']}
        - Toss: {info['toss_winner']} won and chose to {info['toss_decision']}
        - Result: {info['result_type']}
        - Winner: {info['winner'] or info['winner_after_eliminator'] or 'Tie/No Result'}
        - Margin: {f"{info['result_description']}" if info['result_description'] else "N/A"}
        - Player(s) of the Match: {info['players_of_match']}

        Innings Summaries:
        """
        
    for inning in data['innings']:
        is_super = " (Super Over)" if inning['is_super_over'] else ""
        match_details += f"\nInnings {inning['innings_number']}{is_super}: {inning['batting_team']} scored {inning['runs_total']}/{inning['wickets_fallen']} in {inning['recorded_over_count']} overs\n"
    
    match_details += "\nTop Batting Performances:\n"
    for batter in data['top_batters'][:6]:
        sr = (batter['runs_in_match'] / batter['balls_faced_in_match'] * 100) if batter['balls_faced_in_match'] > 0 else 0
        match_details += f"- {batter['batter']} - {batter['runs_in_match']} runs ({batter['innings_scores']}) ({batter['balls_faced_in_match']} balls, {batter['fours_in_match']} fours, {batter['sixes_in_match']} sixes, SR: {sr:.1f})\n"
    
    match_details += "\nTop Bowling Performances:\n"
    for bowler in data['top_bowlers'][:6]:
        overs = bowler['balls_bowled_in_match'] // 6
        balls = bowler['balls_bowled_in_match'] % 6
        economy = (bowler['runs_conceded_in_match'] / (bowler['balls_bowled_in_match'] / 6)) if bowler['balls_bowled_in_match'] > 0 else 0
        match_details += f"- {bowler['bowler']} - {bowler['wickets_in_match']}/{bowler['runs_conceded_in_match']} ({overs}.{balls} overs, economy {economy:.2f}) [{bowler['innings_details']}]\n"
    
    match_details += f"\nKey Wickets: {len(data['key_wickets'])} total dismissals\n"
    for wicket in data['key_wickets'][:40]:
        fielders = f" (c {wicket['wicket_fielder_1']}" + (f" & {wicket['wicket_fielder_2']}" if wicket['wicket_fielder_2'] else "") + ")" if wicket['wicket_fielder_1'] else ""
        match_details += f"- Innings {wicket['innings_number']} : Over {wicket['over_number']}.{wicket['ball_in_over']}: {wicket['wicket_player_out']} {wicket['wicket_kind']} b {wicket['bowler']}{fielders}\n"
    
    if config.type == 'brief':
        # Minimal prompt for brief descriptions
        prompt = f"""
        Provide a ~15/20 word summary of this cricket match. 
        You must be curt, to the point; don't worry about full sentences.
        Describe the high-level vibe using match details. For example, was it close, one-sided, high-scoring, low-scoring, a chase, big hitting, tight bowling, etc.
        What were the most important contributions in the match? Perhaps pick one or two if needed.
        Was there one big turning point?

        Match:
        {match_details}

        """
        
    else:
        # Full detailed prompt
        prompt = f"""
        Write a short, punchy, pithy narrative of this cricket match in two or three paragraphs.  
        Don't be flowery; be direct, briefing-like. 
        Start with the most important information. 
        Pay attention to the match structure. 
        Focus on the key moments, standout performances, and the flow of the game. 
        Pick out the important turning points of the game.
        For instance, test matches are played over multiple days with typically two innings per side, so look at innings number 
          for batting sequence. Pay attention the innings in which the game was won.

        Match:
        {match_details}
        """
        
        prompt += "\nWrite the narrative now:"
    
    return prompt


def generate_narrative(match_id: str, desc_type: Literal['brief', 'full'] = 'full', api_key: str = None, show_prompt: bool = False) -> str:
    """
    Generate match narrative using Gemini.
    
    Args:
        match_id: The cricket match ID
        desc_type: Type of description ('brief' or 'full')
        api_key: Optional Gemini API key (defaults to GEMINI_API_KEY env var)
        show_prompt: If True, print the prompt before generating narrative
    
    Returns:
        The generated narrative text
    """
    config = DescriptionConfig.get_config(desc_type)
    data = fetch_match_data(match_id)
    prompt = format_match_prompt(data, config)
    
    if show_prompt:
        print(f"\n{'='*80}")
        print("PROMPT USED:")
        print(f"{'='*80}\n")
        print(prompt)
        print(f"\n{'='*80}\n")
    
    api_key = api_key or os.getenv('GEMINI_API_KEY')
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set")
    client = genai.Client(api_key=api_key)
    
    response = client.models.generate_content(
        model='gemini-2.5-flash-lite',
        contents=prompt,
        config=genai.types.GenerateContentConfig(
            max_output_tokens=config.max_tokens,
            temperature=config.temperature,
        )
    )
    
    return response.text



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/python/generate_match_narrative.py <match_id> [--type brief|full] [--prompt] [--prompt-only] [--no-store]")
        sys.exit(1)
    
    # Parse arguments
    match_id = sys.argv[1]
    show_prompt = '--prompt' in sys.argv or '-p' in sys.argv
    prompt_only = '--prompt-only' in sys.argv
    store_result = '--no-store' not in sys.argv  # default: store
    
    # Extract --type argument
    desc_type = 'full'
    for arg in sys.argv:
        if arg.startswith('--type='):
            desc_type = arg.split('=')[1]
            break
        elif arg == '--type' and sys.argv.index(arg) + 1 < len(sys.argv):
            desc_type = sys.argv[sys.argv.index(arg) + 1]
            break
    
    if desc_type not in ('brief', 'full'):
        print(f"Error: --type must be 'brief' or 'full', got '{desc_type}'")
        sys.exit(1)
    
    try:
        if prompt_only:
            # Generate and display prompt only, no API call
            config = DescriptionConfig.get_config(desc_type)
            data = fetch_match_data(match_id)
            prompt = format_match_prompt(data, config)
            print(f"\n{'='*80}")
            print(f"PROMPT FOR MATCH: {match_id} ({desc_type})")
            print(f"{'='*80}\n")
            print(prompt)
            print(f"\n{'='*80}\n")
        else:
            narrative = generate_narrative(match_id, desc_type=desc_type, show_prompt=show_prompt)
            if store_result:
                narrative_json = create_narrative_json_blob(match_id, desc_type, narrative, source='cli')
                store_narrative_json(narrative_json)
            print(f"\n{'='*80}")
            print(f"MATCH NARRATIVE: {match_id} ({desc_type})")
            print(f"{'='*80}\n")
            print(narrative)
            print(f"\n{'='*80}\n")
            if store_result:
                print("Stored narrative to raw_narratives (source=cli)")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
