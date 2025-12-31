#!/usr/bin/env python3
"""
Generate a natural language narrative of a cricket match using LLM.
Usage: python scripts/python/generate_match_narrative.py <match_id>
"""

import sys
import os
import duckdb
import google.genai as genai

def _row_to_dict(cursor, row):
    """Convert a database row tuple to a dictionary using cursor column names."""
    if row is None:
        return None
    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))

def fetch_match_data(match_id: str) -> dict:
    """Query DuckDB for match summary data."""
    with duckdb.connect('data/duckdb/dev.duckdb', read_only=True) as conn:
        
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
            SELECT 
                innings_number,
                batter,
                sum(runs_batter) as runs,
                count(*) as balls_faced,
                count_if(runs_batter = 4) as fours,
                count_if(runs_batter = 6) as sixes
            FROM stg_cricket__deliveries
            WHERE match_id = ?
            GROUP BY innings_number, batter
            HAVING runs >= 20
            ORDER BY innings_number, runs DESC
        """, [match_id])
        top_batters = [_row_to_dict(cursor, row) for row in cursor.fetchall()]
        
        # Top bowlers per innings
        cursor = conn.execute("""
            SELECT 
                innings_number,
                bowler,
                count(*) as balls_bowled,
                sum(runs_total) as runs_conceded,
                count_if(is_wicket) as wickets
            FROM stg_cricket__deliveries
            WHERE match_id = ?
            GROUP BY innings_number, bowler
            HAVING wickets > 0
            ORDER BY innings_number, wickets DESC, runs_conceded ASC
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

def format_match_prompt(data: dict) -> str:
    """Format match data into an LLM prompt."""
    info = data['match_info']
    
    prompt = f"""Write a short, punchy, pithy narrative of this cricket match in two or three paragraphs.  Don't be flowery; be direct, briefing-like. Start with the most important information. Pay attention to the match structure, including innings number for batting sequence. Focus on the key moments, standout performances, and the flow of the game. Pick out the important turning points of the game.

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
        prompt += f"\nInnings {inning['innings_number']}{is_super}: {inning['batting_team']} scored {inning['runs_total']}/{inning['wickets_fallen']} in {inning['recorded_over_count']} overs\n"
    
    prompt += "\nTop Batting Performances:\n"
    for batter in data['top_batters'][:6]:
        sr = (batter['runs'] / batter['balls_faced'] * 100) if batter['balls_faced'] > 0 else 0
        prompt += f"- Innings {batter['innings_number']}: {batter['batter']} - {batter['runs']} runs ({batter['balls_faced']} balls, {batter['fours']} fours, {batter['sixes']} sixes, SR: {sr:.1f})\n"
    
    prompt += "\nTop Bowling Performances:\n"
    for bowler in data['top_bowlers'][:6]:
        overs = bowler['balls_bowled'] // 6
        balls = bowler['balls_bowled'] % 6
        prompt += f"- Innings {bowler['innings_number']}: {bowler['bowler']} - {bowler['wickets']}/{bowler['runs_conceded']} ({overs}.{balls} overs)\n"
    
    prompt += f"\nKey Wickets: {len(data['key_wickets'])} total dismissals\n"
    for wicket in data['key_wickets'][:8]:
        fielders = f" (c {wicket['wicket_fielder_1']}" + (f" & {wicket['wicket_fielder_2']}" if wicket['wicket_fielder_2'] else "") + ")" if wicket['wicket_fielder_1'] else ""
        prompt += f"- Over {wicket['over_number']}.{wicket['ball_in_over']}: {wicket['wicket_player_out']} {wicket['wicket_kind']} b {wicket['bowler']}{fielders}\n"
    
    prompt += "\nWrite the narrative now:"
    
    return prompt

def generate_narrative(match_id: str, api_key: str = None, show_prompt: bool = False) -> str:
    """Generate match narrative using Gemini.
    
    Args:
        match_id: The cricket match ID
        api_key: Optional Gemini API key (defaults to GEMINI_API_KEY env var)
        show_prompt: If True, print the prompt before generating narrative
    
    Returns:
        The generated narrative text
    """
    data = fetch_match_data(match_id)
    prompt = format_match_prompt(data)
    
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
            max_output_tokens=1500,
            temperature=0.7,
        )
    )
    
    return response.text

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/python/generate_match_narrative.py <match_id> [prompt=TRUE]")
        sys.exit(1)
    
    match_id = sys.argv[1]
    show_prompt = any(
        arg.lower() in ('prompt=true', '--prompt=true', '--prompt', '-p')
        for arg in sys.argv[2:]
    )
    
    try:
        narrative = generate_narrative(match_id, show_prompt=show_prompt)
        print(f"\n{'='*80}")
        print(f"MATCH NARRATIVE: {match_id}")
        print(f"{'='*80}\n")
        print(narrative)
        print(f"\n{'='*80}\n")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
