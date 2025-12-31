#!/usr/bin/env python3
"""
Generate a natural language narrative of a cricket match using LLM.
Usage: python scripts/python/generate_match_narrative.py <match_id>
"""

import sys
import duckdb
import google.genai as genai

def fetch_match_data(match_id: str) -> dict:
    """Query DuckDB for match summary data."""
    with duckdb.connect('data/duckdb/dev.duckdb', read_only=True) as conn:
        
        # Match metadata
        match_info = conn.execute("""
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
        """, [match_id]).fetchone()
        
        if not match_info:
            raise ValueError(f"Match {match_id} not found")
        
        # Innings summaries
        innings = conn.execute("""
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
        """, [match_id]).fetchall()
        
        # Top batters per innings
        top_batters = conn.execute("""
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
        """, [match_id]).fetchall()
        
        # Top bowlers per innings
        top_bowlers = conn.execute("""
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
        """, [match_id]).fetchall()
        
        # Key wickets
        key_wickets = conn.execute("""
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
        """, [match_id]).fetchall()
        
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
- Event: {info[1]}
- Venue: {info[3]}, {info[2]}
- Date: {info[4]}
- Teams: {info[5]} vs {info[6]}
- Toss: {info[7]} won and chose to {info[8]}
- Result: {info[10]}
- Winner: {info[9] or info[12] or 'Tie/No Result'}
- Margin: {f"{info[11]} runs" if info[11] else f"{info[12]} wickets" if info[12] else "N/A"}
- Player(s) of the Match: {info[14]}

Innings Summaries:
"""
    
    for inning in data['innings']:
        is_super = " (Super Over)" if inning[2] else ""
        prompt += f"\nInnings {inning[0]}{is_super}: {inning[1]} scored {inning[3]}/{inning[4]} in {inning[5]} overs\n"
    
    prompt += "\nTop Batting Performances:\n"
    for batter in data['top_batters'][:6]:
        sr = (batter[2] / batter[3] * 100) if batter[3] > 0 else 0
        prompt += f"- Innings {batter[0]}: {batter[1]} - {batter[2]} runs ({batter[3]} balls, {batter[4]} fours, {batter[5]} sixes, SR: {sr:.1f})\n"
    
    prompt += "\nTop Bowling Performances:\n"
    for bowler in data['top_bowlers'][:6]:
        overs = bowler[2] // 6
        balls = bowler[2] % 6
        prompt += f"- Innings {bowler[0]}: {bowler[1]} - {bowler[4]}/{bowler[3]} ({overs}.{balls} overs)\n"
    
    prompt += f"\nKey Wickets: {len(data['key_wickets'])} total dismissals\n"
    for wicket in data['key_wickets'][:8]:
        fielders = f" (c {wicket[6]}" + (f" & {wicket[7]}" if wicket[7] else "") + ")" if wicket[6] else ""
        prompt += f"- Over {wicket[1]}.{wicket[2]}: {wicket[3]} {wicket[4]} b {wicket[5]}{fielders}\n"
    
    prompt += "\nWrite the narrative now:"
    
    return prompt

def generate_narrative(match_id: str, api_key: str = None) -> str:
    """Generate match narrative using Gemini."""
    data = fetch_match_data(match_id)
    prompt = format_match_prompt(data)
    
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
        print("Usage: python scripts/python/generate_match_narrative.py <match_id>")
        sys.exit(1)
    
    match_id = sys.argv[1]
    
    try:
        narrative = generate_narrative(match_id)
        print(f"\n{'='*80}")
        print(f"MATCH NARRATIVE: {match_id}")
        print(f"{'='*80}\n")
        print(narrative)
        print(f"\n{'='*80}\n")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
