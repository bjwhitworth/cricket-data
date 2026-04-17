#!/usr/bin/env python3
"""
Generate natural language descriptions of cricket matches using LLM.
Usage: python scripts/python/generate_match_narrative.py <match_id> [--type brief|full] [--provider gemini|local] [--model MODEL_ID] [--prompt] [--prompt-only] [--no-store]
"""

import sys
import os
import json
import argparse
import time
import duckdb
import google.genai as genai
from typing import Literal, Optional
from dataclasses import dataclass
from datetime import datetime
from narratives.prompts import format_match_prompt as _format_match_prompt_impl

# Configuration
DB_PATH = os.getenv('CRICKET_DB_PATH', 'data/duckdb/dev.duckdb')
DEFAULT_GEMINI_MODEL = os.getenv('GEMINI_MODEL', 'gemini-2.5-flash-lite')
DEFAULT_LOCAL_MODEL = os.getenv('LOCAL_NARRATIVE_MODEL', 'google/gemma-4-E2B-it')


def _local_log(message: str) -> None:
    """Emit local-model progress logs to stderr for easy CLI tracing."""
    print(f"[local-model] {message}", file=sys.stderr, flush=True)


@dataclass
class GenerationConfig:
    """Configuration for generation backend/model selection."""
    provider: Literal['gemini', 'local']
    model: str

@dataclass
class DescriptionConfig:
    """Configuration for description generation type."""
    type: Literal['brief', 'full']
    max_tokens: int
    temperature: float
    top_p: float
    do_sample: bool
    
    @classmethod
    def get_config(cls, desc_type: Literal['brief', 'full'] = 'full') -> 'DescriptionConfig':
        """Get config for description type."""
        if desc_type == 'brief':
            return cls(type='brief', max_tokens=200, temperature=1.0, top_p=0.95, do_sample=True)
        else:
            return cls(type='full', max_tokens=1500, temperature=1.0, top_p=0.95, do_sample=True)

def create_narrative_json_blob(
    match_id: str,
    desc_type: str,
    description: str,
    source: str = 'cli',
    model: str = DEFAULT_GEMINI_MODEL,
    model_origin: Literal['local', 'api'] = 'api',
) -> dict:
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
        "model_identifier": model,
        "model_origin": model_origin,
        "model": model,
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
                match_type,
                event_name,
                city_mapped_or_source as city,
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
            , batting_team
            , sum(runs_batter) as runs
            , count(*) as balls_faced
            , count_if(runs_batter = 4) as fours
            , count_if(runs_batter = 6) as sixes
            FROM stg_cricket__deliveries
            WHERE match_id::TEXT = ?
            GROUP BY batter, innings_number, batting_team

            )

            select 
            batter
            , batting_team
            , sum(runs) as runs_in_match
            , listagg('Innings ' || innings_number || ': ' || runs, ', ' order by innings_number asc) as innings_scores
            , sum(balls_faced) as balls_faced_in_match
            , sum(fours) as fours_in_match
            , sum(sixes) as sixes_in_match
            from innings_numbers
            group by batter, batting_team
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

def format_match_prompt(
    data: dict,
    config: Optional[DescriptionConfig] = None,
    strict_schema: bool = False,
    compact: bool = False,
) -> str:
    """Format match data into an LLM prompt.
    
    Args:
        data: Match data dictionary
        config: Description configuration (controls detail level)
        strict_schema: If True, enforce strict structured output contract
        compact: If True, pass fewer scorecard details to reduce context bloat
    """
    config = config or DescriptionConfig.get_config('full')
    return _format_match_prompt_impl(
        data,
        config=config,
        strict_schema=strict_schema,
        compact=compact,
    )


def _generate_with_gemini(prompt: str, config: DescriptionConfig, model: str, api_key: Optional[str] = None) -> str:
    """Generate text using Gemini API."""
    api_key = api_key or os.getenv('GEMINI_API_KEY')
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set")
    client = genai.Client(api_key=api_key)

    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=genai.types.GenerateContentConfig(
            max_output_tokens=config.max_tokens,
            temperature=config.temperature,
        )
    )

    return response.text


def _generate_with_local_model(prompt: str, config: DescriptionConfig, model: str) -> str:
    """Generate text using a local Hugging Face transformers model."""
    start_total = time.perf_counter()
    _local_log(f"Starting local generation with model '{model}'")

    try:
        _local_log("Importing local inference dependencies (torch, transformers)")
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer
    except ImportError as exc:
        raise ImportError(
            "Local generation requires 'transformers' and 'torch'. Install them in your environment."
        ) from exc

    tokenizer_load_start = time.perf_counter()
    _local_log("Loading tokenizer from Hugging Face cache/hub")
    tokenizer = AutoTokenizer.from_pretrained(model)
    _local_log(f"Tokenizer loaded in {time.perf_counter() - tokenizer_load_start:.2f}s")

    model_load_start = time.perf_counter()
    _local_log("Loading model weights (first run may download files)")
    model_obj = AutoModelForCausalLM.from_pretrained(model, torch_dtype='auto')
    _local_log(f"Model weights loaded in {time.perf_counter() - model_load_start:.2f}s")

    device = 'mps' if torch.backends.mps.is_available() else 'cpu'
    _local_log(f"Selecting execution device: {device}")
    model_obj = model_obj.to(device)

    tokenization_start = time.perf_counter()
    messages = [
        {
            'role': 'system',
            'content': (
                'You are a cricket match summarizer. Follow the user schema exactly and return only the final answer.'
            ),
        },
        {'role': 'user', 'content': prompt},
    ]

    if hasattr(tokenizer, 'apply_chat_template'):
        _local_log("Applying model chat template")
        model_input_text = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
        )
    else:
        _local_log("Chat template unavailable; using plain text input")
        model_input_text = prompt

    _local_log(f"Tokenizing model input (characters={len(model_input_text)})")
    encoded = tokenizer(model_input_text, return_tensors='pt', truncation=True, max_length=4096)
    encoded = {k: v.to(device) for k, v in encoded.items()}
    prompt_tokens = int(encoded['input_ids'].shape[1])
    _local_log(
        f"Tokenization complete in {time.perf_counter() - tokenization_start:.2f}s "
        f"(prompt_tokens={prompt_tokens})"
    )

    local_max_new_tokens = min(config.max_tokens, 120 if config.type == 'brief' else 320)
    generation_kwargs = {
        'max_new_tokens': local_max_new_tokens,
        'do_sample': config.temperature > 0,
        'temperature': max(min(config.temperature, 0.4), 0.1),
        'repetition_penalty': 1.08,
        'pad_token_id': tokenizer.eos_token_id,
    }

    _local_log(
        "Running generation "
        f"(max_new_tokens={generation_kwargs['max_new_tokens']}, "
        f"temperature={generation_kwargs['temperature']}, "
        f"do_sample={generation_kwargs['do_sample']})"
    )
    generation_start = time.perf_counter()
    output_ids = model_obj.generate(**encoded, **generation_kwargs)
    generation_seconds = time.perf_counter() - generation_start

    generated_ids = output_ids[0][encoded['input_ids'].shape[1]:]
    output_tokens = int(generated_ids.shape[0])
    _local_log(
        f"Generation complete in {generation_seconds:.2f}s "
        f"(generated_tokens={output_tokens})"
    )

    decode_start = time.perf_counter()
    text = tokenizer.decode(generated_ids, skip_special_tokens=True).strip()
    _local_log(
        f"Decode complete in {time.perf_counter() - decode_start:.2f}s "
        f"(output_chars={len(text)})"
    )
    _local_log(f"Local generation finished in {time.perf_counter() - start_total:.2f}s")
    return text


def generate_narrative(
    match_id: str,
    desc_type: Literal['brief', 'full'] = 'brief',
    provider: Literal['gemini', 'local'] = 'local',
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    show_prompt: bool = False,
    return_model: bool = False,
) -> str | tuple[str, str]:
    """
    Generate match narrative using Gemini API or a local transformers model.

    Args:
        match_id: The cricket match ID
        desc_type: Type of description ('brief' or 'full')
        provider: Generation provider ('gemini' or 'local')
        model: Optional model ID override for selected provider
        api_key: Optional Gemini API key (defaults to GEMINI_API_KEY env var)
        show_prompt: If True, print the prompt before generating narrative
        return_model: If True, return tuple of (text, model_used)

    Returns:
        Generated narrative text, or (text, model_id) when return_model=True
    """
    if provider == 'local':
        _local_log(f"Preparing narrative request (match_id={match_id}, type={desc_type})")

    desc_config = DescriptionConfig.get_config(desc_type)
    data = fetch_match_data(match_id)
    if provider == 'local':
        _local_log(
            "Fetched match data "
            f"(innings={len(data['innings'])}, "
            f"top_batters={len(data['top_batters'])}, "
            f"top_bowlers={len(data['top_bowlers'])}, "
            f"key_wickets={len(data['key_wickets'])})"
        )

    prompt = format_match_prompt(
        data,
        desc_config,
        strict_schema=(provider == 'local'),
        # compact=(provider == 'local'),
        compact=False,  # TODO: verify with actual outputs
    )
    if provider == 'local':
        _local_log(f"Prompt prepared (characters={len(prompt)})")

    if show_prompt:
        print(f"\n{'='*80}")
        print("PROMPT USED:")
        print(f"{'='*80}\n")
        print(prompt)
        print(f"\n{'='*80}\n")

    generation = GenerationConfig(
        provider=provider,
        model=model or (DEFAULT_GEMINI_MODEL if provider == 'gemini' else DEFAULT_LOCAL_MODEL),
    )
    if generation.provider == 'local':
        _local_log(f"Using local provider with model '{generation.model}'")

    if generation.provider == 'gemini':
        text = _generate_with_gemini(prompt, desc_config, generation.model, api_key=api_key)
    else:
        text = _generate_with_local_model(prompt, desc_config, generation.model)

    if return_model:
        return text, generation.model
    return text


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse CLI arguments for narrative generation."""
    parser = argparse.ArgumentParser(
        description="Generate cricket match narratives from DuckDB match data."
    )
    parser.add_argument('match_id', help='Cricket match ID')
    parser.add_argument(
        '--type',
        dest='desc_type',
        choices=['brief', 'full'],
        default='brief',
        help='Narrative style/length (default: brief)',
    )
    parser.add_argument(
        '--provider',
        choices=['gemini', 'local'],
        default='local',
        help='Generation backend to use (default: local model)',
    )
    parser.add_argument(
        '--model',
        default=None,
        help=(
            'Optional model override. '
            f"Defaults: gemini={DEFAULT_GEMINI_MODEL}, local={DEFAULT_LOCAL_MODEL}"
        ),
    )
    parser.add_argument('--prompt', '-p', action='store_true', help='Print full prompt before generation')
    parser.add_argument('--prompt-only', action='store_true', help='Print prompt and exit without LLM call')
    parser.add_argument('--no-store', action='store_true', help='Do not store generated narrative in DuckDB')
    parser.add_argument(
        '--api-key',
        default=None,
        help='Gemini API key override (otherwise uses GEMINI_API_KEY environment variable)',
    )
    return parser.parse_args(argv)



if __name__ == "__main__":
    args = parse_args(sys.argv[1:])

    try:
        if args.prompt_only:
            # Generate and display prompt only, no API call
            config = DescriptionConfig.get_config(args.desc_type)
            data = fetch_match_data(args.match_id)
            prompt = format_match_prompt(data, config)
            print(f"\n{'='*80}")
            print(f"PROMPT FOR MATCH: {args.match_id} ({args.desc_type})")
            print(f"{'='*80}\n")
            print(prompt)
            print(f"\n{'='*80}\n")
        else:
            narrative, model_used = generate_narrative(
                args.match_id,
                desc_type=args.desc_type,
                provider=args.provider,
                model=args.model,
                api_key=args.api_key,
                show_prompt=args.prompt,
                return_model=True,
            )
            if not args.no_store:
                source = 'cli_local' if args.provider == 'local' else 'cli'
                model_origin = 'local' if args.provider == 'local' else 'api'
                narrative_json = create_narrative_json_blob(
                    args.match_id,
                    args.desc_type,
                    narrative,
                    source=source,
                    model=model_used,
                    model_origin=model_origin,
                )
                store_narrative_json(narrative_json)
            print(f"\n{'='*80}")
            print(f"MATCH NARRATIVE: {args.match_id} ({args.desc_type}, {args.provider})")
            print(f"{'='*80}\n")
            print(narrative)
            print(f"\n{'='*80}\n")
            print(f"Model used: {model_used}")
            if not args.no_store:
                print("Stored narrative to raw_narratives")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
