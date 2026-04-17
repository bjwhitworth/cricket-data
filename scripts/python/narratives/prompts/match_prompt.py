"""Prompt construction for match narrative generation."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent
from typing import Any


_TEXT_DIR = Path(__file__).resolve().parent / 'text'
_TEXT_CACHE: dict[str, str] = {}
_PROMPT_ASSET_MANIFEST = {
    'system_persona': 'system_persona.md',
    'cricket_context': 'cricket_context.md',
    'strict_rules': 'strict_rules.md',
    'schemas': {
        'brief': 'response_schema_brief.md',
        'full': 'response_schema_full.md',
    },
    'examples': {
        'brief': 'examples_brief.md',
        'full': 'examples_full.md',
    },
    'relaxed_instructions': {
        'brief': 'relaxed_brief_instructions.md',
        'full': 'relaxed_full_instructions.md',
    },
}
_SCHEMA_BY_MODE = {
    'brief': _PROMPT_ASSET_MANIFEST['schemas']['brief'],
    'full': _PROMPT_ASSET_MANIFEST['schemas']['full'],
}
_EXAMPLES_BY_MODE = {
    'brief': _PROMPT_ASSET_MANIFEST['examples']['brief'],
    'full': _PROMPT_ASSET_MANIFEST['examples']['full'],
}
_RELAXED_INSTRUCTIONS_BY_MODE = {
    'brief': _PROMPT_ASSET_MANIFEST['relaxed_instructions']['brief'],
    'full': _PROMPT_ASSET_MANIFEST['relaxed_instructions']['full'],
}
_ASSETS_VALIDATED = False


def _clean_prompt(text: str) -> str:
    """Normalize indentation and surrounding whitespace in prompt blocks."""
    return dedent(text).strip()


def _iter_manifest_filenames() -> list[str]:
    """Flatten the asset manifest into a unique list of required filenames."""
    filenames = [
        _PROMPT_ASSET_MANIFEST['system_persona'],
        _PROMPT_ASSET_MANIFEST['cricket_context'],
        _PROMPT_ASSET_MANIFEST['strict_rules'],
        *_PROMPT_ASSET_MANIFEST['schemas'].values(),
        *_PROMPT_ASSET_MANIFEST['examples'].values(),
        *_PROMPT_ASSET_MANIFEST['relaxed_instructions'].values(),
    ]
    return sorted(set(filenames))


def _validate_prompt_assets() -> None:
    """Validate that required prompt markdown assets exist."""
    global _ASSETS_VALIDATED
    if _ASSETS_VALIDATED:
        return

    missing = [name for name in _iter_manifest_filenames() if not (_TEXT_DIR / name).exists()]
    if missing:
        missing_list = ', '.join(missing)
        raise FileNotFoundError(
            f"Missing prompt asset file(s) in {_TEXT_DIR}: {missing_list}. "
            "Check _PROMPT_ASSET_MANIFEST for required files."
        )

    _ASSETS_VALIDATED = True


def _load_text(filename: str) -> str:
    """Load static prompt text snippets stored alongside this module."""
    _validate_prompt_assets()

    cached = _TEXT_CACHE.get(filename)
    if cached is not None:
        return cached

    text = (_TEXT_DIR / filename).read_text(encoding='utf-8').strip()
    _TEXT_CACHE[filename] = text
    return text


def _mode(config: Any) -> str:
    """Normalize configuration type into supported prompt modes."""
    return 'brief' if config.type == 'brief' else 'full'


def _response_schema(mode: str) -> str:
    """Return strict output schema instructions for a prompt mode."""
    return _load_text(_SCHEMA_BY_MODE[mode])


def _strict_examples(mode: str) -> str:
    """Return strict prompt examples for a prompt mode."""
    return _load_text(_EXAMPLES_BY_MODE[mode])


def _relaxed_instructions(mode: str) -> str:
    """Return relaxed instructions for a prompt mode."""
    return _load_text(_RELAXED_INSTRUCTIONS_BY_MODE[mode])


def _build_strict_prompt(match_details: str, schema_block: str, mode: str) -> str:
    """Build strict contract prompt for local instruction-following generation."""
    system_persona = _load_text(_PROMPT_ASSET_MANIFEST['system_persona'])
    cricket_context = _load_text(_PROMPT_ASSET_MANIFEST['cricket_context'])
    strict_rules = _load_text(_PROMPT_ASSET_MANIFEST['strict_rules'])
    examples = _strict_examples(mode)

    return _clean_prompt(f"""
        SYSTEM:
        {system_persona}

        RULES:
        {schema_block}
        {strict_rules}

        CRICKET CONTEXT:
        {cricket_context}

        EXAMPLES:
        {examples}

        MATCH_DATA:
        {match_details}

        OUTPUT:
        Return only the final narrative following RESPONSE_SCHEMA.
        """)


def _build_relaxed_prompt(match_details: str, schema_block: str, mode: str) -> str:
    """Build legacy relaxed prompt variants for API generation flows."""
    instructions = _relaxed_instructions(mode)
    return _clean_prompt(f"""
        {schema_block}
        {instructions}

        Match:
        {match_details}
    """)


def _format_match_details(
    data: dict,
    batter_limit: int,
    bowler_limit: int,
    wicket_limit: int,
) -> str:
    """Render structured match details used as context for prompt templates."""
    info = data['match_info']

    match_details = _clean_prompt(f"""

        Match Details:
        - Event: {info['event_name']}
        - Type: {info.get('match_type', 'Unknown')}
        - Venue: {info['venue']}, {info['city']}
        - Date: {info['match_start_date']}
        - Teams: {info['team_1']} vs {info['team_2']}
        - Toss: {info['toss_winner']} won and chose to {info['toss_decision']}
        - Winner: {info['winner'] or info['winner_after_eliminator'] or 'Tie/No Result'}
        - Margin: {f"{info['result_description']}" if info['result_description'] else "N/A"}
        - Player(s) of the Match: {info['players_of_match']}

        Innings Summaries:
    """)

    for inning in data['innings']:
        is_super = " (Super Over)" if inning['is_super_over'] else ""
        match_details += (
            f"\nInnings {inning['innings_number']}{is_super}: "
            f"{inning['batting_team']} scored {inning['runs_total']}/{inning['wickets_fallen']} "
            f"in {inning['recorded_over_count']} overs\n"
        )

    match_details += "\nTop Batting Performances:\n"
    for batter in data['top_batters'][:batter_limit]:
        sr = (
            (batter['runs_in_match'] / batter['balls_faced_in_match'] * 100)
            if batter['balls_faced_in_match'] > 0
            else 0
        )
        match_details += (
            f"- {batter['batter']} ({batter['batting_team']}) - {batter['runs_in_match']} runs in the match ({batter['innings_scores']}) "
            f"({batter['balls_faced_in_match']} balls, {batter['fours_in_match']} fours, "
            f"{batter['sixes_in_match']} sixes, SR: {sr:.1f})\n"
        )

    match_details += "\nTop Bowling Performances:\n"
    for bowler in data['top_bowlers'][:bowler_limit]:
        overs = bowler['balls_bowled_in_match'] // 6
        balls = bowler['balls_bowled_in_match'] % 6
        economy = (
            (bowler['runs_conceded_in_match'] / (bowler['balls_bowled_in_match'] / 6))
            if bowler['balls_bowled_in_match'] > 0
            else 0
        )
        match_details += (
            f"- {bowler['bowler']} - {bowler['wickets_in_match']}/{bowler['runs_conceded_in_match']} "
            f"({overs}.{balls} overs, economy {economy:.2f}) [{bowler['innings_details']}]\n"
        )

    match_details += (
        f"\nKey Wickets: {len(data['key_wickets'])} total dismissals "
        f"(showing up to {wicket_limit})\n"
    )
    for wicket in data['key_wickets'][:wicket_limit]:
        fielders = (
            f" (c {wicket['wicket_fielder_1']}"
            + (f" & {wicket['wicket_fielder_2']}" if wicket['wicket_fielder_2'] else "")
            + ")"
            if wicket['wicket_fielder_1']
            else ""
        )
        match_details += (
            f"- Innings {wicket['innings_number']} : Over {wicket['over_number']}.{wicket['ball_in_over']}: "
            f"{wicket['wicket_player_out']} {wicket['wicket_kind']} b {wicket['bowler']}{fielders}\n"
        )

    return match_details


def format_match_prompt(
    data: dict,
    config: Any,
    strict_schema: bool = False,
    compact: bool = False,
) -> str:
    """Format match data into an LLM prompt."""
    limits = {
        'batter_limit': 4 if compact else 6,
        'bowler_limit': 4 if compact else 6,
        'wicket_limit': 10 if compact else 40,
    }

    mode = _mode(config)
    match_details = _format_match_details(data, **limits)
    schema_block = _response_schema(mode)
    if strict_schema:
        return _build_strict_prompt(match_details, schema_block, mode)

    return _build_relaxed_prompt(match_details, schema_block, mode)
