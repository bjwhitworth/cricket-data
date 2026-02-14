#!/usr/bin/env python3
"""
Enrich venue seed data in a single Gemini call.

Reads a venue seed CSV, sends all rows to Gemini once, and writes:
1) row-level city/country suggestions
2) alias mapping candidates

Usage:
  python scripts/python/enrich_venue_seed_with_gemini.py \
    --input seeds/venue_country_mapping.csv \
    --output-updates seeds/venue_country_mapping_gemini_suggestions.csv \
    --output-aliases seeds/venue_alias_mapping_gemini_candidates.csv

Optional:
  --prompt-only   Print prompt and exit (no API call)
  --model         Gemini model name (default: gemini-2.5-flash-lite)
    --raw-response-out  Persist Gemini raw/parsed response to JSON file
    --response-json-in  Reuse a saved response JSON file (skip API call)
    --raw-error-dir     Directory for saving unparseable raw responses
    --max-input-chars   Max prompt size per Gemini call (auto-chunks rows)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime
from typing import Any

import google.genai as genai


def _log(step: str, message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{step}] {message}")


def _read_seed_rows(path: str) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"venue", "city", "country"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Input CSV missing required columns: {sorted(missing)}")

        rows: list[dict[str, str]] = []
        for idx, row in enumerate(reader, start=1):
            rows.append(
                {
                    "row_id": str(idx),
                    "venue": (row.get("venue") or "").strip(),
                    "city": (row.get("city") or "").strip(),
                    "country": (row.get("country") or "").strip(),
                }
            )
        return rows


def _split_rows_by_prompt_size(rows: list[dict[str, str]], max_input_chars: int) -> list[list[dict[str, str]]]:
    if max_input_chars <= 0:
        return [rows]

    chunks: list[list[dict[str, str]]] = []
    current: list[dict[str, str]] = []

    for row in rows:
        candidate = current + [row]
        candidate_prompt_len = len(_build_prompt(candidate))

        if current and candidate_prompt_len > max_input_chars:
            chunks.append(current)
            current = [row]
        else:
            current = candidate

    if current:
        chunks.append(current)

    return chunks


def _chunked_path(base_path: str, chunk_idx: int, chunk_count: int) -> str:
    root, ext = os.path.splitext(base_path)
    suffix = f".part{chunk_idx:03d}_of_{chunk_count:03d}"
    return f"{root}{suffix}{ext}" if ext else f"{base_path}{suffix}"


def _build_prompt(rows: list[dict[str, str]]) -> str:
    payload = {
        "task": "Standardize venue seed rows and propose alias groups",
        "instructions": [
            "You are cleaning cricket venue mapping data.",
            "Suggest standardized city and country only when confident and only for rows that should change or be filled.",
            "Return sparse row_updates only; do not include unchanged rows.",
            "If uncertain, keep suggested_city and suggested_country as empty strings.",
            "Use conventional country names (for example: United States of America, United Arab Emirates, Saint Lucia, England, Scotland, Wales, Northern Ireland).",
            "Then identify likely alias groupings where multiple venue strings refer to the same physical venue.",
            "Alias groups should be conservative; avoid over-merging generic names such as County Ground without strong city evidence.",
            "Each row_update must include row_id and should echo source_venue/source_city for traceability.",
            "Return ONLY valid JSON. No markdown, no commentary.",
        ],
        "output_schema": {
            "row_updates": [
                {
                    "row_id": "string",
                    "source_venue": "string",
                    "source_city": "string",
                    "suggested_city": "string",
                    "suggested_country": "string",
                }
            ],
            "alias_groups": [
                {
                    "canonical_venue": "string",
                    "canonical_city": "string",
                    "canonical_country": "string",
                    "aliases": [{"alias_venue": "string", "alias_city": "string"}],
                }
            ],
        },
        "rows": rows,
    }
    return json.dumps(payload, ensure_ascii=False)


def _extract_json(text: str) -> dict[str, Any]:
    # Remove common JSON artifacts that Gemini might produce
    # 1. Remove trailing commas in lists or objects before a closing brace/bracket
    text = re.sub(r",\s*([\]}])", r"\1", text)
    
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        # If it fails, we try other extraction methods
        pass

    fence_match = re.search(r"```json\s*(\{.*?\})\s*```", text, flags=re.DOTALL)
    if fence_match:
        content = re.sub(r",\s*([\]}])", r"\1", fence_match.group(1))
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass

    brace_start = text.find("{")
    brace_end = text.rfind("}")
    if brace_start >= 0 and brace_end > brace_start:
        candidate = text[brace_start : brace_end + 1]
        candidate = re.sub(r",\s*([\]}])", r"\1", candidate)
        try:
            return json.loads(candidate)
        except json.JSONDecodeError as e:
            # Re-raise the original or the latest error with more context if needed
            raise ValueError(f"Failed to parse JSON from Gemini response: {e}") from e

    raise ValueError("Gemini response did not contain parseable JSON")


def _write_updates_csv(
    rows: list[dict[str, str]],
    updates_by_row_id: dict[str, dict[str, str]],
    output_path: str,
) -> None:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    fields = [
        "row_id",
        "venue",
        "city",
        "country",
        "suggested_city",
        "suggested_country",
    ]
    with open(output_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            upd = updates_by_row_id.get(row["row_id"], {})
            writer.writerow(
                {
                    "row_id": row["row_id"],
                    "venue": row["venue"],
                    "city": row["city"],
                    "country": row["country"],
                    "suggested_city": (upd.get("suggested_city") or "").strip(),
                    "suggested_country": (upd.get("suggested_country") or "").strip(),
                }
            )


def _write_aliases_csv(alias_groups: list[dict[str, Any]], output_path: str) -> int:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    fields = [
        "alias_venue",
        "alias_city",
        "canonical_venue",
        "canonical_city",
        "canonical_country",
        "review_status",
        "notes",
    ]
    written = 0
    with open(output_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()

        for group in alias_groups:
            canonical_venue = (group.get("canonical_venue") or "").strip()
            canonical_city = (group.get("canonical_city") or "").strip()
            canonical_country = (group.get("canonical_country") or "").strip()

            aliases = group.get("aliases") or []
            if not isinstance(aliases, list):
                continue

            for alias in aliases:
                alias_venue = (alias.get("alias_venue") or "").strip()
                alias_city = (alias.get("alias_city") or "").strip()
                if not alias_venue:
                    continue

                writer.writerow(
                    {
                        "alias_venue": alias_venue,
                        "alias_city": alias_city,
                        "canonical_venue": canonical_venue,
                        "canonical_city": canonical_city,
                        "canonical_country": canonical_country,
                        "review_status": "candidate",
                        "notes": "source=gemini_candidate",
                    }
                )
                written += 1

    return written


def _call_gemini(prompt: str, model: str, api_key: str | None) -> dict[str, Any]:
    token = api_key or os.getenv("GEMINI_API_KEY")
    if not token:
        raise ValueError("GEMINI_API_KEY environment variable not set")

    client = genai.Client(api_key=token)
    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=genai.types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.2,
            max_output_tokens=65535,
        ),
    )
    if not getattr(response, "text", None):
        raise ValueError("Gemini returned no text output")
    return _extract_json(response.text)


def _call_gemini_with_raw(prompt: str, model: str, api_key: str | None) -> str:
    token = api_key or os.getenv("GEMINI_API_KEY")
    if not token:
        raise ValueError("GEMINI_API_KEY environment variable not set")

    client = genai.Client(api_key=token)
    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=genai.types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.2,
            max_output_tokens=65535,
        ),
    )
    if not getattr(response, "text", None):
        raise ValueError("Gemini returned no text output")

    return response.text


def _save_raw_error_response(raw_error_dir: str, raw_text: str) -> str:
    os.makedirs(raw_error_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(raw_error_dir, f"venue_seed_gemini_parse_error_{timestamp}.txt")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(raw_text)
    return path


def _save_raw_error_response_for_chunk(raw_error_dir: str, raw_text: str, chunk_idx: int, chunk_count: int) -> str:
    os.makedirs(raw_error_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(
        raw_error_dir,
        f"venue_seed_gemini_parse_error_{timestamp}_part{chunk_idx:03d}_of_{chunk_count:03d}.txt",
    )
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(raw_text)
    return path


def _save_response_snapshot(
    output_path: str,
    input_path: str,
    model: str,
    prompt: str,
    raw_text: str,
    parsed_json: dict[str, Any],
) -> None:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    payload = {
        "created_at": datetime.now().isoformat(),
        "input_csv": input_path,
        "model": model,
        "prompt": prompt,
        "raw_response_text": raw_text,
        "parsed_response": parsed_json,
    }
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def _load_response_snapshot(input_path: str) -> tuple[dict[str, Any], str | None]:
    with open(input_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise ValueError("Saved response JSON must be an object")

    parsed = payload.get("parsed_response")
    raw_text = payload.get("raw_response_text")

    if isinstance(parsed, dict):
        return parsed, raw_text if isinstance(raw_text, str) else None

    if isinstance(raw_text, str) and raw_text.strip():
        return _extract_json(raw_text), raw_text

    if "row_updates" in payload or "alias_groups" in payload:
        return payload, None

    raise ValueError("Saved response JSON missing parsed_response/raw_response_text")


def main() -> int:
    parser = argparse.ArgumentParser(description="Single-call Gemini enrichment for venue seed CSV")
    parser.add_argument("--input", default="seeds/venue_country_mapping.csv")
    parser.add_argument(
        "--output-updates",
        default="seeds/venue_country_mapping_gemini_suggestions.csv",
    )
    parser.add_argument(
        "--output-aliases",
        default="seeds/venue_alias_mapping_gemini_candidates.csv",
    )
    parser.add_argument("--model", default="gemini-2.5-flash-lite")
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--prompt-only", action="store_true")
    parser.add_argument("--raw-response-out", default="")
    parser.add_argument("--response-json-in", default="")
    parser.add_argument("--raw-error-dir", default="batches/results")
    parser.add_argument("--max-input-chars", type=int, default=50000)
    args = parser.parse_args()

    _log("START", "Loading seed rows")
    rows = _read_seed_rows(args.input)
    _log("LOAD", f"Loaded {len(rows)} rows from {args.input}")

    chunks = _split_rows_by_prompt_size(rows, args.max_input_chars)
    _log("PLAN", f"Planned {len(chunks)} chunk(s) using max_input_chars={args.max_input_chars:,}")

    if args.response_json_in and len(chunks) > 1:
        raise ValueError("--response-json-in supports a single response snapshot; disable chunking or run one chunk")

    updates_by_row_id: dict[str, dict[str, str]] = {}
    alias_groups_merged: list[dict[str, Any]] = []

    row_lookup = {
        (
            (row.get("venue") or "").strip().lower(),
            (row.get("city") or "").strip().lower(),
        ): row["row_id"]
        for row in rows
    }

    for chunk_idx, chunk_rows in enumerate(chunks, start=1):
        _log("PROMPT", f"Building prompt for chunk {chunk_idx}/{len(chunks)} ({len(chunk_rows)} rows)")
        prompt = _build_prompt(chunk_rows)
        _log("PROMPT", f"Chunk {chunk_idx}/{len(chunks)} prompt size: {len(prompt):,} chars")

        if args.prompt_only:
            _log("DONE", f"Prompt-only mode: printing chunk {chunk_idx}/{len(chunks)}")
            print(f"\n### CHUNK {chunk_idx}/{len(chunks)} ###\n")
            print(prompt)
            continue

        if args.response_json_in:
            _log("REPLAY", f"Loading saved response from {args.response_json_in}")
            result, loaded_raw_text = _load_response_snapshot(args.response_json_in)
            _log("REPLAY", "Loaded saved response successfully")
            if args.raw_response_out:
                out_path = _chunked_path(args.raw_response_out, chunk_idx, len(chunks)) if len(chunks) > 1 else args.raw_response_out
                _save_response_snapshot(
                    output_path=out_path,
                    input_path=args.input,
                    model=args.model,
                    prompt=prompt,
                    raw_text=loaded_raw_text or json.dumps(result, ensure_ascii=False),
                    parsed_json=result,
                )
                _log("SNAPSHOT", f"Saved replay snapshot to {out_path}")
        else:
            _log("API", f"Calling Gemini model {args.model} for chunk {chunk_idx}/{len(chunks)}")
            try:
                raw_text = _call_gemini_with_raw(prompt=prompt, model=args.model, api_key=args.api_key)
                _log("API", f"Received response for chunk {chunk_idx}/{len(chunks)} ({len(raw_text):,} chars)")
                result = _extract_json(raw_text)
            except ValueError as exc:
                message = str(exc)
                if "Failed to parse JSON from Gemini response" in message:
                    _log("PARSE", f"Parse failed on chunk {chunk_idx}/{len(chunks)}; capturing raw response")
                    raw_text = locals().get("raw_text", "") or ""
                    raw_error_path = _save_raw_error_response_for_chunk(
                        args.raw_error_dir,
                        raw_text,
                        chunk_idx,
                        len(chunks),
                    )
                    _log("PARSE", f"Saved unparseable raw response to {raw_error_path}")
                    raise ValueError(
                        f"{message}. Raw response saved to {raw_error_path}. "
                        "Retry this chunk with a lower --max-input-chars."
                    ) from exc
                raise

            if args.raw_response_out:
                out_path = _chunked_path(args.raw_response_out, chunk_idx, len(chunks)) if len(chunks) > 1 else args.raw_response_out
                _save_response_snapshot(
                    output_path=out_path,
                    input_path=args.input,
                    model=args.model,
                    prompt=prompt,
                    raw_text=raw_text,
                    parsed_json=result,
                )
                _log("SNAPSHOT", f"Saved Gemini response snapshot to {out_path}")

        _log("TRANSFORM", f"Processing parsed payload for chunk {chunk_idx}/{len(chunks)}")
        row_updates = result.get("row_updates") or []
        alias_groups = result.get("alias_groups") or []
        _log(
            "TRANSFORM",
            f"Chunk {chunk_idx}/{len(chunks)} has {len(row_updates) if isinstance(row_updates, list) else 0} row updates and {len(alias_groups) if isinstance(alias_groups, list) else 0} alias groups",
        )

        if isinstance(row_updates, list):
            for item in row_updates:
                row_id = str(item.get("row_id") or "").strip()
                if not row_id:
                    source_venue = str(item.get("source_venue") or "").strip().lower()
                    source_city = str(item.get("source_city") or "").strip().lower()
                    row_id = row_lookup.get((source_venue, source_city), "")
                if not row_id:
                    continue
                updates_by_row_id[row_id] = {
                    "suggested_city": str(item.get("suggested_city") or "").strip(),
                    "suggested_country": str(item.get("suggested_country") or "").strip(),
                }

        if isinstance(alias_groups, list):
            alias_groups_merged.extend(alias_groups)

    if args.prompt_only:
        return 0

    _log("WRITE", f"Writing row suggestions to {args.output_updates}")
    _write_updates_csv(rows, updates_by_row_id, args.output_updates)
    alias_rows = 0
    _log("WRITE", f"Writing alias candidates to {args.output_aliases}")
    alias_rows = _write_aliases_csv(alias_groups_merged, args.output_aliases)

    _log(
        "DONE",
        f"Wrote {len(rows)} row suggestions to {args.output_updates}; wrote {alias_rows} alias candidate rows to {args.output_aliases}",
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
