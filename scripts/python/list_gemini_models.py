#!/usr/bin/env python3
"""List available Gemini model names for API usage.

Usage examples:
  python scripts/python/list_gemini_models.py
  python scripts/python/list_gemini_models.py --contains flash-lite
  python scripts/python/list_gemini_models.py --contains gemini-2.5 --show-all-fields
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Iterable

from google import genai


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "List model names available to your Gemini API key so you can copy the exact "
            "model string for calls like client.models.generate_content(...)."
        )
    )
    parser.add_argument(
        "--contains",
        default="",
        help="Only show models whose name contains this substring (case-insensitive).",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Gemini API key. If omitted, GEMINI_API_KEY environment variable is used.",
    )
    parser.add_argument(
        "--show-all-fields",
        action="store_true",
        help="Also print model display name and description when available.",
    )
    return parser.parse_args()


def resolve_api_key(cli_api_key: str | None) -> str:
    api_key = cli_api_key or os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("GEMINI_API_KEY is not set. Provide --api-key or export GEMINI_API_KEY.", file=sys.stderr)
        sys.exit(1)
    return api_key


def model_matches(model_name: str, contains: str) -> bool:
    if not contains:
        return True
    return contains.lower() in model_name.lower()


def iter_model_records(client: genai.Client) -> Iterable[object]:
    # client.models.list() returns a pager-like iterable.
    return client.models.list()


def main() -> int:
    args = parse_args()
    api_key = resolve_api_key(args.api_key)

    try:
        client = genai.Client(api_key=api_key)
    except Exception as exc:  # pragma: no cover - external service setup
        print(f"Failed to initialize Gemini client: {exc}", file=sys.stderr)
        return 1

    matched = 0
    try:
        for model in iter_model_records(client):
            name = getattr(model, "name", None)
            if not name or not model_matches(name, args.contains):
                continue

            matched += 1
            print(f"Use this string: {name}")

            if args.show_all_fields:
                display_name = getattr(model, "display_name", None)
                description = getattr(model, "description", None)
                if display_name:
                    print(f"  display_name: {display_name}")
                if description:
                    print(f"  description: {description}")

        if matched == 0:
            if args.contains:
                print(f"No models matched --contains '{args.contains}'.", file=sys.stderr)
            else:
                print("No models returned by the API.", file=sys.stderr)
            return 2
    except Exception as exc:  # pragma: no cover - external service call
        print(f"Failed to list models: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
