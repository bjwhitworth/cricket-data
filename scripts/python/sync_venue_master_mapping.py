#!/usr/bin/env python3
"""Synchronize seeds/venue_master_mapping.csv from curated venue and alias seeds.

What this script does:
1) Preserves existing venue IDs in venue_master_mapping.
2) Propagates city/country corrections into existing master rows where safe.
3) Appends new canonical venue-city-country triples with new stable `ven_` IDs.

Safe update rules for existing rows:
- Exact triple match: keep as-is.
- If existing row has missing or stale city/country, and canonical venue maps to exactly
  one curated triple across inputs, update that row in place (ID preserved).
- If canonical venue maps to multiple curated triples (ambiguous), skip auto-update and
  surface in dry-run output for manual review.

Inputs:
- seeds/venue_country_mapping.csv
- seeds/venue_alias_mapping.csv (approved rows only)
- seeds/venue_master_mapping.csv
"""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path


MASTER_FIELDS = [
    "venue_id",
    "canonical_venue",
    "canonical_city",
    "canonical_country",
]


@dataclass(frozen=True)
class Triple:
    venue: str
    city: str
    country: str

    @property
    def key(self) -> tuple[str, str, str]:
        return (_norm(self.venue), _norm(self.city), _norm(self.country))


@dataclass
class SyncResult:
    updated_rows: int
    appended_rows: int
    ambiguous_rows: int


def _norm(value: str | None) -> str:
    return (value or "").strip().lower()


def _clean(value: str | None) -> str:
    return (value or "").strip()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [{k: (v or "") for k, v in row.items()} for row in reader]


def _write_master(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MASTER_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _approved_alias_row(status: str) -> bool:
    s = _norm(status)
    return s.startswith("approved")


def _load_curated_triples(country_path: Path, alias_path: Path) -> set[Triple]:
    triples: set[Triple] = set()

    for row in _read_csv(country_path):
        venue = _clean(row.get("venue"))
        city = _clean(row.get("city"))
        country = _clean(row.get("country"))
        if venue and city and country:
            triples.add(Triple(venue=venue, city=city, country=country))

    for row in _read_csv(alias_path):
        if not _approved_alias_row(row.get("review_status", "")):
            continue
        venue = _clean(row.get("canonical_venue"))
        city = _clean(row.get("canonical_city"))
        country = _clean(row.get("canonical_country"))
        if venue and city and country:
            triples.add(Triple(venue=venue, city=city, country=country))

    return triples


def _extract_max_venue_id(master_rows: list[dict[str, str]]) -> int:
    max_num = 0
    pattern = re.compile(r"^ven_(\d+)$")
    for row in master_rows:
        venue_id = _clean(row.get("venue_id"))
        match = pattern.match(venue_id)
        if match:
            max_num = max(max_num, int(match.group(1)))
    return max_num


def _build_venue_to_triples(curated: set[Triple]) -> dict[str, list[Triple]]:
    out: dict[str, list[Triple]] = {}
    for triple in curated:
        out.setdefault(_norm(triple.venue), []).append(triple)
    return out


def sync_master(master_rows: list[dict[str, str]], curated: set[Triple]) -> tuple[list[dict[str, str]], SyncResult]:
    curated_by_key = {t.key: t for t in curated}
    curated_by_venue = _build_venue_to_triples(curated)

    updated_rows = 0
    ambiguous_rows = 0

    updated_master: list[dict[str, str]] = []
    master_keys_seen: set[tuple[str, str, str]] = set()

    for row in master_rows:
        venue_id = _clean(row.get("venue_id"))
        canonical_venue = _clean(row.get("canonical_venue"))
        canonical_city = _clean(row.get("canonical_city"))
        canonical_country = _clean(row.get("canonical_country"))

        current_key = (_norm(canonical_venue), _norm(canonical_city), _norm(canonical_country))

        if current_key in curated_by_key:
            updated_master.append(
                {
                    "venue_id": venue_id,
                    "canonical_venue": canonical_venue,
                    "canonical_city": canonical_city,
                    "canonical_country": canonical_country,
                }
            )
            master_keys_seen.add(current_key)
            continue

        venue_key = _norm(canonical_venue)
        candidates = curated_by_venue.get(venue_key, [])

        if len(candidates) == 1:
            target = candidates[0]
            target_key = target.key
            updated_master.append(
                {
                    "venue_id": venue_id,
                    "canonical_venue": target.venue,
                    "canonical_city": target.city,
                    "canonical_country": target.country,
                }
            )
            master_keys_seen.add(target_key)

            if (_norm(canonical_city), _norm(canonical_country)) != (_norm(target.city), _norm(target.country)):
                updated_rows += 1
        else:
            if len(candidates) > 1:
                ambiguous_rows += 1
            updated_master.append(
                {
                    "venue_id": venue_id,
                    "canonical_venue": canonical_venue,
                    "canonical_city": canonical_city,
                    "canonical_country": canonical_country,
                }
            )
            master_keys_seen.add(current_key)

    max_id = _extract_max_venue_id(updated_master)
    appended_rows = 0

    missing_curated = [t for t in sorted(curated, key=lambda x: (x.venue, x.city, x.country)) if t.key not in master_keys_seen]
    for triple in missing_curated:
        max_id += 1
        updated_master.append(
            {
                "venue_id": f"ven_{max_id:06d}",
                "canonical_venue": triple.venue,
                "canonical_city": triple.city,
                "canonical_country": triple.country,
            }
        )
        master_keys_seen.add(triple.key)
        appended_rows += 1

    return updated_master, SyncResult(
        updated_rows=updated_rows,
        appended_rows=appended_rows,
        ambiguous_rows=ambiguous_rows,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync venue master mapping from curated seeds")
    parser.add_argument(
        "--country-seed",
        default="seeds/venue_country_mapping.csv",
        help="Path to venue_country_mapping.csv",
    )
    parser.add_argument(
        "--alias-seed",
        default="seeds/venue_alias_mapping.csv",
        help="Path to venue_alias_mapping.csv",
    )
    parser.add_argument(
        "--master-seed",
        default="seeds/venue_master_mapping.csv",
        help="Path to venue_master_mapping.csv",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write updates in place. Without this flag, runs dry-run only.",
    )
    parser.add_argument(
        "--preview-limit",
        type=int,
        default=10,
        help="How many appended rows to print in dry-run preview.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()

    country_path = Path(args.country_seed)
    alias_path = Path(args.alias_seed)
    master_path = Path(args.master_seed)

    curated = _load_curated_triples(country_path, alias_path)
    master_rows = _read_csv(master_path)

    updated_master, result = sync_master(master_rows, curated)

    print("Venue master sync summary")
    print(f"- curated_triples: {len(curated)}")
    print(f"- existing_master_rows: {len(master_rows)}")
    print(f"- updated_master_rows: {len(updated_master)}")
    print(f"- in_place_city_country_updates: {result.updated_rows}")
    print(f"- appended_new_ids: {result.appended_rows}")
    print(f"- ambiguous_venue_name_rows_skipped: {result.ambiguous_rows}")

    if not args.apply:
        if result.appended_rows > 0 and args.preview_limit > 0:
            original_ids = {_clean(r.get("venue_id")) for r in master_rows}
            appended_preview = [
                r
                for r in updated_master
                if _clean(r.get("venue_id")) not in original_ids
            ]
            print("- appended_preview:")
            for row in appended_preview[: args.preview_limit]:
                print(
                    "  "
                    + f"{row['venue_id']} | {row['canonical_venue']} | {row['canonical_city']} | {row['canonical_country']}"
                )
        print("Dry run complete. Re-run with --apply to write changes.")
        return 0

    _write_master(master_path, updated_master)
    print(f"Applied updates to {master_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
