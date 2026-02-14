# Data Quality Issues

Known data quality issues in the cricket dataset, discovered through dbt testing.

## Source Data Gaps

### Missing City Data (1,649 matches)

**Discovered:** 2026-01-09  
**Affected Models:** `stg_cricket__matches`, `int_cricket__matches_flattened`  
**Severity:** Low - venue data is always present

**Description:**  
The `city` field is null for approximately 1,649 matches in the dataset. The source JSON files don't consistently include city information, even though venue names are always provided.

**Examples:**
- Match 1130664 (2017/18 season, Arun Jaitley Stadium)
- Match 1226944 (2020/21 season, Sydney Showground Stadium)
- Match 1280297 (2021/22 season, Arun Jaitley Stadium)

**Resolution:**  
Marked `city` as nullable in schema tests. Venue data provides sufficient location context for most analytical use cases. If city is critical, consider enriching from venue lookup table.

---

### Missing Umpire Data (914 matches)

**Discovered:** 2026-01-09  
**Affected Models:** `stg_cricket__matches`, `int_cricket__matches_flattened`  
**Severity:** Medium - impacts officiating analysis

**Description:**  
The `umpires` array field is null for 914 matches. This appears to be more common in certain competitions or seasons (e.g., 2017/18 season matches).

**Examples:**
- Match 1130664 (2017/18, Arun Jaitley Stadium)
- Match 1280297 (2021/22, Arun Jaitley Stadium)
- Match 1380586 (2023, Scott Page Field, Vinor)

**Pattern:**  
Many affected matches appear to be from domestic/franchise competitions where umpire data may not have been consistently recorded in the source.

**Resolution:**  
Marked `umpires` as nullable in schema tests. Consider investigating if specific competitions/seasons are more affected.

---

### Incomplete Innings Data (13 innings)

**Discovered:** 2026-01-09  
**Affected Models:** `stg_cricket__innings`  
**Severity:** Low - represents edge cases

**Description:**  
13 innings have null values for `recorded_over_count`, `runs_total`, `runs_extras`, and `wickets_fallen`. These appear to be forfeited, abandoned, or incomplete innings that exist in the data structure but have minimal delivery records (typically 1 delivery).

**Examples:**
- Match 1410315, innings 2 (Gloucestershire) - 1 delivery
- Match 1166966, innings 3 (Somerset) - 1 delivery
- Match 1160280, innings 2 (Canterbury) - 1 delivery

**Pattern:**  
All affected innings have exactly 1 delivery recorded, suggesting these may be:
- Forfeited innings
- Rain-abandoned innings
- Data recording artifacts
- Declaration scenarios

**Resolution:**  
Marked innings aggregate fields (`recorded_over_count`, `runs_total`, `runs_extras`, `wickets_fallen`) as nullable. Downstream analysis should filter these out using `WHERE recorded_over_count IS NOT NULL` if complete innings are required.

---

## Future Investigations

### Potential Data Enrichment Opportunities

1. **City backfill:** Cross-reference venue names with a canonical venue-to-city mapping table
2. **Umpire data:** Investigate if umpire information can be sourced from alternative APIs or datasets
3. **Incomplete innings classification:** Add a derived field to flag incomplete/forfeited innings explicitly

### Test Coverage Gaps

- Consider adding relationship tests between teams in `participating_teams` and `team_1`/`team_2`
- Validate `runs_total = runs_batter + runs_extras` at delivery level
- Check delivery/over numbering sequences for gaps

---

## Notes

All issues documented here have been incorporated into the dbt schema definitions and tests. Tests are configured to allow these null values while still validating critical fields like match_id, venue, teams, and core delivery data.

**Last Updated:** 2026-01-09


# TO ADD:

- innings context data is rudimentary at the moment -- missing edge cases like declarations, follow-ons, etc.
- verify versioning of 'most recent' narrative when we have pulled a bunch of batches in -- double filtering?
- check descriptionconfig and json imports into `batch_match_descriptions.py`
- consider adding tests for relationship between `team_1`/`team_2` and `participating_teams` array
- Consider adding tests for configuration retrieval, JSON blob creation, and database storage functionality.
- Exceptions in `batch_match_descriptions_api.py`: The function should either: (1) maintain a count of failures and report it in the summary (lines 225-226), or (2) raise the exception after cleanup to ensure the user is aware of preparation failures, especially for critical production batches.
- check union all on batter/partner to ensure correctness
- create over x delivery index and fix a bunch of partnership start/end stuff