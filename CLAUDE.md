# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current Project Notes (2026-04-13)

- `queue-setup` is the canonical setup command after `bootstrap` and `import-authors`. `validate-setup`, `dates-setup`, and `journals-setup` are compatibility aliases that print a message pointing to `queue-setup`.
- Pipeline state lives in `utb_processing_queue`, including validation, author/date heuristics, LLM results, journal normalization, and librarian workflow columns.
- The `/pipeline` UI is catalog-driven via `web/blueprints/pipeline/catalog.py`; it supports command help, per-option controls, single/bulk runs, scheduled runs, and schedule deletion.
- Pipeline schedules are stored in `data/pipeline_schedules.json`; the scheduler thread in `web/blueprints/pipeline/routes.py` executes due command batches.
- Author matching now handles normalized punctuation/diacritics, reversed names, WoS surname+initial formats, and refuses ambiguous initials/surname-only cases instead of letting fuzzy matching guess.
- Date parsing maps `Revised`, `Resubmitted`, ordinal revisions, and `Prepracovano` to `utb_date_reviewed`.
- Journal normalization tries Crossref Works by DOI before ISSN/ISBN fallback APIs. The detail Crossref UI also uses `https://api.crossref.org/works/{doi}`.
- Detail UI proposals prefer LLM output before validation/heuristic fallbacks, and `Ctrl+S`/`Cmd+S` saves changes to the librarian queue.
- Current verification baseline: `uv run python -m pytest` -> 180 passed.

## Commands

```bash
# Install dependencies
uv sync --all-groups

# Run all tests
uv run python -m pytest

# Run a single test file
uv run python -m pytest tests/test_validation.py -v

# Run a single test class or function
uv run python -m pytest tests/test_validation.py::TestTrailingSpaces -v

# Run the CLI
uv run python -m src.cli <COMMAND> [OPTIONS]
```

## Pipeline Execution Order

```bash
uv run python -m src.cli bootstrap              # Copy remote table to local DB
uv run python -m src.cli import-authors         # Load internal author registry from CSV into local DB
uv run python -m src.cli queue-setup            # Create/sync utb_processing_queue (run after import-authors)
uv run python -m src.cli validate               # Run data quality checks + generate suggested fixes
uv run python -m src.cli apply-fixes            # Apply suggested fixes (--preview to preview first)
uv run python -m src.cli heuristics             # Match internal authors via WoS affiliation
uv run python -m src.cli dates                  # Parse publication dates from fulltext field
uv run python -m src.cli heuristics-llm         # LLM fallback for unmatched author records
uv run python -m src.cli dates-llm              # LLM fallback for unparsed dates
uv run python -m src.cli journals-lookup        # Normalize publisher/ispartof proposals
uv run python -m src.cli journals-apply         # Apply approved publisher/ispartof proposals
uv run python -m src.cli dedup-setup            # Create dedup_histoire table (run once)
uv run python -m src.cli deduplicate            # Find, merge duplicates; history in dedup_histoire
uv run python -m src.cli export --output results.csv
```

## Architecture

The pipeline processes scholarly publication metadata from a remote PostgreSQL DB at Tomas Bata University (UTB). Data flows through phases: validate -> author/date heuristics -> LLM fallback -> journal normalization -> deduplicate -> export.

### Module Map

```
src/
  cli/__main__.py              – Typer CLI entry point; thin wrappers around runner functions
  config/settings.py           – Settings dataclass (env vars via pydantic-settings)
  common/constants.py          – HeuristicStatus, LLMStatus, ValidationStatus, DateLLMStatus,
                                  FlagKey, FACULTIES, DEPARTMENTS, WOS_ABBREV_MAP, OUTPUT_COLUMNS

  db/
    engines.py                 – get_local_engine(), get_remote_engine(), test_connection()
    setup.py                   – run_bootstrap() – copies remote table to local DB

  authors/
    registry.py                – InternalAuthor, MatchResult, load_authors_from_csv(),
                                  import_authors_to_db(), get_author_registry(),
                                  match_author(), lookup_author_affiliations()
    heuristics.py              – run_heuristics() – main author-matching runner
                                  resolve_faculty_and_ou() – WoS text → faculty + OU
    parsers/
      wos.py                   – parse_wos_affiliation(), detect_utb_affiliation(),
                                  extract_ou_candidates(), normalize_text()
      scopus.py                – parse_scopus_affiliation()

  dates/
    labels.py                  – LABEL_MAP, DateCategory, normalize_label(), match_label()
    parser.py                  – parse_fulltext_dates(), ParsedDates, DateEntry
                                  _try_parse_dot_both() – tries both DMY and MDY interpretations
                                  resolve_mdr_format()  – confidence-based format resolution
    heuristics.py              – run_date_heuristics(), setup_date_columns(), print_date_status()

  quality/
    checks.py                  – validate_record(), run_validation(), run_apply_fixes(),
                                  setup_validation_columns(), check_obdid_batch(),
                                  fix_mojibake(), fix_doi(), fix_url(), _fix_text_str()
    dedup.py                   – run_deduplication(), find_duplicates_by_column(),
                                  find_content_duplicates(), find_duplicates_fuzzy(),
                                  setup_dedup_table(), _merge_pair(), _copy_to_history()

  llm/
    client.py                  – LLMClient (ABC), OllamaClient, CloudLLMCompatibleClient,
                                  get_llm_client(), parse_llm_json_output()
    session.py                 – LLMSession, create_authors_session(), create_dates_session()
    tasks/
      authors.py               – LLMAuthorEntry, LLMResult, AUTHORS_JSON_SCHEMA,
                                  SYSTEM_PROMPT, build_user_message(), run_llm()
      dates.py                 – DateLLMResult, DATES_JSON_SCHEMA,
                                  DATES_SYSTEM_PROMPT, build_date_user_message(), run_date_llm()
```

### Two-Phase Author Matching (`src/authors/heuristics.py`)

- **Path A (WoS records):** Parse WoS-format affiliation blocks `[Authors] Institution;`, extract UTB blocks, match authors from `utb_internal_authors` registry. For each matched author, faculty/OU is resolved: WoS text has priority; if WoS faculty doesn't match the author's faculty in remote DB, a `wos_faculty_not_in_registry` flag is written. OU falls back to remote DB when WoS doesn't specify it.
- **Path B (no WoS):** Direct fuzzy match against `dc.contributor.author` field; faculty/OU taken from remote DB per author. If an author belongs to multiple faculties and WoS provides no context, a `multiple_faculties_ambiguous` flag is written for manual resolution by the librarian.
- Author matching uses local `utb_internal_authors` table (populated by `import-authors` from CSV). Affiliation lookup queries remote DB individually per matched author (cached per run).
- Records with unmatched UTB authors in Path A get `needs_llm=TRUE` for LLM processing.
- `match_author()` first tries exact/normalized/reversed-name and surname+initial matches. Ambiguous initial or surname-only candidates return non-match (`ambiguous_initials` / `ambiguous_surname`) instead of falling through to fuzzy guessing.

### MDR Format Resolver (`src/dates/parser.py`)

Resolves ambiguous dot-dates (A.B.YYYY – DMY vs MDY):
1. `_try_parse_dot_both()` tries both European (DD.MM) and American (MM.DD) interpretations
2. `resolve_mdr_format()` determines confidence:
   - **HIGH**: value > 12 forces one interpretation
   - **MEDIUM**: only one ordering satisfies Received ≤ Accepted ≤ Published (librarian flag written)
   - **LOW**: both interpretations chronologically consistent → `needs_llm=True` + librarian flag
   - **INVALID**: no valid interpretation or conflicting forced formats → `needs_llm=True`

### Validation Pipeline (`src/quality/checks.py`)

`_fix_text_str()` applies fixes in order: ftfy (mojibake) → char map (PUA/encoding) → standalone diacritics → nbsp → double space → strip. Returns `(fixed, [fix_types])`.

`validate_record()` returns `(status, issues, suggested_fixes)`. Fixes are stored in `validation_suggested_fixes` JSONB for `apply-fixes` to consume.

### LLM Clients (`src/llm/client.py`) and Session (`src/llm/session.py`)

Two client implementations behind a common interface:
- **OllamaClient**: Uses `format: json_schema` for structured output; supports preamble for KV-cache sharing across requests
- **CloudLLMCompatibleClient**: OpenAI-compatible; structured output fallback chain: JSON Schema → function calling → `json_object`; retry logic for 429/5xx errors

`LLMSession` wraps a client with a fixed system prompt and JSON schema. Each `ask()` call is stateless (no history accumulation between records).

### Status / Flag System (`src/common/constants.py`)

Each processing phase writes to dedicated columns:
- `heuristic_status` / `date_heuristic_status`: `not_processed | processed | error`
- `llm_status` / `date_llm_status`: `not_processed | processed | error | validation_error`
- `validation_status`: `not_checked | ok | has_issues`
- `flags` / `validation_flags` / `date_flags`: JSONB dicts with structured issue details (`FlagKey` constants)

### Deduplication (`src/quality/dedup.py`)

Three phases:
1. Exact match on selected column (default: DOI)
2. Content match 100% (title + authors + abstract) → categorize as `early_access`, `merged_type`, `autoplagiat`, `exact:content`
3. Fuzzy title match (Jaro-Winkler ≥ threshold, year-blocked)

`exact` / `early_access` / `merged_type` → physical merge (copy both to `dedup_histoire`, UPDATE kept, DELETE duplicate).
`autoplagiat` / `fuzzy_title` → flag only (write to `flags['duplicates']` JSONB).

### Column Setup Pattern

`queue-setup` creates/syncs `utb_processing_queue` and is the canonical idempotent setup command for validation, author/date processing, LLM result columns, journal normalization, and librarian workflow state. Older setup commands (`validate-setup`, `dates-setup`, `journals-setup`) are retained as compatibility aliases but do not add columns themselves.

## DB Columns Added by Each Setup Command

**`queue-setup`:**
- `validation_status` TEXT DEFAULT 'not_checked'
- `validation_flags` JSONB DEFAULT '{}'
- `validation_suggested_fixes` JSONB DEFAULT '{}'
- `validation_version` TEXT
- `validation_checked_at` TIMESTAMPTZ
- author columns: `author_heuristic_status`, `author_internal_names`, `author_faculty`, `author_ou`, `author_flags`, `author_llm_result`, `author_llm_status`
- `utb_date_received`, `utb_date_reviewed`, `utb_date_accepted`, `utb_date_published_online`, `utb_date_published` – DATE
- `utb_date_extra` – JSONB
- `date_heuristic_status` TEXT DEFAULT 'not_processed'
- `date_needs_llm` BOOLEAN DEFAULT FALSE
- `date_flags` JSONB DEFAULT '{}'
- `date_heuristic_version`, `date_processed_at`, `date_llm_status`, `date_llm_processed_at`, `date_llm_result`
- journal columns: `journal_norm_status`, `journal_norm_proposed_publisher`, `journal_norm_proposed_ispartof`, `journal_norm_api_source`, `journal_norm_issn_key`, `journal_norm_version`, `journal_norm_processed_at`
- librarian workflow columns: `librarian_checked_at`, `created_at`, `updated_at`

**`dedup-setup`:** Creates `dedup_histoire` table (same structure as source + dedup metadata columns).

## Configuration

Copy `.env.example` to `.env`. Key variables:

```env
# Remote DB (read-only)
REMOTE_DB_HOST, REMOTE_DB_PORT, REMOTE_DB_NAME, REMOTE_DB_USER, REMOTE_DB_PASSWORD

# Local DB (write)
LOCAL_DB_HOST, LOCAL_DB_PORT, LOCAL_DB_NAME, LOCAL_DB_USER, LOCAL_DB_PASSWORD

# LLM (choose one provider)
LLM_PROVIDER=ollama          # or "openai"
LOCAL_LLM_BASE_URL=http://localhost:11434
LOCAL_LLM_MODEL=qwen3:8b
OPENAI_API_KEY=...
OPENAI_BASE_URL=https://api.groq.com/openai/v1
OPENAI_MODEL=llama-3.3-70b-versatile

# Thresholds
AUTHOR_MATCH_THRESHOLD=0.80
FUZZY_DEDUP_THRESHOLD=0.85
```

## Pending TODOs

- Decide whether runtime schedule state (`data/pipeline_schedules.json`) should be ignored or excluded from commits.
- After real-data runs, review author cases flagged as ambiguous initials/surname-only and compare with `heuristics-compare`.
- Before DB migration in an older environment, verify `date_llm_result` has been migrated from legacy TEXT to JSONB by `queue-setup`.
