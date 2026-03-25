# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync --all-groups

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_validation.py -v

# Run a single test class or function
uv run pytest tests/test_validation.py::TestTrailingSpaces -v

# Run the CLI
uv run python -m src.cli <COMMAND> [OPTIONS]
```

## Pipeline Execution Order

```bash
uv run python -m src.cli bootstrap          # Copy remote table to local DB
uv run python -m src.cli import-authors     # Load internal author registry from CSV
uv run python -m src.cli validate-setup     # Add validation columns (run once)
uv run python -m src.cli validate           # Run data quality checks
uv run python -m src.cli dates-setup        # Add date columns (run once)
uv run python -m src.cli heuristics         # Match internal authors via WoS affiliation
uv run python -m src.cli dates              # Parse publication dates from fulltext field
uv run python -m src.cli heuristics-llm     # LLM fallback for unmatched author records
uv run python -m src.cli dates-llm          # LLM fallback for unparsed dates
uv run python -m src.cli deduplicate        # Find and mark duplicate records
uv run python -m src.cli export --output results.csv
```

## Architecture

The pipeline processes scholarly publication metadata from a remote PostgreSQL DB at Tomas Bata University (UTB). Data flows through phases: import → validate → heuristics → LLM fallback → deduplicate → export.

### Two-Phase Author Matching (`src/authors/heuristics.py`)

- **Path A (WoS records):** Parse WoS-format affiliation blocks `[Authors] Institution;`, extract UTB blocks, match authors from registry
- **Path B (no WoS):** Direct fuzzy match against `dc.contributor.author` field
- Records where heuristics fail or are ambiguous get `needs_llm=TRUE` for LLM processing

### LLM Clients (`src/llm/client.py`) and Session (`src/llm/session.py`)

Two client implementations behind a common interface:
- **OllamaClient**: Uses `format: json_schema` for structured output; supports preamble for KV-cache sharing across requests
- **CloudLLMCompatibleClient**: OpenAI-compatible; structured output fallback chain: JSON Schema → function calling → `json_object`; retry logic for 429/5xx errors

`LLMSession` (in `src/llm/session.py`) wraps a client with a fixed system prompt and JSON schema. Each `ask()` call is stateless (no history accumulation between records).

### Status / Flag System (`src/common/constants.py`)

Each processing phase writes to dedicated columns:
- `heuristic_status` / `date_heuristic_status`: `not_processed | processed | error`
- `llm_status` / `date_llm_status`: `not_processed | processed | error | validation_error`
- `validation_status`: `not_checked | ok | has_issues`
- `flags` / `validation_flags` / `date_flags`: JSONB dicts with structured issue details (`FlagKey` constants)

### Deduplication (`src/quality/dedup.py`)

Two-phase: exact match (default on DOI) then optional fuzzy title match (Jaro-Winkler ≥ threshold, year-blocked). Both records in a duplicate pair store mutual references in `flags['duplicates']`.

### Column Setup Pattern

`validate-setup` and `dates-setup` commands add columns via `ALTER TABLE IF NOT EXISTS` (idempotent). Always run setup commands before their corresponding processing commands.

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
