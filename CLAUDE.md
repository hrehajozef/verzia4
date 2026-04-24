# Project Notes For Coding Agents

Aktualizovane: 2026-04-20

## High-Level State

- The web app reviews UTB publication metadata and stores librarian edits in a pending-change buffer before final approval.
- Pipeline command UI lives under `/settings/pipeline`.
- There is no standalone `/pipeline` page anymore.
- The app no longer owns scheduling. Do not reintroduce schedule JSON files, scheduler threads, or schedule CRUD endpoints.
- Repeated runs should be handled by Windows Task Scheduler or Linux cron.
- The pipeline command catalog is in `web/blueprints/pipeline/catalog.py`.
- The SSE runner is in `web/blueprints/pipeline/routes.py`.

## Current CLI Commands

```bash
uv run python -m src.cli bootstrap-local-db
uv run python -m src.cli setup-processing-queue
uv run python -m src.cli setup-dedup-history
uv run python -m src.cli validate-metadata
uv run python -m src.cli apply-validation-fixes
uv run python -m src.cli metadata-validation-status
uv run python -m src.cli detect-authors
uv run python -m src.cli detect-authors-llm
uv run python -m src.cli compare-author-detection
uv run python -m src.cli author-detection-status
uv run python -m src.cli extract-dates
uv run python -m src.cli extract-dates-llm
uv run python -m src.cli date-extraction-status
uv run python -m src.cli normalize-journals
uv run python -m src.cli apply-journal-normalization
uv run python -m src.cli journal-normalization-status
uv run python -m src.cli deduplicate-records
uv run python -m src.cli deduplication-status
```

Old short command names and setup aliases were removed from the registered CLI surface.

## Pipeline Flow

Recommended order:

```bash
uv run python -m src.cli bootstrap-local-db
uv run python -m src.cli setup-processing-queue
uv run python -m src.cli setup-dedup-history
uv run python -m src.cli validate-metadata
uv run python -m src.cli detect-authors
uv run python -m src.cli extract-dates
uv run python -m src.cli normalize-journals
uv run python -m src.cli deduplicate-records
```

Optional LLM fallbacks:

```bash
uv run python -m src.cli detect-authors-llm
uv run python -m src.cli extract-dates-llm
```

## UI Notes

- `/record/<resource_id>` is the main detail page.
- Repository proposals prefer LLM results, then validation fixes, then heuristic/queue values.
- `Ctrl+S` on Windows/Linux and `Cmd+S` on macOS save current changes into the buffer.
- Author search scrollbars are intentionally wider and offset from row action buttons.
- Row order is user-configurable at `/settings/row-order`.

## Crossref

- DOI-level metadata uses `https://api.crossref.org/works/{doi}`.
- Journal normalization may also use ISSN/ISBN fallback sources.
- Crossref Works is preferred for DOI records because it provides richer work-level metadata than journal-only endpoints.

## Verification

Useful smoke checks:

```bash
uv run python -m src.cli --help
uv run python -m compileall web src scripts -q
uv run python -m pytest
```
