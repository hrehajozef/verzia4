"""
reset_and_run.py

Drops local pipeline tables, then runs the current end-to-end pipeline:
  bootstrap-local-db -> setup-processing-queue -> setup-dedup-history
  -> validate-metadata -> detect-authors -> extract-dates
  -> normalize-journals -> deduplicate-records

LLM fallback steps (detect-authors-llm, extract-dates-llm) are skipped by default.
Pass --llm to include them.

Usage:
    uv run python scripts/reset_and_run.py
    uv run python scripts/reset_and_run.py --llm
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

# Ensure project root is on sys.path when running as a script.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from src.config.settings import settings
from src.db.engines import get_local_engine


def step(msg: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {msg}")
    print(f"{'=' * 60}")


def run(cmd: list[str]) -> None:
    """Run a CLI command; exit on failure."""
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print(f"\n[ERROR] Command failed with exit code {result.returncode}")
        sys.exit(result.returncode)


def cli(*args: str) -> None:
    """Shortcut: uv run python -m src.cli <args>."""
    run([sys.executable, "-m", "src.cli", *args])


TABLES_TO_DROP = [
    "utb_processing_queue",
    "dedup_histoire",
    "utb_metadata_arr",
]


def drop_tables() -> None:
    step("Dropping local pipeline tables")
    engine = get_local_engine()
    schema = settings.local_schema
    with engine.begin() as conn:
        for table in TABLES_TO_DROP:
            conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE'))
            print(f"  dropped: {schema}.{table}")
    print("[OK] All selected tables dropped.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reset local DB and run the current full pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=dedent(
            """\
            Pipeline steps (in order):
              1. drop tables
              2. bootstrap-local-db       - copy remote metadata to local DB
              3. setup-processing-queue   - create/update pipeline queue
              4. setup-dedup-history      - create deduplication history table
              5. validate-metadata        - run data quality checks
              6. detect-authors           - detect UTB internal authors
              7. extract-dates            - parse fulltext date fields
              8. normalize-journals       - propose journal/publisher values
              9. deduplicate-records      - find and merge duplicates
             10. detect-authors-llm       - LLM author fallback (--llm only)
             11. extract-dates-llm        - LLM date fallback   (--llm only)
            """
        ),
    )
    parser.add_argument(
        "--llm",
        action="store_true",
        help="Also run LLM fallback steps (detect-authors-llm, extract-dates-llm).",
    )
    parser.add_argument(
        "--skip-drop",
        action="store_true",
        help="Skip dropping tables.",
    )
    args = parser.parse_args()

    if not args.skip_drop:
        drop_tables()
    else:
        step("Skipping table drop (--skip-drop)")

    step("bootstrap-local-db - copying remote table to local DB")
    cli("bootstrap-local-db")

    step("setup-processing-queue - creating/updating utb_processing_queue")
    cli("setup-processing-queue")

    step("setup-dedup-history - creating dedup_histoire")
    cli("setup-dedup-history")

    step("validate-metadata - running data quality checks")
    cli("validate-metadata")

    step("detect-authors - matching internal authors")
    cli("detect-authors")

    step("extract-dates - parsing publication dates")
    cli("extract-dates")

    step("normalize-journals - proposing journal and publisher values")
    cli("normalize-journals")

    step("deduplicate-records - finding and merging duplicates")
    cli("deduplicate-records")

    if args.llm:
        step("detect-authors-llm - LLM fallback for unclear authors")
        cli("detect-authors-llm")

        step("extract-dates-llm - LLM fallback for unclear dates")
        cli("extract-dates-llm")

    step("DONE")
    print("\nAll pipeline steps completed successfully.")
    print("You can now run: uv run python app.py")


if __name__ == "__main__":
    main()
