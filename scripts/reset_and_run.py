"""
reset_and_run.py

Drops all local pipeline tables, then runs the full pipeline:
  bootstrap → import-authors → queue-setup → dedup-setup
  → validate → heuristics → dates → deduplicate

LLM steps (heuristics-llm, dates-llm) are skipped by default.
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

# Ensure project root is on sys.path when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from src.config.settings import settings
from src.db.engines import get_local_engine


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def step(msg: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


def run(cmd: list[str]) -> None:
    """Run a CLI command; exit on failure."""
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print(f"\n[ERROR] Command failed with exit code {result.returncode}")
        sys.exit(result.returncode)


def cli(*args: str) -> None:
    """Shortcut: uv run python -m src.cli <args>"""
    run([sys.executable, "-m", "src.cli", *args])


# ──────────────────────────────────────────────────────────────
# Drop tables
# ──────────────────────────────────────────────────────────────

TABLES_TO_DROP = [
    "utb_processing_queue",
    "dedup_histoire",
    "utb_internal_authors",
    "utb_metadata_arr",
]


def drop_tables() -> None:
    step("Dropping all local pipeline tables")
    engine = get_local_engine()
    schema = settings.local_schema
    with engine.begin() as conn:
        for table in TABLES_TO_DROP:
            conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE'))
            print(f"  dropped: {schema}.{table}")
    print("[OK] All tables dropped.")


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reset local DB and run the full pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=dedent("""\
            Pipeline steps (in order):
              1. drop tables
              2. bootstrap        – copy remote → local
              3. import-authors   – load CSV author registry
              4. queue-setup      – create utb_processing_queue
              5. dedup-setup      – create dedup_histoire
              6. validate         – data quality checks
              7. heuristics       – author matching (WoS/Scopus)
              8. dates            – publication date parsing
              9. deduplicate      – find & merge duplicates
             10. heuristics-llm  – LLM author fallback  (--llm only)
             11. dates-llm       – LLM date fallback    (--llm only)
        """),
    )
    parser.add_argument(
        "--llm",
        action="store_true",
        help="Also run LLM fallback steps (heuristics-llm, dates-llm)",
    )
    parser.add_argument(
        "--skip-drop",
        action="store_true",
        help="Skip dropping tables (useful if bootstrap already ran)",
    )
    args = parser.parse_args()

    # 1. Drop
    if not args.skip_drop:
        drop_tables()
    else:
        step("Skipping table drop (--skip-drop)")

    # 2. Bootstrap (copies remote table to local)
    step("bootstrap – copying remote table to local DB")
    cli("bootstrap")

    # 3. Import authors from CSV
    step("import-authors – loading author registry from CSV")
    cli("import-authors")

    # 4. Create queue table
    step("queue-setup – creating utb_processing_queue")
    cli("queue-setup")

    # 5. Create dedup history table
    step("dedup-setup – creating dedup_histoire")
    cli("dedup-setup")

    # 6. Validate
    step("validate – running data quality checks")
    cli("validate")

    # 7. Author heuristics
    step("heuristics – matching internal authors")
    cli("heuristics")

    # 8. Date heuristics
    step("dates – parsing publication dates")
    cli("dates")

    # 9. Deduplicate
    step("deduplicate – finding and merging duplicates")
    cli("deduplicate")

    # 10–11. LLM steps (optional)
    if args.llm:
        step("heuristics-llm – LLM fallback for unmatched authors")
        cli("heuristics-llm")

        step("dates-llm – LLM fallback for unparsed dates")
        cli("dates-llm")

    step("DONE")
    print("\nAll pipeline steps completed successfully.")
    print("You can now run:  uv run python app.py")


if __name__ == "__main__":
    main()
