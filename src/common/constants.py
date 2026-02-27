"""Zdieľané konštanty pre pipeline spracovanie."""

from __future__ import annotations      # kvôli spätnej kompatibilite

from dataclasses import dataclass


class HeuristicStatus:
    NOT_PROCESSED = "not_processed"
    PROCESSED = "processed"
    ERROR = "error"


class LLMStatus:
    NOT_PROCESSED = "not_processed"
    PROCESSED = "processed"
    ERROR = "error"
    VALIDATION_ERROR = "validation_error"


class FlagKey:
    NO_WOS_DATA = "no_wos_data"
    PARSE_WARNINGS = "wos_parse_warnings"
    MULTIPLE_UTB_BLOCKS = "multiple_utb_blocks"
    UNMATCHED_UTB_AUTHORS = "utb_authors_unmatched"
    MATCHED_UTB_AUTHORS = "utb_authors_found_count"
    ERROR = "error"


DELIMITER = "||"


@dataclass(frozen=True)
class OutputColumn:
    name: str
    sql_type: str
    default_sql: str | None = None      # voliteľná default hodnota pre SQL definíciu


OUTPUT_COLUMNS: tuple[OutputColumn, ...] = (
    OutputColumn("flags", "JSONB", "'{}'::jsonb"),
    OutputColumn("heuristic_status", "TEXT", f"'{HeuristicStatus.NOT_PROCESSED}'"),
    OutputColumn("heuristic_version", "TEXT"),
    OutputColumn("heuristic_processed_at", "TIMESTAMPTZ"),
    OutputColumn("needs_llm", "BOOLEAN", "FALSE"),
    OutputColumn("utb_contributor_internalauthor", "TEXT"),
    OutputColumn("utb_faculty", "TEXT"),
    OutputColumn("utb_ou", "TEXT"),
    OutputColumn("llm_result", "JSONB"),
    OutputColumn("llm_status", "TEXT", f"'{LLMStatus.NOT_PROCESSED}'"),
    OutputColumn("llm_processed_at", "TIMESTAMPTZ"),
)
