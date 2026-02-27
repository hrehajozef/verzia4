from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.authors.internal import InternalAuthor, get_author_registry, match_author
from src.common.constants import DELIMITER, FlagKey, HeuristicStatus
from src.config.settings import settings
from src.db.engines import get_local_engine
from src.parsers.wos_affiliation import extract_ou, normalize_text, parse_wos_affiliation

# TODO: Pridať typer miesto print

HEURISTIC_VERSION = "2.1.0"

# Každá položka obsahuje tuple kľúčových slov a zodpovedajúci názov fakulty
# TODO: dopnit
FACULTY_RULES: tuple[tuple[tuple[str, ...], str], ...] = (
    (("fac technol", "dept polymer", "dept chem", "dept food", "dept phys", "vavreckova"), "Faculty of Technology"),
    (("fac management", "dept business", "dept econ", "mostni"), "Faculty of Management and Economics"),
    (("fac appl informat", "applied informatics"), "Faculty of Applied Informatics"),
    (("fac logist", "crisis management", "uherske hradiste"), "Faculty of Logistics and Crisis Management"),
    (("fac humanities", "dept pedag", "dept hlth"), "Faculty of Humanities"),
    (("fac multimedia",), "Faculty of Multimedia Communications"),
    (("ctr polymer syst", "univ inst", "nad ovcirnou", "tr t bati"), "University Institute"),
)


def _guess_faculty(affiliation_text: str) -> str:
    """
    Pokúsi sa heuristicky odhadnúť fakultu na základe textu z affiliation.
    Prehľadá text pre každou skupinou kľúčových slov a vráti zodpovedajúci názov fakulty, ak nájde zhodu
    """
    normalized = normalize_text(affiliation_text)   # bez diakritiky, malých písmen a zbytočných medzier
    for keywords, faculty_name in FACULTY_RULES:
        if any(keyword in normalized for keyword in keywords):
            return faculty_name
    return ""


def _join(items: list[str]) -> str | None:
    """
    Pomocná funkcia najprv odstráni prázdne položky a potom spojí položky zoznamu do stringu s oddeľovačom DELIMITER.
    """
    normalized = [item for item in items if item]
    return DELIMITER.join(normalized) if normalized else None


def process_record(
    resource_id: int,
    wos_aff_arr: list[str] | None,
    registry: list[InternalAuthor],
) -> dict:
    """
    Spracuje jeden záznam z lokálnej databázy na základe stĺpca ``utb.wos.affiliation`` a vráti slovník s výsledkami heuristík.
        - resource_id: ID záznamu, ktorý sa spracováva
        - wos_aff_arr: Pole s textami z WoS affiliation (môže být None nebo prázdne)
        - registry: Zoznam interných autorov pre porovnávanie
    """
    result = {
        "resource_id": resource_id,
        "heuristic_status": HeuristicStatus.ERROR,
        "heuristic_version": HEURISTIC_VERSION,
        "heuristic_processed_at": datetime.now(timezone.utc),
        "needs_llm": False,
        "utb_contributor_internalauthor": None,
        "utb_faculty": None,
        "utb_ou": None,
        "flags": {},
    }

    try:
        if not wos_aff_arr:
            result["heuristic_status"] = HeuristicStatus.PROCESSED
            result["flags"] = {FlagKey.NO_WOS_DATA: True}   # Ak záznam nemá nijaké dáta v stĺpci utb.wos.affiliation, označíme to flagom a ukončíme spracovanie
            return result

        matched_authors: list[str] = []
        matched_faculties: list[str] = []
        matched_ous: list[str] = []
        unmatched_utb_authors: list[str] = []
        warnings: list[str] = []
        needs_llm = False

        seen_authors: set[str] = set()
        for raw_affiliation in wos_aff_arr:
            if not raw_affiliation:
                continue
            parsed = parse_wos_affiliation(str(raw_affiliation))
            warnings.extend(parsed.warnings)
            if not parsed.ok:
                needs_llm = True

            for block in parsed.blocks:
                faculty = _guess_faculty(block.affiliation_raw)
                ou = extract_ou(block.affiliation_raw)
                for author in block.authors:
                    normalized = normalize_text(author)
                    if normalized in seen_authors:
                        continue
                    seen_authors.add(normalized)

                    match = match_author(author, registry, settings.author_match_threshold)
                    if match.matched and match.author:
                        matched_authors.append(match.author.full_name)
                        matched_faculties.append(faculty)
                        matched_ous.append(ou)
                    elif block.is_utb:
                        unmatched_utb_authors.append(author)
                        needs_llm = True

        flags = {
            FlagKey.MATCHED_UTB_AUTHORS: len(matched_authors),
        }
        if unmatched_utb_authors:
            flags[FlagKey.UNMATCHED_UTB_AUTHORS] = unmatched_utb_authors
        if warnings:
            flags[FlagKey.PARSE_WARNINGS] = warnings
        if any("viac UTB blokov" in warning for warning in warnings):
            flags[FlagKey.MULTIPLE_UTB_BLOCKS] = True

        result.update(
            {
                "heuristic_status": HeuristicStatus.PROCESSED,
                "needs_llm": needs_llm,
                "utb_contributor_internalauthor": _join(matched_authors),
                "utb_faculty": _join(matched_faculties),
                "utb_ou": _join(matched_ous),
                "flags": flags,
            }
        )
    except Exception as exc:
        result["needs_llm"] = True
        result["flags"] = {FlagKey.ERROR: str(exc)}

    return result


def process_batch(rows: list, registry: list[InternalAuthor]) -> list[dict]:
    return [process_record(row.resource_id, row.wos_aff, registry) for row in rows]


def run_heuristics(
    engine: Engine | None = None,
    batch_size: int | None = None,
    limit: int = 0,
    reprocess_errors: bool = False,
) -> None:
    engine = engine or get_local_engine()
    batch_size = batch_size or settings.heuristics_batch_size
    schema = settings.local_schema
    table = settings.local_table
    status_values = [HeuristicStatus.NOT_PROCESSED]
    if reprocess_errors:
        status_values.append(HeuristicStatus.ERROR)

    registry = get_author_registry(engine)

    with engine.connect() as conn:  # Vráti počet záznamov s danými statusmi na spracovanie
        total = conn.execute(
            text(
                f"""
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE heuristic_status = ANY(:statuses)
                """
            ),
            {"statuses": status_values},
        ).scalar_one()

    if limit > 0:
        total = min(total, limit)

    if total == 0:
        print("[INFO] Žiadne záznamy na spracovanie.")
        return

    processed = 0
    while processed < total:
        current_batch = min(batch_size, total - processed)
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT resource_id, "utb.wos.affiliation" AS wos_aff
                    FROM "{schema}"."{table}"
                    WHERE heuristic_status = ANY(:statuses)
                    ORDER BY resource_id
                    LIMIT :lim
                    """
                ),
                {"statuses": status_values, "lim": current_batch},
            ).fetchall()

        if not rows:  # ak sa nevrátili žiadne záznamy, skončiť
            break

        updates = process_batch(rows, registry)
        update_sql = f"""
            UPDATE "{schema}"."{table}"
            SET
                flags = %s::jsonb,
                heuristic_status = %s,
                heuristic_version = %s,
                heuristic_processed_at = %s,
                needs_llm = %s,
                utb_contributor_internalauthor = %s,
                utb_faculty = %s,
                utb_ou = %s
            WHERE resource_id = %s
        """
        params = [
            (
                json.dumps(item["flags"], ensure_ascii=False),
                item["heuristic_status"],
                item["heuristic_version"],
                item["heuristic_processed_at"],
                item["needs_llm"],
                item["utb_contributor_internalauthor"],
                item["utb_faculty"],
                item["utb_ou"],
                item["resource_id"],
            )
            for item in updates
        ]

        raw_conn = engine.raw_connection()
        try:
            with raw_conn.cursor() as cursor:
                cursor.executemany(update_sql, params)
            raw_conn.commit()
        finally:
            raw_conn.close()

        processed += len(rows)
        print(f"  Spracované: {processed}/{total}")
