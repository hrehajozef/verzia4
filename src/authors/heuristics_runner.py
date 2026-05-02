"""Batch orchestrace pre heuristiky autorov."""

from __future__ import annotations

import json

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.authors.heuristics import process_record
from src.authors.heuristics_support import build_source_author_map
from src.authors.registry import InternalAuthor, get_author_registry
from src.authors.workplace_tree import load_workplace_tree
from src.common.constants import HeuristicStatus, QUEUE_TABLE
from src.config.settings import settings
from src.db.engines import get_local_engine, get_remote_engine


def process_batch(
    rows: list,
    registry: list[InternalAuthor],
    normalize: bool = False,
    remote_engine: Engine | None = None,
    source_author_map: dict[int, dict[str, list[str]]] | None = None,
) -> list[dict]:
    workplace_tree = load_workplace_tree(remote_engine=remote_engine)
    return [
        process_record(
            resource_id=row.resource_id,
            wos_aff_arr=row.wos_aff,
            dc_authors_arr=row.dc_authors,
            registry=registry,
            normalize=normalize,
            remote_engine=remote_engine,
            workplace_tree=workplace_tree,
            scopus_aff_arr=row.scopus_aff,
            fulltext_aff_arr=row.fulltext_aff,
            wos_author_arr=(source_author_map or {}).get(row.resource_id, {}).get("wos"),
            scopus_author_arr=(source_author_map or {}).get(row.resource_id, {}).get("scopus"),
        )
        for row in rows
    ]


def run_heuristics(
    engine: Engine | None = None,
    remote_engine: Engine | None = None,
    batch_size: int | None = None,
    limit: int = 0,
    reprocess_errors: bool = False,
    reprocess: bool = False,
    normalize: bool = False,
) -> None:
    engine = engine or get_local_engine()
    remote_engine = remote_engine or get_remote_engine()
    batch_size = batch_size or settings.heuristics_batch_size
    schema = settings.local_schema
    table = settings.local_table
    queue = QUEUE_TABLE
    statuses = [HeuristicStatus.NOT_PROCESSED]
    if reprocess_errors:
        statuses.append(HeuristicStatus.ERROR)
    if reprocess:
        statuses.append(HeuristicStatus.PROCESSED)

    registry = get_author_registry(remote_engine=remote_engine)
    print(f"[INFO] Nacitanych internych autorov z remote DB: {len(registry)}")

    with engine.connect() as conn:
        id_rows = conn.execute(
            text(f"""
                SELECT q.resource_id
                FROM "{schema}"."{queue}" q
                JOIN "{schema}"."{table}" m ON q.resource_id = m.resource_id
                WHERE q.author_heuristic_status = ANY(:s)
                ORDER BY q.resource_id
            """),
            {"s": statuses},
        ).fetchall()

    all_ids = [int(row[0]) for row in id_rows]
    if limit > 0:
        all_ids = all_ids[:limit]
    total = len(all_ids)
    if total == 0:
        print("[INFO] Ziadne zaznamy na spracovanie.")
        return

    print(f"[INFO] Zaznamov na spracovanie: {total}")
    processed = 0
    while processed < total:
        batch_ids = all_ids[processed: processed + batch_size]
        with engine.connect() as conn:
            rows = conn.execute(
                text(f"""
                    SELECT m.resource_id,
                           m."utb.wos.affiliation" AS wos_aff,
                           m."utb.scopus.affiliation" AS scopus_aff,
                           m."utb.fulltext.affiliation" AS fulltext_aff,
                           m."dc.contributor.author" AS dc_authors,
                           m."utb.source" AS source_arr
                    FROM "{schema}"."{table}" m
                    JOIN "{schema}"."{queue}" q ON m.resource_id = q.resource_id
                    WHERE q.resource_id = ANY(:ids)
                    ORDER BY m.resource_id
                """),
                {"ids": batch_ids},
            ).fetchall()

        if not rows:
            processed += len(batch_ids)
            print(f"  [WARN] Davka bez platnych zaznamov: {batch_ids}")
            continue

        updates = process_batch(
            rows,
            registry,
            normalize=normalize,
            remote_engine=remote_engine,
            source_author_map=build_source_author_map(engine, rows),
        )
        update_sql = f"""
            UPDATE "{schema}"."{queue}"
            SET
                author_flags = %s::jsonb ||
                    CASE
                        WHEN author_flags ? 'duplicates'
                        THEN jsonb_build_object('duplicates', author_flags->'duplicates')
                        ELSE '{{}}'::jsonb
                    END,
                author_heuristic_status = %s,
                author_heuristic_version = %s,
                author_heuristic_processed_at = %s,
                author_needs_llm = %s,
                author_dc_names = %s,
                author_internal_names = %s,
                author_faculty = %s,
                author_ou = %s
            WHERE resource_id = %s
        """
        params = [
            (
                json.dumps(update["author_flags"], ensure_ascii=False),
                update["author_heuristic_status"],
                update["author_heuristic_version"],
                update["author_heuristic_processed_at"],
                update["author_needs_llm"],
                update["author_dc_names"],
                update["author_internal_names"],
                update["author_faculty"],
                update["author_ou"],
                update["resource_id"],
            )
            for update in updates
        ]

        raw = engine.raw_connection()
        try:
            with raw.cursor() as cur:
                cur.executemany(update_sql, params)
            raw.commit()
        finally:
            raw.close()

        processed += len(batch_ids)
        print(f"  Spracovane: {processed}/{total}")

    print(f"[OK] Heuristiky autorov hotove. Spracovanych: {processed}")
