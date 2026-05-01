"""Inicializácia (bootstrap) lokálnej DB tabuľky z remote zdroja."""

# TODO: Pridať typer namiesto print

from __future__ import annotations

import json
import time

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from src.common.constants import OUTPUT_COLUMNS, QUEUE_TABLE
from src.config.settings import settings
from src.db.engines import get_local_engine, get_remote_engine

CHANGE_BUFFER_TABLE = "utb_change_buffer"
PRIRASTKY_TABLE = "utb_prirastky_arr"
PRIRASTKY_VIEW = "utb_metadata_arr_with_prirastky"


def _get_remote_columns(remote_engine: Engine) -> list[dict]:
    """
    Vstup - remote_engine: SQLAlchemy Engine pripojený na remote databázu.

    Výstup - list[dict]: zoznam stĺpcov (každý ako dict s kľúčmi ako column_name, data_type, udt_name, ...),
    zoradený podľa ordinal_position.

    Načíta metadáta stĺpcov remote tabuľky z information_schema.columns.
    Ak tabuľka neexistuje alebo je prázdna, vyhodí RuntimeError.
    """
    query = text(
        """
        SELECT
            column_name,
            udt_name,
            data_type,
            is_nullable,
            character_maximum_length,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
        ORDER BY ordinal_position
        """
    )
    with remote_engine.connect() as conn:
        rows = conn.execute(
            query, {"schema": settings.remote_schema, "table": settings.remote_table}
        ).fetchall()
    if not rows:
        raise RuntimeError(f"Remote tabuľka {settings.remote_table_full} neexistuje")
    return [row._asdict() for row in rows]


def _col_sql_type(col: dict) -> str:
    """
    Vstupy - stĺpec ako dict s metadátami stĺpca (očakáva kľúče data_type, udt_name a voliteľne character_maximum_length).

    Výstup - str: SQL typ (napr. INTEGER, VARCHAR(255), TEXT[], TIMESTAMPTZ).

    Funkcia preloží metadáta jedného stĺpca z remote DB na SQL typ pre CREATE TABLE v lokálnej DB.
    špeciálne ošetruje ARRAY typy (mapuje udt_name bez podčiarkovníka), ošetruje character varying/character s dĺžkou,
    inak mapuje bežné PostgreSQL data_type na SQL typ; pri neznámom type použije upper().
    """
    data_type = col["data_type"]
    udt_name = col["udt_name"]

    if data_type == "ARRAY":
        mapping = {
            "varchar": "VARCHAR",
            "text": "TEXT",
            "int4": "INTEGER",
            "int8": "BIGINT",
            "bool": "BOOLEAN",
            "float4": "REAL",
            "float8": "DOUBLE PRECISION",
        }
        return f"{mapping.get(udt_name.lstrip('_'), udt_name.lstrip('_').upper())}[]"

    if data_type in {"character varying", "character"}:
        max_len = col.get("character_maximum_length")
        return f"VARCHAR({max_len})" if max_len else "VARCHAR"

    mapping = {
        "integer": "INTEGER",
        "bigint": "BIGINT",
        "smallint": "SMALLINT",
        "boolean": "BOOLEAN",
        "text": "TEXT",
        "real": "REAL",
        "double precision": "DOUBLE PRECISION",
        "numeric": "NUMERIC",
        "timestamp without time zone": "TIMESTAMP",
        "timestamp with time zone": "TIMESTAMPTZ",
        "date": "DATE",
        "jsonb": "JSONB",
        "json": "JSON",
        "uuid": "UUID",
    }
    return mapping.get(data_type, data_type.upper())


def _build_create_table_sql(columns: list[dict]) -> str:
    """
    Vygeneruje SQL príkaz CREATE TABLE pre lokálnu tabuľku na základe remote stĺpcov
    a doplní aj interné OUTPUT_COLUMNS.
    """
    definitions = []
    for col in columns:
        null_sql = "" if col["is_nullable"] == "YES" else " NOT NULL"
        definitions.append(f'"{col["column_name"]}" {_col_sql_type(col)}{null_sql}')

    for output in OUTPUT_COLUMNS:
        default_sql = f" DEFAULT {output.default_sql}" if output.default_sql else ""
        definitions.append(f'"{output.name}" {output.sql_type}{default_sql}')

    cols_sql = ",\n    ".join(definitions)
    return (
        f'CREATE TABLE IF NOT EXISTS "{settings.local_schema}"."{settings.local_table}" (\n'
        f"    {cols_sql}\n"
        ");"
    )


def _table_exists(local_engine: Engine) -> bool:
    """
    Zistí, či lokálna tabuľka existuje.
    """
    return inspect(local_engine).has_table(settings.local_table, schema=settings.local_schema)


def _drop_table(local_engine: Engine) -> None:
    """
    Zmaže lokálnu tabuľku (ak existuje).
    """
    with local_engine.begin() as conn:
        conn.execute(
            text(
                f'DROP TABLE IF EXISTS "{settings.local_schema}"."{settings.local_table}" CASCADE'
            )
        )


def _create_table(local_engine: Engine, columns: list[dict]) -> None:
    """
    Vytvorí lokálnu tabuľku podľa definície odvodenej z remote stĺpcov.
    """
    with local_engine.begin() as conn:
        conn.execute(text(_build_create_table_sql(columns)))


def _ensure_output_columns(local_engine: Engine) -> None:
    """
    Zabezpečí, aby lokálna tabuľka obsahovala všetky stĺpce definované v OUTPUT_COLUMNS.
    """
    existing = {
        col["name"]
        for col in inspect(local_engine).get_columns(
            settings.local_table, schema=settings.local_schema
        )
    }
    for output in OUTPUT_COLUMNS:
        if output.name in existing:
            continue
        default_sql = f" DEFAULT {output.default_sql}" if output.default_sql else ""
        alter_sql = text(
            f"""
            ALTER TABLE "{settings.local_schema}"."{settings.local_table}"
            ADD COLUMN IF NOT EXISTS "{output.name}" {output.sql_type}{default_sql}
            """
        )
        with local_engine.begin() as conn:
            conn.execute(alter_sql)


def _create_indexes(local_engine: Engine) -> None:
    """
    Vytvorí vybrané indexy na lokálnej tabuľke pre rýchle filtrovanie podľa stavových stĺpcov.
    """
    table = settings.local_table
    schema = settings.local_schema
    index_sql = [
        f'CREATE INDEX IF NOT EXISTS idx_{table}_author_heuristic_status ON "{schema}"."{table}" (author_heuristic_status)',
        f'CREATE INDEX IF NOT EXISTS idx_{table}_author_needs_llm ON "{schema}"."{table}" (author_needs_llm) WHERE author_needs_llm = TRUE',
        f'CREATE INDEX IF NOT EXISTS idx_{table}_author_llm_status ON "{schema}"."{table}" (author_llm_status)',
    ]
    with local_engine.begin() as conn:
        for sql in index_sql:
            conn.execute(text(sql))


def _copy_data(remote_engine: Engine, local_engine: Engine, columns: list[dict]) -> None:
    """
    Skopíruje dáta z remote tabuľky do lokálnej tabuľky po dávkach (batch).
    Z remote tabuľky sa skopírujú
    """
    col_names = [f'"{col["column_name"]}"' for col in columns]
    cols_sql = ", ".join(col_names)
    batch_size = settings.copy_batch_size if settings.copy_batch_size > 0 else 500

    with remote_engine.connect() as conn:
        total = conn.execute(
            text(f'SELECT COUNT(*) FROM "{settings.remote_schema}"."{settings.remote_table}"')
        ).scalar_one()

    if settings.copy_limit > 0:
        total = min(total, settings.copy_limit)

    insert_sql = (
        f'INSERT INTO "{settings.local_schema}"."{settings.local_table}" ({cols_sql}) '
        f"VALUES ({', '.join(['%s'] * len(columns))}) ON CONFLICT DO NOTHING"
    )

    copied = 0
    started = time.time()
    while copied < total:
        current_batch = min(batch_size, total - copied)
        with remote_engine.connect() as conn:
            rows = conn.execute(
                text(
                    f'SELECT {cols_sql} FROM "{settings.remote_schema}"."{settings.remote_table}" '
                    "ORDER BY resource_id LIMIT :lim OFFSET :off"
                ),
                {"lim": current_batch, "off": copied},
            ).fetchall()

        if not rows:
            break

        raw_conn = local_engine.raw_connection()
        try:
            with raw_conn.cursor() as cursor:
                cursor.executemany(insert_sql, [tuple(row) for row in rows])
            raw_conn.commit()
        finally:
            raw_conn.close()

        copied += len(rows)
        speed = copied / max(time.time() - started, 1)
        print(f"  Skopírované: {copied}/{total} | {speed:.0f} riadkov/s")


def setup_processing_queue(local_engine: Engine | None = None) -> None:
    """
    Vytvorí tabuľku utb_processing_queue pre výstupy pipeline.

    Táto tabuľka je medzitabuľka medzi pipelineom a webovou aplikáciou.
    Pipeline sem zapisuje výsledky heuristík, LLM, validácie, dátumov a normalizácie.
    Knihovník cez webovú aplikáciu tieto navrhnuté hodnoty kontroluje a schvaľuje.

    Bezpečné spustiť opakovane (CREATE TABLE IF NOT EXISTS).
    """
    local_engine = local_engine or get_local_engine()
    schema = settings.local_schema
    queue  = QUEUE_TABLE

    ddl = f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."{queue}" (
            resource_id BIGINT PRIMARY KEY,

            -- Autor – heuristiky
            author_flags                   JSONB        DEFAULT '{{}}',
            author_heuristic_status        TEXT         DEFAULT 'not_processed',
            author_heuristic_version       TEXT,
            author_heuristic_processed_at  TIMESTAMPTZ,
            author_needs_llm               BOOLEAN      DEFAULT FALSE,
            author_dc_names                TEXT[],
            author_internal_names          TEXT[],
            author_faculty                 TEXT[],
            author_ou                      TEXT[],

            -- Autor – LLM
            author_llm_result              JSONB,
            author_llm_status              TEXT         DEFAULT 'not_processed',
            author_llm_processed_at        TIMESTAMPTZ,

            -- Validácia
            validation_status              TEXT         DEFAULT 'not_checked',
            validation_flags               JSONB        DEFAULT '{{}}',
            validation_suggested_fixes     JSONB        DEFAULT '{{}}',
            validation_version             TEXT,
            validation_checked_at          TIMESTAMPTZ,

            -- Dátumy – heuristiky
            utb_date_received              DATE,
            utb_date_reviewed              DATE,
            utb_date_accepted              DATE,
            utb_date_published_online      DATE,
            utb_date_published             DATE,
            utb_date_extra                 JSONB,
            date_heuristic_status          TEXT         DEFAULT 'not_processed',
            date_needs_llm                 BOOLEAN      DEFAULT FALSE,
            date_flags                     JSONB        DEFAULT '{{}}',
            date_heuristic_version         TEXT,
            date_processed_at              TIMESTAMPTZ,

            -- Dátumy – LLM
            date_llm_status                TEXT         DEFAULT 'not_processed',
            date_llm_processed_at          TIMESTAMPTZ,
            date_llm_result                JSONB,

            -- Normalizácia journalov
            journal_norm_status            TEXT         DEFAULT 'not_processed',
            journal_norm_proposed_publisher TEXT,
            journal_norm_proposed_ispartof TEXT,
            journal_norm_api_source        TEXT,
            journal_norm_issn_key          TEXT,
            journal_norm_version           TEXT,
            journal_norm_processed_at      TIMESTAMPTZ,

            -- Workflow knihovníka
            librarian_checked_at           TIMESTAMPTZ[],
            created_at                     TIMESTAMPTZ  DEFAULT now(),
            updated_at                     TIMESTAMPTZ  DEFAULT now()
        )
    """

    print(f"[SETUP] Vytváram tabuľku {schema}.{queue}...")
    with local_engine.begin() as conn:
        conn.execute(text(ddl))
        # Index pre rýchle filtrovanie neskontrolovaných záznamov
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{queue}_librarian_checked
            ON "{schema}"."{queue}" (librarian_checked_at)
            WHERE librarian_checked_at IS NULL
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{queue}_author_status
            ON "{schema}"."{queue}" (author_heuristic_status)
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{queue}_date_status
            ON "{schema}"."{queue}" (date_heuristic_status)
        """))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{schema}"."{CHANGE_BUFFER_TABLE}" (
                id BIGSERIAL PRIMARY KEY,
                resource_id BIGINT NOT NULL,
                field_key TEXT NOT NULL,
                target_table TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                created_at TIMESTAMPTZ DEFAULT now(),
                approved_at TIMESTAMPTZ,
                discarded_at TIMESTAMPTZ
            )
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{CHANGE_BUFFER_TABLE}_resource_pending
            ON "{schema}"."{CHANGE_BUFFER_TABLE}" (resource_id)
            WHERE approved_at IS NULL AND discarded_at IS NULL
        """))

    # Naplní queue riadkami pre všetky existujúce resource_id z hlavnej tabuľky
    _ensure_processing_queue_column_types(local_engine, schema, queue)

    main_table = settings.local_table
    with local_engine.begin() as conn:
        inserted = conn.execute(text(f"""
            INSERT INTO "{schema}"."{queue}" (resource_id)
            SELECT resource_id FROM "{schema}"."{main_table}"
            ON CONFLICT (resource_id) DO NOTHING
        """)).rowcount

    print(f"[OK] {queue} pripravená. Nových riadkov: {inserted}")


def _ensure_processing_queue_column_types(local_engine: Engine, schema: str, queue: str) -> None:
    """Keep queue column types aligned with pipeline writers."""
    inspector = inspect(local_engine)
    existing = {col["name"] for col in inspector.get_columns(queue, schema=schema)}
    if "date_llm_result" not in existing:
        return

    with local_engine.connect() as conn:
        row = conn.execute(text("""
            SELECT udt_name
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name = :queue
              AND column_name = 'date_llm_result'
        """), {"schema": schema, "queue": queue}).mappings().fetchone()

    if not row or row["udt_name"] == "jsonb":
        return

    legacy_col = "date_llm_result_legacy"
    print(f"[MIGRATE] Menim {queue}.date_llm_result na JSONB...")

    with local_engine.begin() as conn:
        if legacy_col not in existing:
            conn.execute(text(
                f'ALTER TABLE "{schema}"."{queue}" RENAME COLUMN date_llm_result TO {legacy_col}'
            ))
        else:
            conn.execute(text(f'ALTER TABLE "{schema}"."{queue}" DROP COLUMN date_llm_result'))
        conn.execute(text(f'ALTER TABLE "{schema}"."{queue}" ADD COLUMN date_llm_result JSONB'))

    with local_engine.begin() as conn:
        rows = conn.execute(text(
            f'SELECT resource_id, {legacy_col} FROM "{schema}"."{queue}" WHERE {legacy_col} IS NOT NULL'
        )).fetchall()
        for row in rows:
            raw_value = row[1]
            try:
                parsed = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
            except Exception:
                parsed = {"raw": str(raw_value)}
            conn.execute(
                text(
                    f'UPDATE "{schema}"."{queue}" '
                    'SET date_llm_result = CAST(:value AS jsonb) '
                    'WHERE resource_id = :resource_id'
                ),
                {
                    "value": json.dumps(parsed, ensure_ascii=False),
                    "resource_id": row.resource_id,
                },
            )
        conn.execute(text(f'ALTER TABLE "{schema}"."{queue}" DROP COLUMN {legacy_col}'))


def rename_legacy_author_columns(local_engine: Engine | None = None) -> None:
    """
    Premenuje staré názvy stĺpcov (pred refactoringom) na nové author_* názvy.
    Bezpečné spustiť opakovane – stĺpce sa premenujú len ak ešte existujú pod starým názvom.
    """
    local_engine = local_engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table

    renames = [
        ("flags",                          "author_flags"),
        ("heuristic_status",               "author_heuristic_status"),
        ("heuristic_version",              "author_heuristic_version"),
        ("heuristic_processed_at",         "author_heuristic_processed_at"),
        ("needs_llm",                      "author_needs_llm"),
        ("dc_contributor_author",          "author_dc_names"),
        ("utb_contributor_internalauthor", "author_internal_names"),
        ("utb_faculty",                    "author_faculty"),
        ("utb_ou",                         "author_ou"),
        ("llm_result",                     "author_llm_result"),
        ("llm_status",                     "author_llm_status"),
        ("llm_processed_at",               "author_llm_processed_at"),
    ]

    existing = {
        col["name"]
        for col in inspect(local_engine).get_columns(table, schema=schema)
    }

    renamed = 0
    with local_engine.begin() as conn:
        for old_name, new_name in renames:
            if old_name in existing and new_name not in existing:
                conn.execute(text(
                    f'ALTER TABLE "{schema}"."{table}" RENAME COLUMN "{old_name}" TO "{new_name}"'
                ))
                print(f"  Premenovaný: {old_name} -> {new_name}")
                renamed += 1

    if renamed:
        print(f"[OK] Premenovaných {renamed} stĺpcov.")
    else:
        print("[INFO] Žiadne stĺpce na premenovanie (už sú aktuálne alebo tabuľka neexistuje).")


def run_bootstrap(drop_existing: bool = False) -> None:
    """
    Riadi bootstrap lokálnej tabuľky z remote zdroja.
    """
    remote_engine = get_remote_engine()
    local_engine = get_local_engine()

    print("BOOTSTRAP - príprava lokálnej tabuľky")
    columns = _get_remote_columns(remote_engine)

    existed_before = _table_exists(local_engine)
    if existed_before and drop_existing:
        _drop_table(local_engine)
        _create_table(local_engine, columns)
    elif existed_before:
        _ensure_output_columns(local_engine)
    else:
        _create_table(local_engine, columns)

    _create_indexes(local_engine)

    with local_engine.connect() as conn:
        local_count = conn.execute(
            text(f'SELECT COUNT(*) FROM "{settings.local_schema}"."{settings.local_table}"')
        ).scalar_one()

    if drop_existing or not existed_before or local_count == 0:
        _copy_data(remote_engine, local_engine, columns)
    else:
        print(f"[INFO] Lokálna tabuľka už obsahuje {local_count} riadkov, kopírovanie sa preskočilo.")

    setup_prirastky_view(local_engine)
    print("[OK] Bootstrap dokončený.")


def setup_prirastky_view(local_engine: Engine | None = None) -> None:
    """
    Vytvori view utb_metadata_arr_with_prirastky, ak existuje public.utb_prirastky_arr.

    Ak tabulka prirastkov este neexistuje, iba vypise warning a skonci bez padu.
    """
    local_engine = local_engine or get_local_engine()
    schema = settings.local_schema
    inspector = inspect(local_engine)
    if not inspector.has_table(PRIRASTKY_TABLE, schema=schema):
        print(
            f"[WARN] {schema}.{PRIRASTKY_TABLE} neexistuje, "
            f"view {schema}.{PRIRASTKY_VIEW} sa nevytvorilo."
        )
        return

    with local_engine.begin() as conn:
        conn.execute(text(f"""
            CREATE OR REPLACE VIEW "{schema}"."{PRIRASTKY_VIEW}" AS
            SELECT * FROM "{schema}"."{settings.local_table}"
            UNION ALL
            SELECT * FROM "{schema}"."{PRIRASTKY_TABLE}"
        """))
    print(f"[OK] View {schema}.{PRIRASTKY_VIEW} pripravene.")
