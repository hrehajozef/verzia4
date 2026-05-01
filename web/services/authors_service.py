"""CRUD pristup k remote tabulke veda.utb_authors."""

from __future__ import annotations

import unicodedata
from typing import Any

from sqlalchemy import text

from src.authors.workplace_tree import load_workplace_tree
from src.config.settings import settings
from src.db.engines import get_remote_engine

_SCHEMA = settings.remote_schema
_TABLE = "utb_authors"
_UTB_FILTER_SQL = "COALESCE(utb, '') ILIKE 'ano'"
_FIELD_LABELS = {
    "poradie": "Poradie",
    "utb": "UTB",
    "author_id": "Author ID",
    "utbid": "UTB ID",
    "display_name": "Zobrazované meno",
    "surname": "Priezvisko",
    "given_name": "Meno",
    "middle_name": "Stredné meno",
    "other_name": "Iný tvar mena",
    "public_email": "Verejný e-mail",
    "email": "Interný e-mail",
    "czech_authority_id": "Czech authority ID",
    "orcid_unconfirmed": "ORCID (nepotvrdené)",
    "orcid": "ORCID",
    "researcherid": "ResearcherID",
    "scopusid": "Scopus ID",
    "wos_id": "WoS ID",
    "plumx_id": "PlumX ID",
    "scholar_id": "Scholar ID",
    "organization_id": "Organization ID",
    "employee_number": "Osobné číslo",
    "obd_id": "OBD ID",
    "obd_id_old": "OBD ID (staré)",
    "role": "Rola",
    "comment": "Poznámka",
    "faculty": "Fakulta",
}
_INTEGER_FIELDS = {"poradie", "author_id", "organization_id"}
_REQUIRED_FIELDS = {"poradie", "author_id", "display_name"}


def _normalize_search(value: str) -> str:
    if not value:
        return ""
    decomposed = unicodedata.normalize("NFD", value)
    without_marks = "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")
    return " ".join(without_marks.lower().split())


def _fetch_faculty_options(engine=None) -> list[str]:
    engine = engine or get_remote_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT e.enumlabel
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            JOIN pg_namespace n ON n.oid = t.typnamespace
            WHERE t.typname = 'type_utb_authors_faculty'
            ORDER BY e.enumsortorder
        """)).fetchall()
    return [str(row.enumlabel) for row in rows]


def _fetch_author_columns(engine=None) -> list[dict[str, Any]]:
    engine = engine or get_remote_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT column_name, data_type, udt_name, is_nullable, column_default, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name = :table
            ORDER BY ordinal_position
        """), {"schema": _SCHEMA, "table": _TABLE}).mappings().fetchall()

    faculty_options = _fetch_faculty_options(engine)
    columns: list[dict[str, Any]] = []
    for row in rows:
        column = {
            "name": row["column_name"],
            "data_type": row["data_type"],
            "udt_name": row["udt_name"],
            "nullable": row["is_nullable"] == "YES",
            "default": row["column_default"],
            "label": _FIELD_LABELS.get(row["column_name"], row["column_name"]),
            "required": row["column_name"] in _REQUIRED_FIELDS,
            "kind": "number" if row["column_name"] in _INTEGER_FIELDS else "text",
            "options": [],
        }
        if row["column_name"] == "faculty":
            column["kind"] = "select"
            column["options"] = faculty_options
        columns.append(column)
    return columns


def get_author_editor_config(engine=None) -> dict[str, Any]:
    engine = engine or get_remote_engine()
    return {
        "columns": _fetch_author_columns(engine),
        "can_write": can_write_authors(engine),
        "faculty_options": _fetch_faculty_options(engine),
    }


def can_write_authors(engine=None) -> bool:
    engine = engine or get_remote_engine()
    with engine.connect() as conn:
        grants = conn.execute(text(f"""
            SELECT privilege_type
            FROM information_schema.role_table_grants
            WHERE table_schema = :schema
              AND table_name = :table
              AND grantee = current_user
        """), {"schema": _SCHEMA, "table": _TABLE}).fetchall()
    available = {str(row.privilege_type).upper() for row in grants}
    return {"SELECT", "INSERT", "UPDATE", "DELETE"}.issubset(available)


def _fetch_author_rows(engine=None) -> list[dict[str, Any]]:
    engine = engine or get_remote_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT ctid::text AS row_ref, *
            FROM "{_SCHEMA}"."{_TABLE}"
            WHERE {_UTB_FILTER_SQL}
            ORDER BY poradie, display_name, faculty NULLS LAST
        """)).mappings().fetchall()
    return [dict(row) for row in rows]


def _row_to_summary(row: dict[str, Any]) -> dict[str, Any]:
    raw = str(row.get("display_name") or "").strip()
    variants = [value.strip() for value in raw.split("||") if value.strip()]
    faculty_raw = str(row.get("faculty") or "").strip()
    faculties = [value.strip() for value in faculty_raw.split("||") if value.strip()]
    return {
        "row_ref": str(row["row_ref"]),
        "author_id": int(row["author_id"]) if row.get("author_id") is not None else None,
        "display_name": raw,
        "variants": variants,
        "primary": variants[0] if variants else raw,
        "faculty": faculties[0] if faculties else None,
        "faculties": faculties,
    }


def _row_department(row: dict[str, Any], workplace_tree: dict[int, Any]) -> str | None:
    organization_id = row.get("organization_id")
    if organization_id in (None, ""):
        return None
    try:
        node = workplace_tree.get(int(organization_id))
    except (TypeError, ValueError):
        return None
    return node.name_en if node else None


def _summary_key(row: dict[str, Any]) -> str:
    summary = _row_to_summary(row)
    return _normalize_search(summary["display_name"] or summary["primary"])


def _group_summaries(rows: list[dict[str, Any]], engine=None) -> list[dict[str, Any]]:
    engine = engine or get_remote_engine()
    workplace_tree = load_workplace_tree(remote_engine=engine)
    grouped: dict[str, dict[str, Any]] = {}
    order: list[str] = []

    for row in rows:
        summary = _row_to_summary(row)
        key = _normalize_search(summary["display_name"] or summary["primary"])
        if key not in grouped:
            grouped[key] = {
                **summary,
                "row_refs": [summary["row_ref"]],
                "faculties": [],
                "departments": [],
                "affiliations": [],
            }
            order.append(key)

        group = grouped[key]
        if summary["row_ref"] not in group["row_refs"]:
            group["row_refs"].append(summary["row_ref"])

        department = _row_department(row, workplace_tree)
        faculties = summary["faculties"] or []

        if faculties:
            for faculty in faculties:
                affiliation = {"faculty": faculty, "department": department or ""}
                if affiliation not in group["affiliations"]:
                    group["affiliations"].append(affiliation)
                if faculty not in group["faculties"]:
                    group["faculties"].append(faculty)
        elif department:
            affiliation = {"faculty": "", "department": department}
            if affiliation not in group["affiliations"]:
                group["affiliations"].append(affiliation)

        if department and department not in group["departments"]:
            group["departments"].append(department)

    return [grouped[key] for key in order]


def _modal_details_from_rows(rows: list[dict[str, Any]], row_ref: str, engine=None) -> dict[str, Any] | None:
    target = None
    for row in rows:
        if str(row.get("row_ref")) == str(row_ref):
            target = row
            break
    if target is None:
        return None

    key = _summary_key(target)
    group_rows = [row for row in rows if _summary_key(row) == key]
    summary = _group_summaries(group_rows, engine=engine)[0]

    preferred_email = str(target.get("public_email") or "").strip() or str(target.get("email") or "").strip()
    return {
        "row_ref": str(target.get("row_ref")),
        "display_name": str(target.get("display_name") or "").strip(),
        "surname": str(target.get("surname") or "").strip(),
        "given_name": str(target.get("given_name") or "").strip(),
        "middle_name": str(target.get("middle_name") or "").strip(),
        "orcid": str(target.get("orcid") or "").strip(),
        "scopusid": str(target.get("scopusid") or "").strip(),
        "wos_id": str(target.get("wos_id") or "").strip() or str(target.get("researcherid") or "").strip(),
        "faculties": list(summary.get("faculties") or []),
        "departments": list(summary.get("departments") or []),
        "affiliations": list(summary.get("affiliations") or []),
        "preferred_email": preferred_email,
        "row_refs": list(summary.get("row_refs") or []),
        "primary": summary.get("primary") or str(target.get("display_name") or "").strip(),
    }


def _row_search_text(row: dict[str, Any]) -> str:
    values = [
        row.get("display_name"),
        row.get("surname"),
        row.get("given_name"),
        row.get("utbid"),
        row.get("orcid"),
        row.get("faculty"),
        row.get("email"),
        row.get("public_email"),
    ]
    return _normalize_search(" ".join(str(value or "") for value in values))


def get_all_authors(engine=None) -> list[dict[str, Any]]:
    """Vrati vsetky interne riadky autorov z remote utb_authors."""
    return _group_summaries(_fetch_author_rows(engine), engine=engine)


def search_authors(query: str, engine=None) -> list[dict[str, Any]]:
    """Vyhlada internych autorov diakriticky necitlivo."""
    needle = _normalize_search(query)
    if not needle:
        return get_all_authors(engine)
    matched_rows: list[dict[str, Any]] = []
    for row in _fetch_author_rows(engine):
        if needle in _row_search_text(row):
            matched_rows.append(row)
            if len(matched_rows) >= 50:
                break
    return _group_summaries(matched_rows, engine=engine)


def _full_row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for key, value in row.items():
        if value is None or isinstance(value, int):
            data[key] = value
        else:
            data[key] = str(value)
    return data


def get_full_authors(query: str = "", row_ref: str | None = None, limit: int = 100, engine=None) -> list[dict[str, Any]]:
    rows = _fetch_author_rows(engine)
    if row_ref is not None:
        rows = [row for row in rows if str(row.get("row_ref")) == str(row_ref)]
    elif query.strip():
        needle = _normalize_search(query)
        rows = [row for row in rows if needle in _row_search_text(row)]
    return [_full_row_to_dict(row) for row in rows[: max(1, min(limit, 500))]]


def get_author(row_ref: str, engine=None) -> dict[str, Any] | None:
    authors = get_full_authors(row_ref=row_ref, limit=1, engine=engine)
    return authors[0] if authors else None


def get_author_modal_details(row_ref: str, engine=None) -> dict[str, Any] | None:
    rows = _fetch_author_rows(engine)
    return _modal_details_from_rows(rows, row_ref, engine=engine)


def _normalize_payload(payload: dict[str, Any], *, for_create: bool, engine=None) -> dict[str, Any]:
    column_map = {column["name"]: column for column in _fetch_author_columns(engine)}
    faculty_options = set(_fetch_faculty_options(engine))
    normalized: dict[str, Any] = {}

    for field_name, raw_value in payload.items():
        if field_name not in column_map:
            continue
        value = raw_value
        if isinstance(value, str):
            value = value.strip()
        if value == "":
            value = None

        if field_name in _INTEGER_FIELDS:
            if value is None:
                normalized[field_name] = None
            else:
                try:
                    normalized[field_name] = int(value)
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"Pole '{field_name}' musí byť celé číslo.") from exc
        elif field_name == "faculty":
            if value is not None and value not in faculty_options:
                raise ValueError("Pole 'faculty' obsahuje nepovolenú hodnotu.")
            normalized[field_name] = value
        else:
            normalized[field_name] = value

    if for_create and "utb" not in normalized:
        normalized["utb"] = "ano"

    if for_create:
        for required in _REQUIRED_FIELDS:
            if normalized.get(required) in (None, ""):
                raise ValueError(f"Pole '{required}' je povinné.")
    else:
        for required in _REQUIRED_FIELDS:
            if required in normalized and normalized.get(required) in (None, ""):
                raise ValueError(f"Pole '{required}' je povinné.")
    return normalized


def create_author(payload: dict[str, Any], engine=None) -> dict[str, Any]:
    """Vytvori noveho autora v remote utb_authors."""
    engine = engine or get_remote_engine()
    data = _normalize_payload(payload, for_create=True, engine=engine)
    columns = list(data.keys())
    placeholders = [f":{name}" for name in columns]
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO "{_SCHEMA}"."{_TABLE}" ({', '.join(f'"{name}"' for name in columns)})
            VALUES ({', '.join(placeholders)})
        """), data)

    with engine.connect() as conn:
        created_ref = conn.execute(text(f"""
            SELECT ctid::text
            FROM "{_SCHEMA}"."{_TABLE}"
            WHERE display_name = :display_name
              AND author_id = :author_id
              AND poradie = :poradie
            ORDER BY ctid DESC
            LIMIT 1
        """), {
            "display_name": data["display_name"],
            "author_id": data["author_id"],
            "poradie": data["poradie"],
        }).scalar()
    return get_author(str(created_ref), engine=engine) or data


def update_author(row_ref: str, payload: dict[str, Any], engine=None) -> dict[str, Any]:
    """Upravi jeden konkretny riadok autora v remote utb_authors."""
    engine = engine or get_remote_engine()
    original = get_author(row_ref, engine=engine)
    if original is None:
        raise ValueError("Autor neexistuje.")
    data = _normalize_payload(payload, for_create=False, engine=engine)
    if not data:
        return original

    assignments = ", ".join(f'"{name}" = :{name}' for name in data.keys())
    params = dict(data)
    params["row_ref"] = row_ref
    with engine.begin() as conn:
        result = conn.execute(text(f"""
            UPDATE "{_SCHEMA}"."{_TABLE}"
            SET {assignments}
            WHERE ctid::text = :row_ref
        """), params)
    if result.rowcount == 0:
        raise ValueError("Autor neexistuje.")

    expected = dict(original)
    expected.update(data)
    with engine.connect() as conn:
        new_row_ref = conn.execute(text(f"""
            SELECT ctid::text
            FROM "{_SCHEMA}"."{_TABLE}"
            WHERE display_name = :display_name
              AND author_id = :author_id
              AND poradie = :poradie
            ORDER BY 1 DESC
            LIMIT 1
        """), {
            "display_name": expected.get("display_name"),
            "author_id": expected.get("author_id"),
            "poradie": expected.get("poradie"),
        }).scalar()
    return get_author(str(new_row_ref), engine=engine) or {"row_ref": row_ref, **data}


def delete_author(row_ref: str, engine=None) -> None:
    """Vymaze jeden konkretny riadok autora z remote utb_authors."""
    engine = engine or get_remote_engine()
    with engine.begin() as conn:
        result = conn.execute(text(f"""
            DELETE FROM "{_SCHEMA}"."{_TABLE}"
            WHERE ctid::text = :row_ref
        """), {"row_ref": str(row_ref)})
    if result.rowcount == 0:
        raise ValueError("Autor neexistuje.")
