"""Hlavné trasy: zoznam záznamov a detail záznamu."""

from urllib.parse import urlparse

from flask import render_template, request, redirect, url_for, abort, jsonify

from src.common.constants import FACULTIES, DEPARTMENTS, QUEUE_TABLE
from src.config.settings import settings
from web.blueprints.records import bp
from web.services.records_service import (
    fetch_unchecked_records,
    fetch_pending_records,
    local_table_exists,
    search_records,
    SORT_OPTIONS,
    SEARCH_FIELD_CONDITIONS,
    GROUP_EXISTING,
    GROUP_DUPLICATE,
    GROUP_SINGLE,
)
from web.services.queue_service import (
    get_history_record_detail,
    get_record_detail,
    mark_checked,
    save_record_field,
    QUEUE_FIELDS,
)
from web.services.authors_service import (
    create_author,
    delete_author,
    get_author,
    get_author_editor_config,
    get_record_sidebar_data,
    update_author,
)


def _save_fields_payload(resource_id: str, fields: dict[str, object]) -> dict[str, str]:
    errors: dict[str, str] = {}
    for field_key, new_value in fields.items():
        try:
            save_record_field(resource_id, field_key, str(new_value))
        except Exception as exc:
            errors[field_key] = str(exc)
    return errors


def _safe_return_to(raw_url: str | None) -> str:
    if not raw_url:
        return url_for("records.index")
    parsed = urlparse(raw_url)
    if parsed.scheme or parsed.netloc:
        return url_for("records.index")
    path = parsed.path or "/"
    if not path.startswith("/"):
        path = "/" + path
    return f"{path}?{parsed.query}" if parsed.query else path

GROUP_LABELS = {
    GROUP_EXISTING:  "Záznamy zodpovedajúce existujúcemu záznamu v repozitári",
    GROUP_DUPLICATE: "Záznamy importované z WoS aj Scopus",
    GROUP_SINGLE:    "Záznamy len z jedného zdroja (WoS alebo Scopus)",
}


REQUIRED_LOCAL_TABLES: tuple[str, ...] = (
    settings.local_table,   # utb_metadata_arr (z .env)
    QUEUE_TABLE,            # utb_processing_queue
)


def _local_db_status() -> tuple[bool, list[str]]:
    """Vráti (ready, missing_tables).

    Skontroluje len existenciu tabuliek – nezahadzuje výnimky pri zlom
    pripojení. Ak nie sú dostupné, ready=False a missing_tables obsahuje
    názvy chýbajúcich tabuliek (alebo placeholder ak DB sa nedá pripojiť).
    """
    missing: list[str] = []
    for table_name in REQUIRED_LOCAL_TABLES:
        if not table_name:
            missing.append("(LOCAL_TABLE nie je v .env)")
            continue
        try:
            if not local_table_exists(table_name):
                missing.append(f"{settings.local_schema}.{table_name}")
        except Exception as exc:  # napr. DB nedostupná
            missing.append(f"{settings.local_schema}.{table_name} (chyba pripojenia: {exc})")
    return (not missing, missing)


def _local_db_ready() -> bool:
    ready, _ = _local_db_status()
    return ready

SORT_LABELS = {
    "id_asc":  "ID rastúco",
    "id_desc": "ID klesajúco",
    "oldest":  "od najstaršieho",
    "newest":  "od najmladšieho",
    "journal": "podľa časopisu",
    "volume":  "podľa volume/issue",
}


@bp.route("/")
def index():
    sort = request.args.get("sort", "id_asc")
    show_approved = request.args.get("show_approved", "0") == "1"
    if sort not in SORT_OPTIONS:
        sort = "id_asc"

    db_ready = _local_db_ready()
    bootstrap_needed = not db_ready
    bootstrap_message = None
    fetch_error: str | None = None
    groups = {
        GROUP_EXISTING: [],
        GROUP_DUPLICATE: [],
        GROUP_SINGLE: [],
    }
    pending = []
    if db_ready:
        try:
            groups = fetch_unchecked_records(sort=sort, include_checked=show_approved)
            pending = fetch_pending_records()
        except Exception as exc:
            fetch_error = (
                "Nepodarilo sa načítať záznamy z lokálnej DB. "
                f"Detail: {type(exc).__name__}: {exc}"
            )
            print(f"[records.index] {fetch_error}")
    else:
        try:
            _, missing = _local_db_status()
        except Exception:
            missing = []
        missing_text = (
            " Chýbajú tabuľky: " + ", ".join(missing) + "." if missing else ""
        )
        bootstrap_message = (
            "Lokálna databáza ešte nie je inicializovaná." + missing_text
            + " Otvorte Nastavenia a spustite bootstrap."
        )

    group_list = [
        {
            "key":    key,
            "label":  GROUP_LABELS[key],
            "records": groups[key],
            "count":  len(groups[key]),
        }
        for key in (GROUP_EXISTING, GROUP_DUPLICATE, GROUP_SINGLE)
    ]

    total = sum(len(groups[k]) for k in groups)

    # HTMX partial – vráti len zoznam bez celého layoutu
    if request.headers.get("HX-Request"):
        return render_template(
            "records/_groups.html",
            group_list=group_list,
            sort=sort,
            sort_labels=SORT_LABELS,
            show_approved=show_approved,
            bootstrap_needed=bootstrap_needed,
        )

    return render_template(
        "records/list.html",
        group_list=group_list,
        sort=sort,
        sort_labels=SORT_LABELS,
        show_approved=show_approved,
        total=total,
        pending=pending,
        bootstrap_needed=bootstrap_needed,
        bootstrap_message=bootstrap_message,
        fetch_error=fetch_error,
    )


@bp.route("/api/search")
def api_search():
    if not _local_db_ready():
        return jsonify({"results": []})

    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"results": []})

    fields            = request.args.getlist("fields") or list(SEARCH_FIELD_CONDITIONS.keys())
    include_processed = request.args.get("include_processed", "0") == "1"
    limit             = min(int(request.args.get("limit", "10")), 200)

    results = search_records(q, fields, include_processed, limit)
    return jsonify({"results": results})


def _safe_get_record_sidebar_data() -> dict:
    try:
        return get_record_sidebar_data()
    except Exception as exc:
        print(f"[record_detail] get_record_sidebar_data zlyhalo: {type(exc).__name__}: {exc}")
        return {
            "authors": [],
            "editor": {"columns": [], "can_write": False, "faculty_options": []},
        }


@bp.route("/record/<resource_id>")
def record_detail(resource_id: str):
    if not _local_db_ready():
        return redirect(url_for("records.index"))

    try:
        detail = get_record_detail(resource_id)
    except Exception as exc:
        print(f"[record_detail] get_record_detail zlyhalo pre {resource_id}: "
              f"{type(exc).__name__}: {exc}")
        return render_template(
            "records/list.html",
            group_list=[],
            sort="id_asc",
            sort_labels=SORT_LABELS,
            show_approved=False,
            total=0,
            pending=[],
            bootstrap_needed=False,
            bootstrap_message=None,
            fetch_error=(
                f"Nepodarilo sa otvoriť záznam {resource_id}: "
                f"{type(exc).__name__}: {exc}"
            ),
        ), 500
    if not detail:
        abort(404)

    sidebar_data = _safe_get_record_sidebar_data()
    doi = detail["main"].get("dc.identifier.doi")
    if isinstance(doi, list):
        doi = doi[0] if doi else None

    return render_template(
        "record/detail.html",
        detail=detail,
        utb_authors=sidebar_data["authors"],
        author_editor=sidebar_data["editor"],
        doi=doi,
        resource_id=resource_id,
        faculties=FACULTIES,
        departments=DEPARTMENTS,
    )


@bp.route("/record/history")
def record_history_detail():
    if not _local_db_ready():
        return redirect(url_for("records.index"))

    history_row_ref = request.args.get("row_ref", "").strip()
    if not history_row_ref:
        abort(404)

    try:
        detail = get_history_record_detail(history_row_ref)
    except Exception as exc:
        print(f"[record_history_detail] zlyhalo: {type(exc).__name__}: {exc}")
        abort(404)
    if not detail:
        abort(404)

    sidebar_data = _safe_get_record_sidebar_data()
    doi = detail["main"].get("dc.identifier.doi")
    if isinstance(doi, list):
        doi = doi[0] if doi else None

    return render_template(
        "record/detail.html",
        detail=detail,
        utb_authors=sidebar_data["authors"],
        author_editor=sidebar_data["editor"],
        doi=doi,
        resource_id=detail["resource_id"],
        faculties=FACULTIES,
        departments=DEPARTMENTS,
    )


@bp.route("/authors/editor", methods=["GET", "POST"])
def author_editor():
    row_ref = (request.values.get("row_ref") or "").strip()
    return_to = _safe_return_to(request.values.get("return_to") or request.referrer)
    editor = get_author_editor_config()
    columns = editor.get("columns", [])

    if request.method == "POST":
        payload = {
            column["name"]: request.form.get(column["name"], "")
            for column in columns
        }
        try:
            if row_ref:
                update_author(row_ref, payload)
            else:
                create_author(payload)
        except ValueError as exc:
            author = dict(payload)
            if row_ref:
                author["row_ref"] = row_ref
            return render_template(
                "record/author_editor.html",
                mode="edit" if row_ref else "create",
                author=author,
                author_editor=editor,
                row_ref=row_ref,
                return_to=return_to,
                error=str(exc),
            ), 422
        return redirect(return_to)

    author = get_author(row_ref) if row_ref else {"utb": "ano"}
    if row_ref and not author:
        abort(404)

    return render_template(
        "record/author_editor.html",
        mode="edit" if row_ref else "create",
        author=author,
        author_editor=editor,
        row_ref=row_ref,
        return_to=return_to,
        error=None,
    )


@bp.route("/authors/editor/<path:row_ref>/delete", methods=["POST"])
def author_editor_delete(row_ref: str):
    return_to = _safe_return_to(request.form.get("return_to") or request.referrer)
    try:
        delete_author(row_ref)
    except ValueError:
        abort(404)
    return redirect(return_to)


@bp.route("/record/<resource_id>/save-fields", methods=["POST"])
def save_fields(resource_id: str):
    """Uloží zmenené polia záznamu (JSON body: {fields: {field_key: new_value}})."""
    data = request.get_json(silent=True) or {}
    fields: dict = data.get("fields", {})

    if not fields:
        return jsonify({"ok": True})

    errors = _save_fields_payload(resource_id, fields)
    if errors:
        return jsonify({"ok": False, "errors": errors}), 422
    return jsonify({"ok": True})


@bp.route("/record/<resource_id>/approve", methods=["POST"])
def approve_record(resource_id: str):
    data = request.get_json(silent=True) or {}
    fields: dict[str, object] = data.get("fields", {}) if isinstance(data, dict) else {}
    if fields:
        errors = _save_fields_payload(resource_id, fields)
        if errors:
            return jsonify({"ok": False, "errors": errors}), 422
    mark_checked(resource_id)
    if request.is_json:
        return jsonify({"ok": True, "redirect": url_for("records.index")})
    # HTMX: po schválení presmeruje na ďalší záznam alebo zoznam
    if request.headers.get("HX-Request"):
        return (
            '<div class="alert alert-success">Záznam schválený.</div>'
            f'<script>setTimeout(()=>window.location="/",1500)</script>'
        )
    return redirect(url_for("records.index"))
