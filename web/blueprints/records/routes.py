"""Hlavné trasy: zoznam záznamov a detail záznamu."""

from flask import render_template, request, redirect, url_for, abort, jsonify

from src.common.constants import FACULTIES, DEPARTMENTS
from web.blueprints.records import bp
from web.services.records_service import (
    fetch_unchecked_records,
    fetch_pending_records,
    search_records,
    SORT_OPTIONS,
    SEARCH_FIELD_CONDITIONS,
    GROUP_EXISTING,
    GROUP_DUPLICATE,
    GROUP_SINGLE,
)
from web.services.queue_service import get_record_detail, mark_checked, save_record_field, QUEUE_FIELDS
from web.services.authors_service import get_all_authors

GROUP_LABELS = {
    GROUP_EXISTING:  "Záznamy zodpovedajúce existujúcemu záznamu v repozitári",
    GROUP_DUPLICATE: "Záznamy importované z WoS aj Scopus",
    GROUP_SINGLE:    "Záznamy len z jedného zdroja (WoS alebo Scopus)",
}

SORT_LABELS = {
    "oldest":  "od najstaršieho",
    "newest":  "od najmladšieho",
    "journal": "podľa časopisu",
    "volume":  "podľa volume/issue",
}


@bp.route("/")
def index():
    sort = request.args.get("sort", "oldest")
    if sort not in SORT_OPTIONS:
        sort = "oldest"

    groups = fetch_unchecked_records(sort=sort)
    pending = fetch_pending_records()

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
        )

    return render_template(
        "records/list.html",
        group_list=group_list,
        sort=sort,
        sort_labels=SORT_LABELS,
        total=total,
        pending=pending,
    )


@bp.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"results": []})

    fields            = request.args.getlist("fields") or list(SEARCH_FIELD_CONDITIONS.keys())
    include_processed = request.args.get("include_processed", "0") == "1"
    limit             = min(int(request.args.get("limit", "10")), 200)

    results = search_records(q, fields, include_processed, limit)
    return jsonify({"results": results})


@bp.route("/record/<resource_id>")
def record_detail(resource_id: str):
    detail = get_record_detail(resource_id)
    if not detail:
        abort(404)

    authors = get_all_authors()
    doi = detail["main"].get("dc.identifier.doi")
    if isinstance(doi, list):
        doi = doi[0] if doi else None

    return render_template(
        "record/detail.html",
        detail=detail,
        utb_authors=authors,
        doi=doi,
        resource_id=resource_id,
        faculties=FACULTIES,
        departments=DEPARTMENTS,
    )


@bp.route("/record/<resource_id>/save-fields", methods=["POST"])
def save_fields(resource_id: str):
    """Uloží zmenené polia záznamu (JSON body: {fields: {field_key: new_value}})."""
    data = request.get_json(silent=True) or {}
    fields: dict = data.get("fields", {})

    if not fields:
        return jsonify({"ok": True})

    errors: dict[str, str] = {}
    for field_key, new_value in fields.items():
        try:
            save_record_field(resource_id, field_key, str(new_value))
        except Exception as exc:
            errors[field_key] = str(exc)

    if errors:
        return jsonify({"ok": False, "errors": errors}), 422
    return jsonify({"ok": True})


@bp.route("/record/<resource_id>/approve", methods=["POST"])
def approve_record(resource_id: str):
    mark_checked(resource_id)
    # HTMX: po schválení presmeruje na ďalší záznam alebo zoznam
    if request.headers.get("HX-Request"):
        return (
            '<div class="alert alert-success">Záznam schválený.</div>'
            f'<script>setTimeout(()=>window.location="/",1500)</script>'
        )
    return redirect(url_for("records.index"))
