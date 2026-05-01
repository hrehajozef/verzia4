"""API endpointy pre remote tabulku veda.utb_authors."""

from __future__ import annotations

from flask import Blueprint, jsonify, render_template_string, request

from web.services.authors_service import (
    can_write_authors,
    create_author,
    get_all_authors,
    get_author,
    get_author_editor_config,
    get_author_modal_details,
    get_full_authors,
    search_authors,
    update_author,
)

bp = Blueprint("authors_api", __name__)

_AUTHORS_LIST_TMPL = """
{% for author in authors %}
<div class="author-row d-flex justify-content-between align-items-center py-1 border-bottom"
     data-name="{{ author.display_name }}"
     data-row-ref="{{ author.row_ref }}"
     data-author-name="{{ author.primary or author.display_name }}">
  <div class="author-row-main" style="min-width:0;">
    <span class="fw-semibold">{{ author.primary }}</span>
    {% if author.affiliations %}
    <div class="small text-muted mt-1">
      {% for aff in author.affiliations %}
      <div>&#8627; {{ aff.faculty or "—" }} / {{ aff.department or "—" }}</div>
      {% endfor %}
    </div>
    {% endif %}
    {% if author.variants|length > 1 %}
    <br><small class="text-muted">
      {% for v in author.variants[1:] %}{{ v }}{% if not loop.last %}<br>{% endif %}{% endfor %}
    </small>
    {% endif %}
  </div>
  <button class="btn p-0 px-1 text-secondary author-menu-btn flex-shrink-0 ms-1"
          style="font-size:1rem; line-height:1.4; background:none; border:none;"
          type="button"
          data-row-ref="{{ author.row_ref }}"
          data-author-name="{{ author.primary or author.display_name }}">&#8942;</button>
</div>
{% else %}
<p class="text-muted small">Žiadni autori.</p>
{% endfor %}
"""


def _request_payload() -> dict:
    return request.get_json(silent=True) or request.form.to_dict(flat=True)


@bp.route("/authors", methods=["GET"])
def list_authors():
    q = request.args.get("q", "").strip()
    authors = search_authors(q) if q else get_all_authors()
    return render_template_string(_AUTHORS_LIST_TMPL, authors=authors)


@bp.route("/authors/full", methods=["GET"])
def list_full_authors():
    q = request.args.get("q", "").strip()
    row_ref = request.args.get("row_ref", "").strip() or None
    authors = get_full_authors(query=q, row_ref=row_ref, limit=100)
    return jsonify({
        "authors": authors,
        "editor": get_author_editor_config(),
        "can_write": can_write_authors(),
    })


@bp.route("/authors", methods=["POST"])
def create_author_route():
    payload = _request_payload()
    try:
        author = create_author(payload)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 422
    return jsonify({"ok": True, "author": author})


@bp.route("/authors/row", methods=["GET"])
def get_author_route():
    row_ref = (request.args.get("row_ref") or "").strip()
    if not row_ref:
        return jsonify({"ok": False, "error": "Chýba row_ref."}), 400
    author = get_author(row_ref)
    if not author:
        return jsonify({"ok": False, "error": "Autor neexistuje."}), 404
    return jsonify({
        "ok": True,
        "author": author,
        "summary": get_author_modal_details(row_ref),
        "editor": get_author_editor_config(),
    })


@bp.route("/authors/row", methods=["PATCH"])
def update_author_route():
    payload = _request_payload()
    row_ref = str(payload.pop("row_ref", "") or "").strip()
    if not row_ref:
        return jsonify({"ok": False, "error": "Chýba row_ref."}), 400
    try:
        author = update_author(row_ref, payload)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 422
    return jsonify({"ok": True, "author": author})
