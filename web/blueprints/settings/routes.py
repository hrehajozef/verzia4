"""UI settings routes."""

from flask import flash, redirect, render_template, request, url_for

from web.blueprints.settings import bp
from web.services.queue_service import (
    PRIORITY_FIELDS,
    get_detail_row_order,
    reset_detail_row_order,
    save_detail_row_order,
)


@bp.route("/row-order", methods=["GET", "POST"])
def row_order():
    if request.method == "POST":
        action = request.form.get("action", "save")
        if action == "reset":
            reset_detail_row_order()
            flash("Poradie riadkov bolo resetované na predvolené.", "success")
        else:
            raw = request.form.get("fields", "")
            fields = [line.strip() for line in raw.splitlines() if line.strip()]
            save_detail_row_order(fields)
            flash("Poradie riadkov bolo uložené.", "success")
        return redirect(url_for("settings.row_order"))

    current_order = get_detail_row_order()
    return render_template(
        "settings/row_order.html",
        current_order=current_order,
        default_order=PRIORITY_FIELDS,
    )
