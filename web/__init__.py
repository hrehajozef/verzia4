"""Flask web aplikácia pre UTB metadata review."""

from flask import Flask

from src.config.settings import settings


def _ensure_librarian_columns() -> None:
    """Adds librarian tracking columns to the queue table if they don't exist yet."""
    try:
        from sqlalchemy import text
        from src.common.constants import QUEUE_TABLE
        from src.db.engines import get_local_engine

        engine = get_local_engine()
        schema = settings.local_schema
        queue  = QUEUE_TABLE

        with engine.begin() as conn:
            conn.execute(text(f"""
                ALTER TABLE "{schema}"."{queue}"
                ADD COLUMN IF NOT EXISTS librarian_modified_at TIMESTAMPTZ
            """))
    except Exception:
        pass  # Non-critical – DB may not be available at startup


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = settings.flask_secret_key

    _ensure_librarian_columns()

    from web.blueprints.records import bp as records_bp
    from web.blueprints.api.authors import bp as authors_bp
    from web.blueprints.api.crossref import bp as crossref_bp
    from web.blueprints.pipeline import bp as pipeline_bp
    from web.blueprints.settings import bp as settings_bp

    app.register_blueprint(records_bp)
    app.register_blueprint(authors_bp, url_prefix="/api")
    app.register_blueprint(crossref_bp, url_prefix="/api")
    app.register_blueprint(pipeline_bp)
    app.register_blueprint(settings_bp)

    return app
