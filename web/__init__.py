"""Flask web aplikácia pre UTB metadata review."""

from flask import Flask

from src.config.settings import settings


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = settings.flask_secret_key

    from web.blueprints.records import bp as records_bp
    from web.blueprints.api.authors import bp as authors_bp
    from web.blueprints.api.crossref import bp as crossref_bp

    app.register_blueprint(records_bp)
    app.register_blueprint(authors_bp, url_prefix="/api")
    app.register_blueprint(crossref_bp, url_prefix="/api")

    return app
