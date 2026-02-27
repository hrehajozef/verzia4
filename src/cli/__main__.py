"""CLI vstupný bod pre UTB metadata pipeline."""

from __future__ import annotations

import csv
from pathlib import Path

import typer
from sqlalchemy import text

from src.config.settings import settings
from src.db.engines import get_local_engine, get_remote_engine, test_connection

# Vytvorenie
app = typer.Typer(name="utb-pipeline", add_completion=False)

@app.command()
def bootstrap(drop: bool = typer.Option(False, "--drop", help="Zmaže lokálnu tabuľku a vytvorí ju znova.")) -> None:
    """
    Inicializácia (bootstrap) lokálnej databázy - vytvorí tabuľku pre výsledky spracovania
    Prípadne zmaže existujúcu tabuľku, ak je zahrnutý flag --drop.
    """
    from src.db.bootstrap import run_bootstrap

    typer.echo("Testujem DB pripojenia...")
    if not test_connection(get_remote_engine(), "Remote DB"):
        raise typer.Exit(1)
    if not test_connection(get_local_engine(), "Lokálna DB"):
        raise typer.Exit(1)

    if drop:
        typer.confirm("Naozaj zmazať lokálnu tabuľku?", abort=True)

    run_bootstrap(drop_existing=drop)


@app.command(name="import-authors")
def import_authors(
    csv_file: Path = typer.Option(
        Path("./data/autori_utb_oficial_utf8.csv"),
        "--csv",
        exists=True,
        readable=True,
        help="CSV súbor s internými autormi.",
    )
) -> None:
    """
    Import autorov z csv súboru do lokálnej tabuľky utb_internal_authors
    """
    from src.authors.internal import (
        clear_author_registry_cache,
        import_authors_to_db,
        load_authors_from_csv,
        setup_authors_table,
    )

    authors = load_authors_from_csv(csv_file)
    engine = get_local_engine()
    setup_authors_table(engine)
    count = import_authors_to_db(authors, engine)
    clear_author_registry_cache()
    typer.echo(f"[OK] Importovaných autorov: {count}")


@app.command()
def heuristics(
    limit: int = typer.Option(0, "--limit", help="Maximálny počet záznamov (0 = bez limitu)."),
    batch_size: int | None = typer.Option(None, "--batch-size", help="Veľkosť dávky."),
    reprocess_errors: bool = typer.Option(False, "--reprocess-errors", help="Spracovať aj záznamy so statusom error."),
) -> None:
    """
    Spustenie heuristického spracovania mien autorov
    """
    from src.heuristics.runner import run_heuristics
    run_heuristics(batch_size=batch_size, limit=limit, reprocess_errors=reprocess_errors)


@app.command()
def llm(
    limit: int = typer.Option(0, "--limit", help="Maximálny počet záznamov (0 = bez limitu)."),
    batch_size: int | None = typer.Option(None, "--batch-size", help="Veľkosť dávky."),
    provider: str | None = typer.Option(None, "--provider", help="ollama alebo openai."),
) -> None:
    """
    Spustenie LLM spracovania mien autorov (po heuristike)
    """
    from src.llm.runner import run_llm
    run_llm(batch_size=batch_size, limit=limit, provider=provider)


@app.command()
def status() -> None:
    """
    Vypíše štatistické údaje z lokálnej tabuľky public.utb_metadata_arr
    """
    engine = get_local_engine()
    schema = settings.local_schema
    table = settings.local_table

    with engine.connect() as conn:
        total = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')).scalar_one()
        with_authors = conn.execute(
            text(
                f'SELECT COUNT(*) FROM "{schema}"."{table}" '
                "WHERE utb_contributor_internalauthor IS NOT NULL"
            )
        ).scalar_one()
        heuristic_rows = conn.execute(
            text(
                f"""
                SELECT heuristic_status, COUNT(*) AS cnt
                FROM "{schema}"."{table}"
                GROUP BY heuristic_status
                ORDER BY cnt DESC
                """
            )
        ).fetchall()
        llm_rows = conn.execute(
            text(
                f"""
                SELECT llm_status, COUNT(*) AS cnt
                FROM "{schema}"."{table}"
                WHERE needs_llm = TRUE
                GROUP BY llm_status
                ORDER BY cnt DESC
                """
            )
        ).fetchall()

    typer.echo(f"Celkom záznamov: {total}")
    typer.echo(f"So zisteným autorom: {with_authors}")
    typer.echo("Heuristiky:")
    for row in heuristic_rows:
        typer.echo(f"  {row.heuristic_status}: {row.cnt}")
    typer.echo("LLM:")
    for row in llm_rows:
        typer.echo(f"  {row.llm_status}: {row.cnt}")


@app.command()
def export(
    output: Path = typer.Option(Path("./data/vysledky_export.csv"), "--output", "-o"),
    only_llm: bool = typer.Option(False, "--only-llm"),
) -> None:
    """
    Export výsledkov z lokálnej tabuľky do CSV súboru.
     - --only-llm: Exportuje len záznamy, ktoré potrebujú LLM spracovanie (needs_llm = TRUE).
     - --output: Cesta k výstupnému CSV súboru (default: ./data/vysledky_export.csv).
    """
    engine = get_local_engine()
    schema = settings.local_schema
    table = settings.local_table

    query = f"""
        SELECT
            resource_id,
            heuristic_status,
            needs_llm,
            utb_contributor_internalauthor AS heuristic_authors,
            utb_faculty AS heuristic_faculty,
            utb_ou AS heuristic_ou,
            llm_status,
            llm_result
        FROM "{schema}"."{table}"
    """
    if only_llm:
        query += " WHERE needs_llm = TRUE"

    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()
        columns = result.keys()

    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(columns)
        writer.writerows(rows)

    typer.echo(f"[OK] Exportovaných záznamov: {len(rows)}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
