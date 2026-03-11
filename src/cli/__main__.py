"""CLI vstupný bod pre UTB metadata pipeline."""

from __future__ import annotations

import csv
from pathlib import Path

import typer
from sqlalchemy import text

from src.config.settings import settings
from src.db.engines import get_local_engine, get_remote_engine, test_connection

app = typer.Typer(name="utb-pipeline", add_completion=False)


@app.command()
def bootstrap(
    drop: bool = typer.Option(False, "--drop", help="Zmaže lokálnu tabuľku a vytvorí ju znova.")
) -> None:
    """Inicializácia lokálnej databázy – skopíruje remote tabuľku."""
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
    source: str = typer.Option(
        "remote",
        "--source",
        help="Zdroj autorov: 'remote' (školská DB) alebo 'csv' (súbor).",
    ),
    csv_file: Path = typer.Option(
        Path("./data/autori_utb_oficial_utf8.csv"),
        "--csv",
        help="CSV súbor (použije sa len ak --source=csv).",
    ),
) -> None:
    """
    Import interných autorov do lokálnej tabuľky utb_internal_authors.

    Predvolený zdroj je školská remote DB (obd_prac / obd_lideprac / S_LIDE).
    Obsahuje mená s diakritikou + kód a názov pracoviska.

    Príklady:
      python -m src.cli import-authors                    # z remote DB (default)
      python -m src.cli import-authors --source csv --csv autori.csv
    """
    from src.authors.internal import (
        clear_author_registry_cache,
        import_authors_to_db,
        load_authors_from_csv,
        load_authors_from_remote_db,
        setup_authors_table,
    )

    engine = get_local_engine()
    setup_authors_table(engine)

    if source.lower() == "csv":
        if not csv_file.exists():
            typer.echo(f"[CHYBA] CSV súbor neexistuje: {csv_file}", err=True)
            raise typer.Exit(1)
        typer.echo(f"Načítavam autorov z CSV: {csv_file}")
        authors = load_authors_from_csv(csv_file)
    else:
        typer.echo("Načítavam autorov zo školskej remote DB (obd_prac / S_LIDE)...")
        if not test_connection(get_remote_engine(), "Remote DB"):
            raise typer.Exit(1)
        authors = load_authors_from_remote_db(get_remote_engine())

    count = import_authors_to_db(authors, engine)
    clear_author_registry_cache()

    # Štatistiky
    with_workplace = sum(1 for a in authors if a.parent_name)
    typer.echo(f"[OK] Importovaných záznamov: {count}")
    typer.echo(f"     Z toho s pracoviskom:   {with_workplace}")
    typer.echo(f"     Bez pracoviska:         {count - with_workplace}")


@app.command()
def heuristics(
    limit:            int       = typer.Option(0,     "--limit",            help="Max počet záznamov (0 = všetky)."),
    batch_size:       int | None = typer.Option(None, "--batch-size",       help="Veľkosť dávky."),
    reprocess_errors: bool      = typer.Option(False, "--reprocess-errors", help="Spracovať aj záznamy so statusom error."),
) -> None:
    """Spustenie heuristického spracovania mien autorov."""
    from src.heuristics.runner import run_heuristics
    run_heuristics(batch_size=batch_size, limit=limit, reprocess_errors=reprocess_errors)


@app.command()
def llm(
    limit:      int        = typer.Option(0,     "--limit",      help="Max počet záznamov (0 = všetky)."),
    batch_size: int | None = typer.Option(None,  "--batch-size", help="Veľkosť dávky."),
    provider:   str | None = typer.Option(None,  "--provider",   help="ollama alebo openai."),
) -> None:
    """Spustenie LLM spracovania (po heuristike)."""
    from src.llm.runner import run_llm
    run_llm(batch_size=batch_size, limit=limit, provider=provider)


@app.command()
def status() -> None:
    """Štatistiky z lokálnej tabuľky."""
    engine = get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table

    with engine.connect() as conn:
        total = conn.execute(
            text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        ).scalar_one()
        with_authors = conn.execute(
            text(f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE utb_contributor_internalauthor IS NOT NULL')
        ).scalar_one()
        heuristic_rows = conn.execute(
            text(f'SELECT heuristic_status, COUNT(*) AS cnt FROM "{schema}"."{table}" GROUP BY heuristic_status ORDER BY cnt DESC')
        ).fetchall()
        llm_rows = conn.execute(
            text(f'SELECT llm_status, COUNT(*) AS cnt FROM "{schema}"."{table}" WHERE needs_llm = TRUE GROUP BY llm_status ORDER BY cnt DESC')
        ).fetchall()

    typer.echo(f"Celkom záznamov:      {total}")
    typer.echo(f"So zisteným autorom:  {with_authors}")
    typer.echo("Heuristiky:")
    for row in heuristic_rows:
        typer.echo(f"  {row.heuristic_status}: {row.cnt}")
    typer.echo("LLM (needs_llm=true):")
    for row in llm_rows:
        typer.echo(f"  {row.llm_status}: {row.cnt}")


@app.command()
def export(
    output:   Path = typer.Option(Path("./data/vysledky_export.csv"), "--output", "-o"),
    only_llm: bool = typer.Option(False, "--only-llm"),
) -> None:
    """Export výsledkov do CSV."""
    engine = get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table

    query = f"""
        SELECT resource_id, heuristic_status, needs_llm,
               utb_contributor_internalauthor,
               utb_faculty, utb_ou,
               llm_status, llm_result
        FROM "{schema}"."{table}"
    """
    if only_llm:
        query += " WHERE needs_llm = TRUE"

    with engine.connect() as conn:
        result  = conn.execute(text(query))
        rows    = result.fetchall()
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