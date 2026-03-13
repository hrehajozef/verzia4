"""CLI vstupný bod pre UTB metadata pipeline.

Pipeline (odporúčané poradie):
  1. bootstrap           – skopíruje remote tabuľku do lokálnej DB
  2. import-authors      – importuje interných autorov (remote DB alebo CSV)
  3. validate-setup      – pridá validation stĺpce (spusti raz)
  4. validate            – validácia metadát (trailing spaces, mojibake, DOI, ...)
  5. dates-setup         – pridá DATE stĺpce (spusti raz)
  6. heuristics          – heuristické spracovanie mien a afiliácií autorov
  7. dates               – heuristické parsovanie dátumov
  8. llm                 – LLM spracovanie autorov (záznamy s needs_llm=TRUE)
  9. dates-llm           – LLM spracovanie dátumov (záznamy s date_needs_llm=TRUE)
 10. deduplicate         – identifikácia duplikátov

Príkazy štatistík:
  status         – štatistiky spracovania autorov
  dates-status   – štatistiky dátumov
  validate-status – štatistiky validácie
  dedup-status   – štatistiky deduplikácie

  export         – exportuje výsledky do CSV
"""

from __future__ import annotations

import csv
from pathlib import Path

import typer
from sqlalchemy import text

from src.config.settings import settings
from src.db.engines import get_local_engine, get_remote_engine, test_connection

app = typer.Typer(name="utb-pipeline", add_completion=False)


# ═══════════════════════════════════════════════════════════════════════
# BOOTSTRAP
# ═══════════════════════════════════════════════════════════════════════

@app.command()
def bootstrap(
    drop: bool = typer.Option(False, "--drop", help="Zmaže lokálnu tabuľku a vytvorí ju znova."),
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


# ═══════════════════════════════════════════════════════════════════════
# AUTORI
# ═══════════════════════════════════════════════════════════════════════

@app.command(name="import-authors")
def import_authors(
    csv_file: Path = typer.Option(
        Path("./data/autori_utb_oficial_utf8.csv"), "--csv",
        help="CSV súbor s internými autormi (surname;firstname, s hlavičkou).",
    ),
) -> None:
    """
    Import interných autorov z CSV do lokálnej tabuľky utb_internal_authors.

    Formát CSV: priezvisko;krstné_meno, 1 riadok = 1 osoba, s hlavičkou.

    Príklad:
      python -m src.cli import-authors
      python -m src.cli import-authors --csv data/autori_utb_oficial_utf8.csv

    Poznámka: import z remote DB (obd_prac / S_LIDE) je zatiaľ zakomentovaný
    v src/authors/internal.py – reaktivovať keď budú tabuľky dostupné.
    """
    from src.authors.internal import (
        clear_author_registry_cache,
        import_authors_to_db,
        load_authors_from_csv,
        setup_authors_table,
    )

    if not csv_file.exists():
        typer.echo(f"[CHYBA] CSV súbor neexistuje: {csv_file}", err=True)
        raise typer.Exit(1)

    engine = get_local_engine()
    setup_authors_table(engine)

    typer.echo(f"Načítavam autorov z CSV: {csv_file}")
    authors = load_authors_from_csv(csv_file)

    count = import_authors_to_db(authors, engine)
    clear_author_registry_cache()

    typer.echo(f"[OK] Importovaných záznamov: {count}")


# ═══════════════════════════════════════════════════════════════════════
# VALIDÁCIA
# ═══════════════════════════════════════════════════════════════════════

@app.command(name="validate-setup")
def validate_setup() -> None:
    """
    Pridá validation stĺpce do lokálnej DB tabuľky.

    Spusti raz pred prvým spustením validate. Bezpečné spustiť opakovane.
    """
    from src.validation.checks import setup_validation_columns
    setup_validation_columns()


@app.command(name="validate")
def validate(
    limit:      int  = typer.Option(0,     "--limit",      help="Max počet záznamov (0 = všetky)."),
    batch_size: int  = typer.Option(500,   "--batch-size", help="Veľkosť dávky."),
    revalidate: bool = typer.Option(False, "--revalidate", help="Znovu validuje aj záznamy s existujúcim výsledkom."),
) -> None:
    """
    Validácia kvality metadát (trailing spaces, mojibake, DOI formát, interní autori).

    Spúšťa sa pred heuristickým spracovaním. Kontroly interných autorov
    sú k dispozícii až po spustení heuristics.

    Príklady:
      python -m src.cli validate
      python -m src.cli validate --limit 100 --revalidate
    """
    from src.validation.checks import run_validation
    run_validation(batch_size=batch_size, limit=limit, revalidate=revalidate)


@app.command(name="validate-status")
def validate_status() -> None:
    """Štatistiky validácie metadát."""
    from src.validation.checks import print_validation_status
    print_validation_status()


# ═══════════════════════════════════════════════════════════════════════
# HEURISTIKY – AUTORI
# ═══════════════════════════════════════════════════════════════════════

@app.command()
def heuristics(
    limit:            int        = typer.Option(0,     "--limit",            help="Max počet záznamov (0 = všetky)."),
    batch_size:       int | None = typer.Option(None,  "--batch-size",       help="Veľkosť dávky."),
    reprocess_errors: bool       = typer.Option(False, "--reprocess-errors", help="Spracovať aj záznamy so statusom error."),
    normalize:        bool       = typer.Option(False, "--normalize",        help="Porovnávať mená aj na normalizovaných hodnotách (bez diakritiky, lowercase) + fuzzy. Štandardne vypnuté – porovnáva sa na surových hodnotách."),
) -> None:
    """Heuristické spracovanie mien a afiliácií autorov."""
    from src.heuristics.authors import run_heuristics
    run_heuristics(batch_size=batch_size, limit=limit, reprocess_errors=reprocess_errors, normalize=normalize)


@app.command()
def llm(
    limit:      int        = typer.Option(0,    "--limit",      help="Max počet záznamov (0 = všetky)."),
    batch_size: int | None = typer.Option(None, "--batch-size", help="Veľkosť dávky."),
    provider:   str | None = typer.Option(None, "--provider",   help="ollama alebo openai."),
) -> None:
    """LLM spracovanie autorov (záznamy s needs_llm=TRUE, po heuristikách)."""
    from src.llm.runners.authors import run_llm
    run_llm(batch_size=batch_size, limit=limit, provider=provider)


@app.command()
def status() -> None:
    """Štatistiky spracovania mien a afiliácií."""
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
    typer.echo("\nHeuristiky (mená):")
    for row in heuristic_rows:
        typer.echo(f"  {row.heuristic_status}: {row.cnt}")
    typer.echo("\nLLM (needs_llm=true):")
    for row in llm_rows:
        typer.echo(f"  {row.llm_status}: {row.cnt}")


# ═══════════════════════════════════════════════════════════════════════
# HEURISTIKY – DÁTUMY
# ═══════════════════════════════════════════════════════════════════════

@app.command(name="dates-setup")
def dates_setup() -> None:
    """
    Pridá DATE stĺpce do lokálnej DB tabuľky.

    Spusti raz pred prvým spracovaním dátumov. Bezpečné spustiť opakovane.
    """
    from src.heuristics.dates import setup_date_columns
    setup_date_columns()


@app.command(name="dates")
def dates_run(
    limit:      int  = typer.Option(0,     "--limit",      help="Max počet záznamov (0 = všetky)."),
    batch_size: int  = typer.Option(200,   "--batch-size", help="Veľkosť dávky."),
    reprocess:  bool = typer.Option(False, "--reprocess",  help="Spracovať aj záznamy so statusom error."),
) -> None:
    """
    Heuristické parsovanie dátumov z utb_fulltext_dates.

    Príklady:
      python -m src.cli dates
      python -m src.cli dates --limit 50
      python -m src.cli dates --reprocess
    """
    from src.heuristics.dates import run_date_heuristics
    run_date_heuristics(batch_size=batch_size, limit=limit, reprocess=reprocess)


@app.command(name="dates-llm")
def dates_llm(
    limit:         int        = typer.Option(0,    "--limit",         help="Max počet záznamov (0 = všetky)."),
    batch_size:    int | None = typer.Option(None, "--batch-size",    help="Veľkosť dávky."),
    provider:      str | None = typer.Option(None, "--provider",      help="ollama alebo openai."),
    reprocess:     bool       = typer.Option(False, "--reprocess",    help="Spracovať aj záznamy s chybou."),
    include_dash:  bool       = typer.Option(False, "--include-dash", help="Spracovať aj záznamy kde utb.fulltext.dates = '{-}'. Štandardne preskočené."),
) -> None:
    """LLM spracovanie dátumov (záznamy s date_needs_llm=TRUE, po dates heuristikách)."""
    from src.llm.runners.dates import run_date_llm
    run_date_llm(batch_size=batch_size, limit=limit, provider=provider, reprocess=reprocess, include_dash=include_dash)


@app.command(name="dates-status")
def dates_status() -> None:
    """Štatistiky spracovania dátumov."""
    from src.heuristics.dates import print_date_status
    print_date_status()


# ═══════════════════════════════════════════════════════════════════════
# DEDUPLIKÁCIA
# ═══════════════════════════════════════════════════════════════════════

@app.command(name="deduplicate")
def deduplicate(
    by:              str   = typer.Option(
        "dc.identifier.doi", "--by",
        help="Stĺpec pre presnú zhodu (napr. 'dc.identifier.doi', 'dc.title').",
    ),
    no_fuzzy:        bool  = typer.Option(False,  "--no-fuzzy",   help="Vypne fuzzy porovnanie."),
    threshold:       float = typer.Option(0.0,    "--threshold",  help="Jaro-Winkler prah (0 = použije .env / default 0.85)."),
    dry_run:         bool  = typer.Option(False,  "--dry-run",    help="Iba vypíše výsledky, nezapíše do DB."),
) -> None:
    """
    Identifikácia a označenie duplikátov.

    Stratégia:
      1. Presná zhoda podľa stĺpca --by (case-insensitive)
      2. Fuzzy: podobnosť titulu + rok ±1 + ISSN/ISBN (ak nie je --no-fuzzy)

    Výsledok sa zapíše do flags['duplicates'] oboch záznamov.

    Príklady:
      python -m src.cli deduplicate
      python -m src.cli deduplicate --by dc.identifier.doi
      python -m src.cli deduplicate --by dc.title --no-fuzzy
      python -m src.cli deduplicate --dry-run
    """
    from src.deduplication.deduplicator import run_deduplication

    effective_threshold = threshold if threshold > 0.0 else settings.fuzzy_dedup_threshold

    run_deduplication(
        by_column       = by,
        fuzzy_fallback  = not no_fuzzy,
        title_threshold = effective_threshold,
        dry_run         = dry_run,
    )


@app.command(name="dedup-status")
def dedup_status() -> None:
    """Štatistiky deduplikácie."""
    from src.deduplication.deduplicator import print_dedup_status
    print_dedup_status()


# ═══════════════════════════════════════════════════════════════════════
# EXPORT
# ═══════════════════════════════════════════════════════════════════════

@app.command()
def export(
    output:   Path = typer.Option(Path("./data/vysledky_export.csv"), "--output", "-o"),
    only_llm: bool = typer.Option(False, "--only-llm", help="Exportuj len záznamy kde needs_llm=TRUE."),
) -> None:
    """Export výsledkov do CSV."""
    engine = get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table

    query = f"""
        SELECT resource_id,
               heuristic_status, needs_llm,
               utb_contributor_internalauthor,
               utb_faculty, utb_ou,
               llm_status, llm_result,
               utb_date_received, utb_date_reviewed, utb_date_accepted,
               utb_date_published_online, utb_date_published,
               date_heuristic_status, date_needs_llm, date_llm_status,
               validation_status, validation_flags
        FROM "{schema}"."{table}"
    """
    if only_llm:
        query += " WHERE needs_llm = TRUE"

    with engine.connect() as conn:
        result  = conn.execute(text(query))
        rows    = result.fetchall()
        columns = list(result.keys())

    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(columns)
        writer.writerows(rows)

    typer.echo(f"[OK] Exportovaných záznamov: {len(rows)} → {output}")


# ═══════════════════════════════════════════════════════════════════════
# VSTUPNÝ BOD
# ═══════════════════════════════════════════════════════════════════════

def main() -> None:
    app()

if __name__ == "__main__":
    main()
