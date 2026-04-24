"""CLI vstupny bod pre UTB metadata pipeline.

Odporucane poradie:
  1. bootstrap-local-db
  2. setup-processing-queue
  3. setup-dedup-history
  4. validate-metadata
  5. detect-authors
  6. extract-dates
  7. normalize-journals
  8. deduplicate-records

Volitelne LLM fallbacky:
  detect-authors-llm
  extract-dates-llm

Kontrolne prehlady:
  metadata-validation-status
  author-detection-status
  date-extraction-status
  journal-normalization-status
  deduplication-status
"""

from __future__ import annotations

import sys
import io
import typer
from sqlalchemy import text

# Windows terminál predvolene používa cp1250, ktoré nepodporuje všetky Unicode znaky.
# Prepneme stdout/stderr na UTF-8, aby sa dáta z DB (aj špeciálne znaky) tlačili správne.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from src.config.settings import settings
from src.db.engines import get_local_engine, get_remote_engine, test_connection

app = typer.Typer(name="utb-pipeline", add_completion=False)


# ═══════════════════════════════════════════════════════════════════════
# BOOTSTRAP
# ═══════════════════════════════════════════════════════════════════════

@app.command(name="bootstrap-local-db")
def bootstrap(
    drop: bool = typer.Option(False, "--drop", help="Zmaže lokálnu tabuľku a vytvorí ju znova."),
) -> None:
    """Inicializácia lokálnej databázy – skopíruje remote tabuľku."""
    from src.db.setup import run_bootstrap

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

# VALIDÁCIA
# ═══════════════════════════════════════════════════════════════════════

@app.command(name="setup-processing-queue")
def queue_setup() -> None:
    """
    Vytvorí tabuľku utb_processing_queue (medzitabuľka pre výstupy pipeline).

    Spusti raz po bootstrape, pred ostatnými setup príkazmi.
    Bezpečné spustiť opakovane.
    """
    from src.db.setup import setup_processing_queue
    setup_processing_queue()


# Deprecated setup alias: setup-processing-queue now prepares validation columns.
def validate_setup() -> None:
    """
    Pridá validation stĺpce do lokálnej DB tabuľky.

    Spusti raz pred prvým spustením validate. Bezpečné spustiť opakovane.
    """
    from src.quality.checks import setup_validation_columns
    setup_validation_columns()


@app.command(name="validate-metadata")
def validate(
    limit:      int  = typer.Option(0,     "--limit",      help="Max počet záznamov (0 = všetky)."),
    batch_size: int  = typer.Option(500,   "--batch-size", help="Veľkosť dávky."),
    revalidate: bool = typer.Option(False, "--revalidate", help="Znovu validuje aj záznamy s existujúcim výsledkom."),
) -> None:
    """
    Validácia kvality metadát + návrhy opráv.

    Kontroly: trailing spaces, mojibake, DOI formát, URL query params, OBDID existencia.
    Navrhnuté opravy sa uložia do validation_suggested_fixes – spusti 'apply-validation-fixes' na ich aplikovanie.

    Príklady:
      python -m src.cli validate-metadata
      python -m src.cli validate-metadata --limit 100 --revalidate
    """
    from src.quality.checks import run_validation
    run_validation(batch_size=batch_size, limit=limit, revalidate=revalidate)


@app.command(name="apply-validation-fixes")
def apply_fixes(
    preview: bool = typer.Option(False, "--preview", help="Zobraz farebný diff bez zápisu do DB."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Alias pre --preview."),
    limit:   int  = typer.Option(0,     "--limit",   help="Max počet záznamov (0 = všetky)."),
) -> None:
    """
    Aplikuje navrhnuté opravy z validácie (validation_suggested_fixes) do skutočných stĺpcov.

    Červenou sa zobrazí pôvodná hodnota, zelenou navrhnutá oprava (--preview).
    Po aplikovaní sa záznamy automaticky označia na re-validáciu.

    Príklady:
      python -m src.cli apply-validation-fixes --preview
      python -m src.cli apply-validation-fixes
      python -m src.cli apply-validation-fixes --limit 50
    """
    from src.quality.checks import run_apply_fixes
    run_apply_fixes(preview=preview, dry_run=dry_run, limit=limit)


@app.command(name="metadata-validation-status")
def validate_status() -> None:
    """Štatistiky validácie metadát."""
    from src.quality.checks import print_validation_status
    print_validation_status()


# ═══════════════════════════════════════════════════════════════════════
# HEURISTIKY – AUTORI
# ═══════════════════════════════════════════════════════════════════════

@app.command(name="detect-authors")
def heuristics_run(
    limit:            int        = typer.Option(0,     "--limit",            help="Max počet záznamov (0 = všetky)."),
    batch_size:       int | None = typer.Option(None,  "--batch-size",       help="Veľkosť dávky."),
    reprocess_errors: bool       = typer.Option(False, "--reprocess-errors", help="Spracovať aj záznamy so statusom error."),
    reprocess:        bool       = typer.Option(False, "--reprocess",        help="Spracovať aj už spracované záznamy (status processed)."),
    normalize:        bool       = typer.Option(False, "--normalize",        help="Porovnávať mená aj na normalizovaných hodnotách (bez diakritiky, lowercase) + fuzzy. Štandardne vypnuté – porovnáva sa na surových hodnotách."),
) -> None:
    """Heuristické spracovanie mien a afiliácií autorov."""
    from src.authors.heuristics import run_heuristics
    run_heuristics(batch_size=batch_size, limit=limit, reprocess_errors=reprocess_errors, reprocess=reprocess, normalize=normalize)


@app.command(name="detect-authors-llm")
def heuristics_llm(
    limit:      int        = typer.Option(0,    "--limit",      help="Max počet záznamov (0 = všetky)."),
    batch_size: int | None = typer.Option(None, "--batch-size", help="Veľkosť dávky."),
    provider:   str | None = typer.Option(None, "--provider",   help="ollama alebo openai."),
    reprocess:  bool       = typer.Option(False, "--reprocess", help="Spracuje znova aj záznamy so stavom processed alebo validation_error."),
) -> None:
    """LLM spracovanie autorov (záznamy s needs_llm=TRUE, po heuristikách)."""
    from src.llm.tasks.authors import run_llm
    run_llm(batch_size=batch_size, limit=limit, provider=provider, reprocess=reprocess)


@app.command(name="compare-author-detection")
def heuristics_compare() -> None:
    """
    Porovná author_internal_names (program) vs utb.contributor.internalauthor (knihovník).

    Zobrazí štatistiky: presná zhoda, čiastočná zhoda, bez prieniku, len jeden zdroj.
    """
    from src.authors.heuristics import compare_with_librarian
    compare_with_librarian()


@app.command(name="author-detection-status")
def status() -> None:
    """Štatistiky spracovania mien a afiliácií."""
    from src.common.constants import QUEUE_TABLE
    engine = get_local_engine()
    schema = settings.local_schema
    queue  = QUEUE_TABLE

    with engine.connect() as conn:
        total = conn.execute(
            text(f'SELECT COUNT(*) FROM "{schema}"."{queue}"')
        ).scalar_one()
        with_authors = conn.execute(
            text(f'SELECT COUNT(*) FROM "{schema}"."{queue}" WHERE author_internal_names IS NOT NULL')
        ).scalar_one()
        heuristic_rows = conn.execute(
            text(f'SELECT author_heuristic_status, COUNT(*) AS cnt FROM "{schema}"."{queue}" GROUP BY author_heuristic_status ORDER BY cnt DESC')
        ).fetchall()
        llm_rows = conn.execute(
            text(f'SELECT author_llm_status, COUNT(*) AS cnt FROM "{schema}"."{queue}" WHERE author_needs_llm = TRUE GROUP BY author_llm_status ORDER BY cnt DESC')
        ).fetchall()

    typer.echo(f"Celkom záznamov:      {total}")
    typer.echo(f"So zisteným autorom:  {with_authors}")
    typer.echo("\nHeuristiky (mená):")
    for row in heuristic_rows:
        typer.echo(f"  {row.author_heuristic_status}: {row.cnt}")
    typer.echo("\nLLM (author_needs_llm=true):")
    for row in llm_rows:
        typer.echo(f"  {row.author_llm_status}: {row.cnt}")


# ═══════════════════════════════════════════════════════════════════════
# HEURISTIKY – DÁTUMY
# ═══════════════════════════════════════════════════════════════════════

# Deprecated setup alias: setup-processing-queue now prepares date columns.
def dates_setup() -> None:
    """
    Pridá DATE stĺpce do lokálnej DB tabuľky.

    Spusti raz pred prvým spracovaním dátumov. Bezpečné spustiť opakovane.
    """
    from src.dates.heuristics import setup_date_columns
    setup_date_columns()


@app.command(name="extract-dates")
def dates_run(
    limit:      int  = typer.Option(0,     "--limit",      help="Max počet záznamov (0 = všetky)."),
    batch_size: int  = typer.Option(200,   "--batch-size", help="Veľkosť dávky."),
    reprocess:  bool = typer.Option(False, "--reprocess",  help="Spracovať aj záznamy so statusom error."),
) -> None:
    """
    Heuristické parsovanie dátumov z utb_fulltext_dates.

    Príklady:
      python -m src.cli extract-dates
      python -m src.cli extract-dates --limit 50
      python -m src.cli extract-dates --reprocess
    """
    from src.dates.heuristics import run_date_heuristics
    run_date_heuristics(batch_size=batch_size, limit=limit, reprocess=reprocess)


@app.command(name="extract-dates-llm")
def dates_llm(
    limit:         int        = typer.Option(0,    "--limit",         help="Max počet záznamov (0 = všetky)."),
    batch_size:    int | None = typer.Option(None, "--batch-size",    help="Veľkosť dávky."),
    provider:      str | None = typer.Option(None, "--provider",      help="ollama alebo openai."),
    reprocess:     bool       = typer.Option(False, "--reprocess",    help="Spracovať aj záznamy s chybou."),
    include_dash:  bool       = typer.Option(False, "--include-dash", help="Spracovať aj záznamy kde utb.fulltext.dates = '{-}'. Štandardne preskočené."),
) -> None:
    """LLM spracovanie dátumov (záznamy s date_needs_llm=TRUE, po dates heuristikách)."""
    from src.llm.tasks.dates import run_date_llm
    run_date_llm(batch_size=batch_size, limit=limit, provider=provider, reprocess=reprocess, include_dash=include_dash)


@app.command(name="date-extraction-status")
def dates_status() -> None:
    """Štatistiky spracovania dátumov."""
    from src.dates.heuristics import print_date_status
    print_date_status()


# ═══════════════════════════════════════════════════════════════════════
# DEDUPLIKÁCIA
# ═══════════════════════════════════════════════════════════════════════

@app.command(name="setup-dedup-history")
def dedup_setup() -> None:
    """
    Vytvorí tabuľku dedup_histoire pre uchovanie histórie pred zlúčením.

    Spusti raz pred prvým spustením deduplicate-records. Bezpečné spustiť opakovane.
    """
    from src.quality.dedup import setup_dedup_table
    setup_dedup_table()


@app.command(name="deduplicate-records")
def deduplicate(
    by:        str   = typer.Option(
        "dc.identifier.doi", "--by",
        help="Stĺpec pre presnú zhodu (napr. 'dc.identifier.doi').",
    ),
    no_fuzzy:  bool  = typer.Option(False, "--no-fuzzy",  help="Vypne fuzzy porovnanie."),
    threshold: float = typer.Option(0.0,   "--threshold", help="Jaro-Winkler prah (0 = .env / default 0.85)."),
    dry_run:   bool  = typer.Option(False, "--dry-run",   help="Iba vypíše výsledky, nezapíše do DB."),
) -> None:
    """
    Deduplikácia: nájde a fyzicky zlúči duplikáty, zachová históriu.

    Stratégie:
      1. Presná zhoda podľa --by (default: DOI)
      2. Obsahová zhoda 100% (title+autori+abstrakt) → early_access / merged_type / autoplagiat
      3. Fuzzy zhoda titulu ≥ threshold (len flag, bez zlúčenia)

    Záznamy exact/early_access/merged_type sú fyzicky zlúčené (UPDATE+DELETE),
    pred tým nakopírované do dedup_histoire. Autoplagiát a fuzzy sú len flagované.

    Príklady:
      python -m src.cli deduplicate-records
      python -m src.cli deduplicate-records --dry-run
      python -m src.cli deduplicate-records --no-fuzzy
    """
    from src.quality.dedup import run_deduplication

    effective_threshold = threshold if threshold > 0.0 else settings.fuzzy_dedup_threshold

    run_deduplication(
        by_column       = by,
        fuzzy_fallback  = not no_fuzzy,
        title_threshold = effective_threshold,
        dry_run         = dry_run,
    )


@app.command(name="deduplication-status")
def dedup_status() -> None:
    """Štatistiky deduplikácie."""
    from src.quality.dedup import print_dedup_status
    print_dedup_status()


# ═══════════════════════════════════════════════════════════════════════
# NORMALIZÁCIA PUBLISHER / RELATION.ISPARTOF
# ═══════════════════════════════════════════════════════════════════════

# Deprecated setup alias: setup-processing-queue now prepares journal normalization columns.
def journals_setup() -> None:
    """
    Pridá journal_norm_* stĺpce do lokálnej tabuľky.

    Spusti raz pred prvým spracovaním. Bezpečné spustiť opakovane.
    """
    from src.journals.normalizer import setup_journal_columns
    setup_journal_columns()


@app.command(name="normalize-journals")
def journals_lookup(
    limit:     int  = typer.Option(0,     "--limit",     help="Max počet ISSN/ISBN skupín (0 = všetky)."),
    reprocess: bool = typer.Option(False, "--reprocess", help="Spracovať aj záznamy so statusom no_change/has_proposal."),
) -> None:
    """
    Lookupuje Crossref/OpenAlex (ISSN) a Google Books/OpenLibrary (ISBN),
    ukladá návrhy normalizácie publisher a relation.ispartof.

    Príklady:
      python -m src.cli normalize-journals
      python -m src.cli normalize-journals --limit 20
      python -m src.cli normalize-journals --reprocess
    """
    from src.journals.normalizer import run_journal_lookup
    run_journal_lookup(limit=limit, reprocess=reprocess)


@app.command(name="apply-journal-normalization")
def journals_apply(
    preview:     bool           = typer.Option(False, "--preview",     help="Zobraziť diff bez zápisu."),
    interactive: bool           = typer.Option(False, "--interactive", help="Potvrdzovať každú ISSN skupinu zvlášť (y/n)."),
    limit:       int            = typer.Option(0,     "--limit",       help="Max počet záznamov (0 = všetky)."),
    issn:        str | None     = typer.Option(None,  "--issn",        help="Spracovať len konkrétnu ISSN/ISBN skupinu."),
) -> None:
    """
    Zobrazí navrhnuté zmeny publisher/ispartof a aplikuje po schválení knihovníkom.

    Módy:
      --preview      : farebný diff, žiadne zmeny
      --interactive  : pre každú ISSN skupinu zvlášť y/n
      (bez flagu)    : zobraziť všetko, jedno spoločné potvrdenie

    Príklady:
      python -m src.cli apply-journal-normalization --preview
      python -m src.cli apply-journal-normalization --interactive
      python -m src.cli apply-journal-normalization --issn 0002-9726 --interactive
      python -m src.cli apply-journal-normalization
    """
    from src.journals.normalizer import run_journal_apply
    run_journal_apply(preview=preview, interactive=interactive, limit=limit, issn_filter=issn)


@app.command(name="journal-normalization-status")
def journals_status() -> None:
    """Štatistiky normalizácie publisher / relation.ispartof."""
    from src.journals.normalizer import print_journal_status
    print_journal_status()


# ═══════════════════════════════════════════════════════════════════════
# VSTUPNÝ BOD
# ═══════════════════════════════════════════════════════════════════════

def main() -> None:
    app()

if __name__ == "__main__":
    main()
