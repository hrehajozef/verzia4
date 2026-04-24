# UTB Metadata Pipeline

Aktualizovane: 2026-04-20

Projekt spracovava a kontroluje metadata vedeckych publikacii UTB. Lokalna kopia dat sa cisti, doplna o navrhy pre autorov, datumy, vydavatelov a zurnaly, zobrazuje v knihovnickom UI a pred finalnym zapisom uchovava zmeny v zasobniku.

## Aktualny stav

- Centralna pracovna tabulka pipeline je `utb_processing_queue`.
- Pipeline sa v UI spusta v nastaveniach: `/settings/pipeline`.
- Samostatna stranka `/pipeline` uz neexistuje.
- Appka nema interny casovac, planovane behy ani `data/pipeline_schedules.json`.
- Opakovane spustanie sa riesi mimo aplikacie cez Windows Task Scheduler alebo Linux cron.
- Zastarane CLI prikazy boli odstranene z katalogu aj z registrovanych Typer prikazov.
- Lokalne CSV nacitavanie internych autorov uz nie je sucastou spustanej pipeline.
- Detail zaznamu dava prednost LLM navrhom; ak nie su dostupne, pouzije validacne alebo heuristicke navrhy.
- Ulozenie do zasobnika funguje cez tlacidlo aj `Ctrl+S` na Windows/Linux a `Cmd+S` na macOS.

## Spustenie webu

```bash
uv sync --all-groups
uv run python app.py
```

Predvolena adresa je `http://127.0.0.1:5000`.

## Hlavne stranky

| URL | Ucel |
|-----|------|
| `/` | Zoznam zaznamov na kontrolu, vyhladavanie, grupovanie duplicity a zmeny cakajuce na schvalenie. |
| `/record/<resource_id>` | Detail zaznamu, porovnanie Repozitar/WoS/Scopus/Crossref, editacia a ulozenie do zasobnika. |
| `/settings/pipeline` | Rucne spustanie CLI krokov pipeline a navod na externy systemovy planovac. |
| `/settings/row-order` | Nastavenie poradia riadkov na detailnej stranke. |

## Aktualne CLI prikazy

| Sekcia | Prikaz | Popis |
|--------|--------|-------|
| Inicializacia | `bootstrap-local-db` | Skopiruje remote metadata tabulku do lokalnej DB. |
| Inicializacia | `setup-processing-queue` | Vytvori alebo zosuladi `utb_processing_queue` a pomocne workflow stlpce. |
| Validacia | `validate-metadata` | Skontroluje metadata a ulozi navrhy oprav do `validation_suggested_fixes`. |
| Validacia | `apply-validation-fixes` | Aplikuje navrhnute validacne opravy do skutocnych stlpcov. |
| Validacia | `metadata-validation-status` | Vypise prehlad validacnych stavov. |
| Autori | `detect-authors` | Detekuje internych UTB autorov, fakulty a ustavy. |
| Autori | `detect-authors-llm` | LLM fallback pre nejasnych autorov. |
| Autori | `compare-author-detection` | Porovna navrhy pipeline s hodnotami od knihovnika. |
| Autori | `author-detection-status` | Vypise stav spracovania autorov. |
| Datumy | `extract-dates` | Parsuje received/reviewed/accepted/published datumy. |
| Datumy | `extract-dates-llm` | LLM fallback pre nejednoznacne datumy. |
| Datumy | `date-extraction-status` | Vypise stav spracovania datumov. |
| Zurnaly | `normalize-journals` | Navrhne kanonicke hodnoty publisher/ispartof cez DOI/ISSN/ISBN zdroje. |
| Zurnaly | `apply-journal-normalization` | Zobrazi diff a aplikuje schvalene zurnalove navrhy. |
| Zurnaly | `journal-normalization-status` | Vypise stav normalizacie zurnalov. |
| Deduplikacia | `setup-dedup-history` | Vytvori historiu deduplikacie. |
| Deduplikacia | `deduplicate-records` | Najde, oznaci alebo fyzicky zluci duplicity. |
| Deduplikacia | `deduplication-status` | Vypise stav deduplikacie. |

## Odporucane poradie pipeline

```bash
uv run python -m src.cli bootstrap-local-db
uv run python -m src.cli setup-processing-queue
uv run python -m src.cli setup-dedup-history
uv run python -m src.cli validate-metadata
uv run python -m src.cli detect-authors
uv run python -m src.cli extract-dates
uv run python -m src.cli normalize-journals
uv run python -m src.cli deduplicate-records
```

Volitelne LLM fallbacky:

```bash
uv run python -m src.cli detect-authors-llm
uv run python -m src.cli extract-dates-llm
```

Full reset skript:

```bash
uv run python scripts/reset_and_run.py
uv run python scripts/reset_and_run.py --llm
```

## Externe planovanie

Appka ulohy neplanuje a nema background scheduler. Planovanie nastav v systeme.

Windows Task Scheduler, akcia `Start a program`:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "cd C:\Users\jozef\School\diplomovka\git_repo\verzia4; uv run python -m src.cli detect-authors"
```

Linux cron:

```cron
0 2 * * * cd /path/to/verzia4 && uv run python -m src.cli detect-authors >> logs/pipeline.log 2>&1
```

Prikaz `detect-authors` v prikladoch nahraď konkretnym krokom pipeline.

## Crossref

Pre DOI sa pouziva Crossref Works endpoint:

```text
https://api.crossref.org/works/{doi}
```

Tento endpoint dava najviac uzitocnych poli pre detail publikacie: titul, autorov, DOI, publisher, journal/container, datumy publikovania, strany, rocnik, cislo, ISSN, typ, URL, funding, licencie a linky. Pre ISSN fallback sa stale pouzivaju zurnalove zdroje.

## Testy

```bash
uv run python -m pytest
uv run python -m compileall web src scripts -q
```

Posledny overeny stav pred tymto refaktorom: `180 passed`.
