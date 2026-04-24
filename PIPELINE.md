# Pipeline - aktualny tok dat a funkcii

Aktualizovane: 2026-04-20

Tento dokument popisuje aktualny stav pipeline po presune spustania do nastaveni a po odstraneni interneho casovaca.

## Aktualny Stav

- UI pre pipeline je na `/settings/pipeline`.
- Backend route pre spustanie prikazov je `POST /settings/pipeline/run`.
- Interny scheduler bol odstraneny. Neexistuje schedule API ani `data/pipeline_schedules.json`.
- Planovanie sa riesi mimo aplikacie cez Windows Task Scheduler alebo Linux cron.
- `setup-processing-queue` je jediny kanonicky setup pracovnej queue.
- Zastarane CLI aliasy uz nie su zaregistrovane v Typer CLI.
- Lokalne CSV importovanie internych autorov nie je sucastou spustanej pipeline.
- Detekcia internych autorov pouziva aktualny author registry z DB logiky projektu, nie samostatny webovy CSV import.
- LLM navrhy maju v detaile zaznamu prednost pred heuristickymi navrhmi.

## Aktualne CLI Prikazy

| Faza | Prikaz | Hlavny modul/funkcia |
|------|--------|----------------------|
| Bootstrap | `bootstrap-local-db` | `src.db.setup.run_bootstrap()` |
| Queue setup | `setup-processing-queue` | `src.db.setup.setup_processing_queue()` |
| Validacia | `validate-metadata` | `src.quality.checks.run_validation()` |
| Validacne opravy | `apply-validation-fixes` | `src.quality.checks.run_apply_fixes()` |
| Validacny status | `metadata-validation-status` | `src.quality.checks.print_validation_status()` |
| Autori | `detect-authors` | `src.authors.heuristics.run_heuristics()` |
| Autori LLM | `detect-authors-llm` | `src.llm.tasks.authors.run_llm()` |
| Porovnanie autorov | `compare-author-detection` | `src.authors.heuristics.compare_with_librarian()` |
| Status autorov | `author-detection-status` | query nad `utb_processing_queue` |
| Datumy | `extract-dates` | `src.dates.heuristics.run_date_heuristics()` |
| Datumy LLM | `extract-dates-llm` | `src.llm.tasks.dates.run_date_llm()` |
| Status datumov | `date-extraction-status` | `src.dates.heuristics.print_date_status()` |
| Zurnaly | `normalize-journals` | `src.journals.normalizer.run_journal_lookup()` |
| Aplikovanie zurnalov | `apply-journal-normalization` | `src.journals.normalizer.run_journal_apply()` |
| Status zurnalov | `journal-normalization-status` | `src.journals.normalizer.print_journal_status()` |
| Dedup setup | `setup-dedup-history` | `src.quality.dedup.setup_dedup_table()` |
| Deduplikacia | `deduplicate-records` | `src.quality.dedup.run_deduplication()` |
| Status deduplikacie | `deduplication-status` | `src.quality.dedup.print_dedup_status()` |

## Odporucane Poradie

```text
Remote PostgreSQL DB
  |
  v
[1] bootstrap-local-db
  |
  v
[2] setup-processing-queue
  |
  v
[3] setup-dedup-history
  |
  v
[4] validate-metadata
  |
  +--> [4b] apply-validation-fixes
  |
  v
[5] detect-authors
  |
  +--> [5b] detect-authors-llm
  |
  v
[6] extract-dates
  |
  +--> [6b] extract-dates-llm
  |
  v
[7] normalize-journals
  |
  +--> [7b] apply-journal-normalization
  |
  v
[8] deduplicate-records
  |
  v
Detail UI / zasobnik / knihovnicke schvalenie
```

## Faza 1: Bootstrap

`bootstrap-local-db` kopiruje remote metadata tabulku do lokalnej DB.

Volitelny flag:

- `--drop` zmaze lokalnu tabulku a vytvori ju znova.

Kvoli promptu na potvrdenie posiela web runner pri `--drop` automaticky `y`.

## Faza 2: Queue Setup

`setup-processing-queue` vytvori alebo zosuladi `utb_processing_queue`.

Queue obsahuje:

- validacne stavy a navrhy,
- author heuristic/LLM stavy,
- date heuristic/LLM stavy,
- journal normalization stavy,
- zasobnikove workflow stlpce,
- pomocne stlpce pre UI.

Samostatne setup prikazy pre validaciu, datumy a zurnaly uz nie su CLI prikazy.

## Faza 3: Validacia

`validate-metadata` kontroluje kvalitu metadat a uklada navrhy oprav do `validation_suggested_fixes`.

Kontroly zahrnaju:

- whitespace a non-breaking spaces,
- dvojite medzery,
- mojibake a encoding problemy,
- standalone diakritiku,
- DOI format a DOI URL prefixy,
- query parametre v URL,
- znak `|` v hodnote,
- hranate zatvorky v `dc.title`,
- format WoS identifikatora,
- existenciu OBDID v remote DB.

`apply-validation-fixes` cita `validation_suggested_fixes`, vie zobrazit diff cez `--preview` a bez preview zapisuje navrhnute hodnoty do hlavnej tabulky.

## Faza 4: Autori

`detect-authors` spracuje autorov z WoS, Scopus a repozitarovych hodnot a navrhne:

- `author_dc_names`,
- `author_internal_names`,
- `author_faculty`,
- `author_ou`,
- `author_flags`,
- `author_needs_llm`.

Dolezite pravidla:

- Pri zlucenych zaznamoch ma repozitarovy mix autorov sluzit ako zdroj pre vyber internych autorov.
- Jeden autor patri na jeden riadok v UI.
- Fakulty a ustavy sa mozu opakovat, aby poradie sedelo s poradim internych autorov.
- Deduplikacne flagy v `author_flags['duplicates']` sa pri opakovanom behu autorov nemaju mazat.
- Nejednoznacne iniciály alebo priezviska sa radsej flaguju ako neisto uhadnu.

`detect-authors-llm` sa pouzije az po heuristike pre zaznamy, kde `author_needs_llm=TRUE`.

`compare-author-detection` porovna programove navrhy s knihovnickymi hodnotami.

## Faza 5: Datumy

`extract-dates` parsuje `utb.fulltext.dates` do:

- `utb_date_received`,
- `utb_date_reviewed`,
- `utb_date_accepted`,
- `utb_date_published_online`,
- `utb_date_published`,
- `utb_date_extra`,
- `date_flags`.

Pravidla:

- `Reviewed`, `Revised`, `Resubmitted`, `1st/2nd/3rd Revision` a `Prepracovano` patria do `utb_date_reviewed`.
- Nejednoznacne DMY/MDY datumy riesi MDR resolver.
- Nizka istota alebo konflikt ide do `date_needs_llm=TRUE`.

`extract-dates-llm` doplna datumy pre nejednoznacne zaznamy a zapisuje `date_llm_result`.

## Faza 6: Zurnaly A Publisher

`normalize-journals` navrhuje kanonicke hodnoty pre:

- `dc.publisher`,
- `dc.relation.ispartof`.

Priorita zdrojov:

1. Crossref Works podla DOI: `https://api.crossref.org/works/{doi}`
2. ISSN zdroje: Crossref Journals a OpenAlex
3. ISBN zdroje: Google Books a OpenLibrary
4. Existujuce hodnoty v datasetoch, preferovane Scopus, potom WoS, potom najcastejsia hodnota

`apply-journal-normalization` zobrazi diff a aplikuje schvalene navrhy. Web runner mu pri ne-preview behu posiela automaticke potvrdenie.

## Faza 7: Deduplikacia

`setup-dedup-history` vytvara `dedup_histoire`.

`deduplicate-records` pouziva:

- presnu zhodu zvoleneho stlpca, default DOI,
- obsahovu zhodu title + authors + abstract,
- fuzzy zhodu titulu s rokom a ISSN/ISBN kontextom.

Typy:

- `exact:<column>` fyzicky zlucuje,
- `early_access` fyzicky zlucuje,
- `merged_type` fyzicky zlucuje,
- `autoplagiat` iba flaguje,
- `fuzzy_title` iba flaguje.

Pred fyzickym zlucenim sa zaznamy kopiruju do `dedup_histoire`.

## Detail UI A Zasobnik

Detail page zobrazuje Repozitar, WoS, Scopus a Crossref stlpce. Repozitarovy stlpec sklada aktualny navrh v priorite:

1. LLM navrh,
2. validacny navrh,
3. heuristicky navrh,
4. existujuca hodnota.

Zmeny sa ukladaju do zasobnika, aby knihovnik pred finalnym schvalenim videl, co bolo upravene.

## Externe Planovanie

Windows Task Scheduler:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "cd C:\Users\jozef\School\diplomovka\git_repo\verzia4; uv run python -m src.cli detect-authors"
```

Linux cron:

```cron
0 2 * * * cd /path/to/verzia4 && uv run python -m src.cli detect-authors >> logs/pipeline.log 2>&1
```

Prikaz nahraď konkretnym krokom pipeline.

## Overenie

```bash
uv run python -m src.cli --help
uv run python -m compileall web src scripts -q
uv run python -m pytest
```
