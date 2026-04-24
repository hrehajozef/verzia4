"""Catalog of CLI commands exposed on the Pipeline settings page."""

from __future__ import annotations


PIPELINE_STEPS: tuple[dict, ...] = (
    {
        "title": "1. Inicializácia dát",
        "body": "Skopíruje sa remote tabuľka do lokálnej DB a pripraví sa pracovný queue, do ktorého zapisuje pipeline aj web.",
    },
    {
        "title": "2. Validácia metadát",
        "body": "Skontrolujú sa formátovacie chyby, DOI/URL problémy, encoding a ďalšie pravidlá. Návrhy opráv sa dajú aplikovať samostatným krokom.",
    },
    {
        "title": "3. Autori a afiliácie",
        "body": "Detegujú sa interní UTB autori, fakulty a ústavy. Neisté prípady môže po heuristike doriešiť LLM fallback.",
    },
    {
        "title": "4. Dátumy",
        "body": "Z fulltextových dátumov sa extrahujú received/reviewed/accepted/published hodnoty. LLM fallback rieši nejednoznačné prípady.",
    },
    {
        "title": "5. Žurnály a vydavatelia",
        "body": "Dopĺňa sa kanonický publisher a journal cez DOI/ISSN/ISBN zdroje a existujúce záznamy.",
    },
    {
        "title": "6. Deduplikácia",
        "body": "Presné, obsahové a fuzzy zhody sa buď zlúčia s históriou, alebo iba označia na kontrolu.",
    },
)


EXTERNAL_SCHEDULER_GUIDE: tuple[dict, ...] = (
    {
        "title": "Windows Task Scheduler",
        "body": "Vytvor úlohu, ktorá spustí PowerShell v koreni projektu. Príkaz uprav podľa kroku pipeline.",
        "code": (
            'powershell.exe -NoProfile -ExecutionPolicy Bypass -Command '
            '"cd C:\\Users\\jozef\\School\\diplomovka\\git_repo\\verzia4; '
            'uv run python -m src.cli detect-authors"'
        ),
    },
    {
        "title": "Linux cron",
        "body": "Do crontabu pridaj riadok s časom a príkazom v koreni projektu.",
        "code": (
            "0 2 * * * cd /path/to/verzia4 && "
            "uv run python -m src.cli detect-authors >> logs/pipeline.log 2>&1"
        ),
    },
)


PIPELINE_SECTIONS: tuple[dict, ...] = (
    {
        "title": "Inicializácia",
        "commands": (
            {
                "name": "bootstrap-local-db",
                "description": "Skopíruje remote metadata tabuľku do lokálnej DB.",
                "help": "Prvý krok pipeline. Bez volieb ponechá existujúcu lokálnu tabuľku; s --drop ju zmaže a naplní znova.",
                "badge": "základ",
                "options": (
                    {"name": "drop", "flag": "--drop", "type": "bool", "label": "--drop", "help": "Zmaže existujúcu lokálnu tabuľku a znovu ju naplní z remote DB."},
                ),
            },
            {
                "name": "setup-processing-queue",
                "description": "Pripraví pracovný queue a pomocné tabuľky.",
                "help": "Vytvorí alebo zosúladí utb_processing_queue, stĺpce pre validáciu, autorov, dátumy, žurnály a zásobník zmien.",
                "badge": "raz",
                "options": (),
            },
        ),
    },
    {
        "title": "Validácia",
        "commands": (
            {
                "name": "validate-metadata",
                "description": "Skontroluje kvalitu metadát a uloží návrhy opráv.",
                "help": "Kontroluje whitespace, encoding, DOI, URL, OBDID a ďalšie problémy. Výsledky zapisuje do validation_* stĺpcov.",
                "options": (
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximálny počet záznamov. 0 znamená všetky."},
                    {"name": "batch_size", "flag": "--batch-size", "type": "int", "label": "--batch-size", "default": 500, "help": "Veľkosť dávky pri spracovaní."},
                    {"name": "revalidate", "flag": "--revalidate", "type": "bool", "label": "--revalidate", "help": "Znovu skontroluje aj záznamy, ktoré už majú výsledok validácie."},
                ),
            },
            {
                "name": "apply-validation-fixes",
                "description": "Aplikuje navrhnuté opravy z validácie.",
                "help": "Číta validation_suggested_fixes. Preview alebo dry-run iba vypíše diff, bez nich zapíše navrhnuté hodnoty.",
                "options": (
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximálny počet záznamov. 0 znamená všetky."},
                    {"name": "preview", "flag": "--preview", "type": "bool", "label": "--preview", "help": "Zobrazí farebný diff bez zápisu do DB."},
                    {"name": "dry_run", "flag": "--dry-run", "type": "bool", "label": "--dry-run", "help": "Alias pre preview, bez zápisu do DB."},
                ),
            },
            {"name": "metadata-validation-status", "description": "Vypíše štatistiky validácie.", "help": "Prehľad stavov validácie a problémov čakajúcich na spracovanie.", "badge": "info", "options": ()},
        ),
    },
    {
        "title": "Autori",
        "commands": (
            {
                "name": "detect-authors",
                "description": "Nájde interných autorov a ich afiliácie.",
                "help": "Spracuje WoS afiliačné bloky a autorov, matchuje ich oproti remote OBD registru a dopĺňa faculty/OU hodnoty.",
                "options": (
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximálny počet záznamov. 0 znamená všetky."},
                    {"name": "batch_size", "flag": "--batch-size", "type": "int", "label": "--batch-size", "default": "", "help": "Veľkosť dávky. Prázdne použije hodnotu z konfigurácie."},
                    {"name": "reprocess_errors", "flag": "--reprocess-errors", "type": "bool", "label": "--reprocess-errors", "help": "Spracuje znova záznamy so stavom error."},
                    {"name": "reprocess", "flag": "--reprocess", "type": "bool", "label": "--reprocess", "help": "Spracuje znova aj záznamy so stavom processed."},
                    {"name": "normalize", "flag": "--normalize", "type": "bool", "label": "--normalize", "help": "Použije mená bez diakritiky/lowercase pri fuzzy matchingu."},
                ),
            },
            {
                "name": "detect-authors-llm",
                "description": "LLM fallback pre internych autorov.",
                "help": "Spracuje zaznamy s author_needs_llm=true a doplni autorov, fakulty a ustavy s kontrolou oproti registru.",
                "options": (
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximalny pocet zaznamov. 0 znamena vsetky."},
                    {"name": "batch_size", "flag": "--batch-size", "type": "int", "label": "--batch-size", "default": "", "help": "Velkost davky. Prazdne pouzije hodnotu z konfiguracie."},
                    {"name": "provider", "flag": "--provider", "type": "select", "label": "--provider", "default": "", "choices": (("", "z .env"), ("ollama", "ollama"), ("openai", "openai")), "help": "LLM provider. Prazdne pouzije LLM_PROVIDER z .env."},
                    {"name": "reprocess", "flag": "--reprocess", "type": "bool", "label": "--reprocess", "help": "Spracuje znova aj zaznamy so stavom processed alebo validation_error."},
                ),
            },
            {"name": "compare-author-detection", "description": "Porovna detekciu autorov s knihovnickou hodnotou.", "help": "Porovnava author_internal_names oproti utb.contributor.internalauthor.", "badge": "info", "options": ()},
            {"name": "author-detection-status", "description": "Vypise statistiky detekcie autorov.", "help": "Prehlad author_heuristic_status a author_llm_status.", "badge": "info", "options": ()},
        ),
    },
    {
        "title": "Datumy",
        "commands": (
            {
                "name": "extract-dates",
                "description": "Extrahuje datumy z fulltextovych metadat.",
                "help": "Riesi Received/Reviewed/Revised/Accepted/Published labely a nejednoznacne DMY/MDY datumy.",
                "options": (
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximalny pocet zaznamov. 0 znamena vsetky."},
                    {"name": "batch_size", "flag": "--batch-size", "type": "int", "label": "--batch-size", "default": 200, "help": "Velkost davky pri spracovani."},
                    {"name": "reprocess", "flag": "--reprocess", "type": "bool", "label": "--reprocess", "help": "Spracuje znova zaznamy so stavom error."},
                ),
            },
            {
                "name": "extract-dates-llm",
                "description": "LLM fallback pre nejednoznacne datumy.",
                "help": "Spracuje zaznamy s date_needs_llm=true a doplni datumove stlpce.",
                "options": (
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximalny pocet zaznamov. 0 znamena vsetky."},
                    {"name": "batch_size", "flag": "--batch-size", "type": "int", "label": "--batch-size", "default": "", "help": "Velkost davky. Prazdne pouzije hodnotu z konfiguracie."},
                    {"name": "provider", "flag": "--provider", "type": "select", "label": "--provider", "default": "", "choices": (("", "z .env"), ("ollama", "ollama"), ("openai", "openai")), "help": "LLM provider. Prazdne pouzije LLM_PROVIDER z .env."},
                    {"name": "reprocess", "flag": "--reprocess", "type": "bool", "label": "--reprocess", "help": "Spracuje znova aj chybove LLM zaznamy."},
                    {"name": "include_dash", "flag": "--include-dash", "type": "bool", "label": "--include-dash", "help": "Zahrnie aj zaznamy, kde utb.fulltext.dates obsahuje '{-}'."},
                ),
            },
            {"name": "date-extraction-status", "description": "Vypise statistiky datumov.", "help": "Prehlad date_heuristic_status a date_llm_status.", "badge": "info", "options": ()},
        ),
    },
    {
        "title": "Žurnály",
        "commands": (
            {
                "name": "normalize-journals",
                "description": "Navrhne kanonicky journal a publisher.",
                "help": "Skusa DOI-level Crossref Works, ISSN/ISBN zdroje a existujuce zaznamy.",
                "options": (
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximalny pocet skupin. 0 znamena vsetky."},
                    {"name": "reprocess", "flag": "--reprocess", "type": "bool", "label": "--reprocess", "help": "Spracuje znova aj skupiny so stavom no_change alebo has_proposal."},
                ),
            },
            {
                "name": "apply-journal-normalization",
                "description": "Aplikuje navrhy normalizacie journalu.",
                "help": "Preview iba zobrazí diff. Bez preview zapíše schválené zmeny do dc.publisher a dc.relation.ispartof.",
                "options": (
                    {"name": "preview", "flag": "--preview", "type": "bool", "label": "--preview", "help": "Zobrazi diff bez zapisu do DB."},
                    {"name": "interactive", "flag": "--interactive", "type": "bool", "label": "--interactive", "help": "CLI rezim s potvrdenim kazdej skupiny."},
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximalny pocet zaznamov. 0 znamena vsetky."},
                    {"name": "issn", "flag": "--issn", "type": "text", "label": "--issn", "default": "", "help": "Spracuje iba konkretnu ISSN/ISBN skupinu."},
                ),
            },
            {"name": "journal-normalization-status", "description": "Vypise statistiky normalizacie journalov.", "help": "Prehlad journal_norm_status hodnot.", "badge": "info", "options": ()},
        ),
    },
    {
        "title": "Deduplikacia",
        "commands": (
            {"name": "setup-dedup-history", "description": "Pripravi historiu deduplikacie.", "help": "Vytvori tabulku dedup_histoire pre audit pred fyzickym zlucenim.", "badge": "raz", "options": ()},
            {
                "name": "deduplicate-records",
                "description": "Najde, zluci alebo oznaci duplicity.",
                "help": "Presná zhoda, obsahová zhoda a fuzzy titulová zhoda. Zlúčiteľné záznamy sa kopírujú do histórie.",
                "options": (
                    {
                        "name": "by",
                        "flag": "--by",
                        "type": "select",
                        "label": "--by",
                        "default": "dc.identifier.doi",
                        "choices": (
                            ("dc.identifier.doi", "DOI"),
                            ("utb.identifier.obdid", "OBDID"),
                            ("utb.identifier.wok", "WoS ID"),
                            ("utb.identifier.scopus", "Scopus ID"),
                            ("dc.title", "Názov"),
                        ),
                        "help": "Stlpec pre presnu zhodu pri prvej faze deduplikacie.",
                    },
                    {"name": "threshold", "flag": "--threshold", "type": "float", "label": "--threshold", "default": 0, "help": "Jaro-Winkler prah pre fuzzy titul. 0 pouzije .env hodnotu."},
                    {"name": "no_fuzzy", "flag": "--no-fuzzy", "type": "bool", "label": "--no-fuzzy", "help": "Vypne fuzzy titulovu fallback fazu."},
                    {"name": "dry_run", "flag": "--dry-run", "type": "bool", "label": "--dry-run", "help": "Iba vypise vysledky, bez zapisu do DB."},
                ),
            },
            {"name": "deduplication-status", "description": "Vypise statistiky deduplikacie.", "help": "Prehlad duplicit a historie deduplikacie.", "badge": "info", "options": ()},
        ),
    },
)


def iter_commands():
    for section in PIPELINE_SECTIONS:
        for command in section["commands"]:
            yield command


def command_map() -> dict[str, dict]:
    return {command["name"]: command for command in iter_commands()}


def allowed_flags() -> set[str]:
    flags: set[str] = set()
    for command in iter_commands():
        for option in command["options"]:
            flags.add(option["flag"])
    return flags
