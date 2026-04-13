"""Katalog CLI prikazov pre stranku Pipeline."""

from __future__ import annotations


PIPELINE_STEPS: tuple[dict, ...] = (
    {
        "title": "1. Inicializacia dat",
        "body": "Bootstrap skopiruje remote tabulku do lokalnej databazy. Potom sa importuje register internych autorov a vytvori sa medzitabuka utb_processing_queue, do ktorej zapisuje web aj pipeline.",
    },
    {
        "title": "2. Validacia a opravy metadat",
        "body": "Validacia hlada formatovacie chyby, mojibake, DOI/URL problemy a neplatne OBDID. Navrhy oprav sa daju najprv skontrolovat cez preview a potom aplikovat do skutocnych hodnot.",
    },
    {
        "title": "3. Autori a afiliacie",
        "body": "Heuristiky vyhodnotia WoS/DC autorov oproti internemu registru UTB, doplnia fakulty a organizacne jednotky a nejasne pripady oznacia pre LLM fallback.",
    },
    {
        "title": "4. Datumy",
        "body": "Datumova cast parsuje utb_fulltext_dates, riesi nejednoznacne DMY/MDY formaty a problematicke zaznamy posiela do LLM spracovania.",
    },
    {
        "title": "5. Zurnaly a vydavatelia",
        "body": "Journal lookup hlada kanonicke publisher/ispartof hodnoty cez ISSN/ISBN zdroje alebo existujuce zaznamy. Apply krok navrhy zobrazi a po schvaleni zapise.",
    },
    {
        "title": "6. Deduplikacia a kontrolne vystupy",
        "body": "Deduplikacia hlada presne, obsahove a fuzzy zhody. Zluctelne duplicity fyzicky merguje s historiou, rizikove pripady len flaguje na kontrolu.",
    },
)


PIPELINE_SECTIONS: tuple[dict, ...] = (
    {
        "title": "Inicializacia",
        "commands": (
            {
                "name": "bootstrap",
                "description": "Skopiruje remote tabulku do lokalnej DB.",
                "help": "Prvy krok pipeline. Bez volieb je idempotentny; s --drop zmaze lokalnu tabulku a vytvori ju znova.",
                "badge": "zaklad",
                "options": (
                    {
                        "name": "drop",
                        "flag": "--drop",
                        "type": "bool",
                        "label": "--drop",
                        "help": "Zmaze existujucu lokalnu tabulku a znovu ju naplni z remote DB.",
                    },
                ),
            },
            {
                "name": "import-authors",
                "description": "Nahra register internych UTB autorov z CSV.",
                "help": "Vytvori alebo obnovi tabulku utb_internal_authors a importuje mena z CSV suboru.",
                "options": (
                    {
                        "name": "csv",
                        "flag": "--csv",
                        "type": "text",
                        "label": "--csv",
                        "default": "data/autori_utb_oficial_utf8.csv",
                        "help": "Cesta k CSV suboru vo formate priezvisko;krstne_meno.",
                    },
                ),
            },
            {
                "name": "queue-setup",
                "description": "Vytvori medzitabulku utb_processing_queue.",
                "help": "Spusta sa po bootstrape. Je bezpecny na opakovane spustenie a pripravi tabulku, s ktorou pracuje webove UI.",
                "badge": "raz",
                "options": (),
            },
        ),
    },
    {
        "title": "Validacia",
        "commands": (
            {
                "name": "validate-setup",
                "description": "Prida validation_* stlpce.",
                "help": "Jednorazova priprava stlpcov pre validaciu. Prikaz je idempotentny.",
                "badge": "raz",
                "options": (),
            },
            {
                "name": "validate",
                "description": "Skontroluje kvalitu metadat a ulozi navrhy oprav.",
                "help": "Kontroluje whitespace, encoding, DOI, URL, OBDID a dalsie problemy. Vysledky zapisuje do validation_status, validation_flags a validation_suggested_fixes.",
                "options": (
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximalny pocet zaznamov. 0 znamena vsetky."},
                    {"name": "batch_size", "flag": "--batch-size", "type": "int", "label": "--batch-size", "default": 500, "help": "Velkost davky pri spracovani."},
                    {"name": "revalidate", "flag": "--revalidate", "type": "bool", "label": "--revalidate", "help": "Znovu skontroluje aj zaznamy, ktore uz maju vysledok validacie."},
                ),
            },
            {
                "name": "apply-fixes",
                "description": "Aplikuje navrhnute opravy z validacie.",
                "help": "Cita validation_suggested_fixes. Preview alebo dry-run iba vypise diff, bez nich zapise navrhnute hodnoty a oznaci zaznamy na revalidaciu.",
                "options": (
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximalny pocet zaznamov. 0 znamena vsetky."},
                    {"name": "preview", "flag": "--preview", "type": "bool", "label": "--preview", "help": "Zobrazi farebny diff bez zapisu do DB."},
                    {"name": "dry_run", "flag": "--dry-run", "type": "bool", "label": "--dry-run", "help": "Alias pre preview, bez zapisu do DB."},
                ),
            },
            {
                "name": "validate-status",
                "description": "Vypise statistiky validacie.",
                "help": "Prehlad stavov validacie a problemov, ktore este cakaju na spracovanie.",
                "badge": "info",
                "options": (),
            },
        ),
    },
    {
        "title": "Autori",
        "commands": (
            {
                "name": "heuristics",
                "description": "Heuristicky najde internych autorov a afiliacie.",
                "help": "Spracuje WoS afiliacne bloky alebo DC autorov, matchuje ich oproti registru a doplna faculty/OU hodnoty.",
                "options": (
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximalny pocet zaznamov. 0 znamena vsetky."},
                    {"name": "batch_size", "flag": "--batch-size", "type": "int", "label": "--batch-size", "default": "", "help": "Velkost davky. Prazdne pouzije hodnotu z konfiguracie."},
                    {"name": "reprocess_errors", "flag": "--reprocess-errors", "type": "bool", "label": "--reprocess-errors", "help": "Spracuje znova zaznamy so stavom error."},
                    {"name": "reprocess", "flag": "--reprocess", "type": "bool", "label": "--reprocess", "help": "Spracuje znova aj zaznamy so stavom processed."},
                    {"name": "normalize", "flag": "--normalize", "type": "bool", "label": "--normalize", "help": "Pouzije normalizovane mena bez diakritiky a lowercase pri fuzzy matchingu."},
                ),
            },
            {
                "name": "heuristics-llm",
                "description": "LLM fallback pre autorov oznacenych heuristikou.",
                "help": "Spracuje zaznamy s author_needs_llm=true a doplni autorov, fakulty a OU s kontrolou proti registru.",
                "options": (
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximalny pocet zaznamov. 0 znamena vsetky."},
                    {"name": "batch_size", "flag": "--batch-size", "type": "int", "label": "--batch-size", "default": "", "help": "Velkost davky. Prazdne pouzije hodnotu z konfiguracie."},
                    {"name": "provider", "flag": "--provider", "type": "select", "label": "--provider", "default": "", "choices": (("", "z .env"), ("ollama", "ollama"), ("openai", "openai")), "help": "LLM provider. Prazdne pouzije LLM_PROVIDER z .env."},
                ),
            },
            {"name": "heuristics-compare", "description": "Porovna programove vysledky s knihovnikom.", "help": "Porovnava author_internal_names oproti utb.contributor.internalauthor a vypise typy zhody.", "badge": "info", "options": ()},
            {"name": "heuristics-status", "description": "Vypise statistiky autorov a LLM.", "help": "Prehlad stavov author_heuristic_status a author_llm_status.", "badge": "info", "options": ()},
        ),
    },
    {
        "title": "Datumy",
        "commands": (
            {"name": "dates-setup", "description": "Prida datumove a LLM stlpce.", "help": "Jednorazova priprava DATE, date_flags a date_llm_* stlpcov. Je idempotentna.", "badge": "raz", "options": ()},
            {
                "name": "dates",
                "description": "Heuristicky parsuje utb_fulltext_dates.",
                "help": "Riesi Received/Reviewed/Accepted/Published labely a nejednoznacne DMY/MDY bodkove datumy.",
                "options": (
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximalny pocet zaznamov. 0 znamena vsetky."},
                    {"name": "batch_size", "flag": "--batch-size", "type": "int", "label": "--batch-size", "default": 200, "help": "Velkost davky pri spracovani."},
                    {"name": "reprocess", "flag": "--reprocess", "type": "bool", "label": "--reprocess", "help": "Spracuje znova zaznamy so stavom error."},
                ),
            },
            {
                "name": "dates-llm",
                "description": "LLM fallback pre nejednoznacne datumy.",
                "help": "Spracuje zaznamy s date_needs_llm=true a doplni datumove stlpce.",
                "options": (
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximalny pocet zaznamov. 0 znamena vsetky."},
                    {"name": "batch_size", "flag": "--batch-size", "type": "int", "label": "--batch-size", "default": "", "help": "Velkost davky. Prazdne pouzije hodnotu z konfiguracie."},
                    {"name": "provider", "flag": "--provider", "type": "select", "label": "--provider", "default": "", "choices": (("", "z .env"), ("ollama", "ollama"), ("openai", "openai")), "help": "LLM provider. Prazdne pouzije LLM_PROVIDER z .env."},
                    {"name": "reprocess", "flag": "--reprocess", "type": "bool", "label": "--reprocess", "help": "Spracuje znova aj chybove LLM zaznamy."},
                    {"name": "include_dash", "flag": "--include-dash", "type": "bool", "label": "--include-dash", "help": "Zahrnie aj zaznamy, kde utb_fulltext_dates obsahuje '{-}'."},
                ),
            },
            {"name": "dates-status", "description": "Vypise statistiky datumov.", "help": "Prehlad date_heuristic_status a date_llm_status.", "badge": "info", "options": ()},
        ),
    },
    {
        "title": "Zurnaly",
        "commands": (
            {"name": "journals-setup", "description": "Prida journal_norm_* stlpce.", "help": "Jednorazova priprava stlpcov pre normalizaciu publisher/ispartof.", "badge": "raz", "options": ()},
            {
                "name": "journals-lookup",
                "description": "Vyhlada kanonicke publisher/ispartof hodnoty.",
                "help": "Lookupuje ISSN/ISBN cez Crossref, OpenAlex, Google Books, OpenLibrary alebo vyberie kanonicku hodnotu z existujucich zaznamov.",
                "options": (
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximalny pocet ISSN/ISBN skupin. 0 znamena vsetky."},
                    {"name": "reprocess", "flag": "--reprocess", "type": "bool", "label": "--reprocess", "help": "Spracuje znova aj skupiny so stavom no_change alebo has_proposal."},
                ),
            },
            {
                "name": "journals-apply",
                "description": "Zobrazi a aplikuje navrhy normalizacie.",
                "help": "Preview iba zobrazi diff. Bez preview vie zapisat schvalene zmeny do dc.publisher a dc.relation.ispartof.",
                "options": (
                    {"name": "preview", "flag": "--preview", "type": "bool", "label": "--preview", "help": "Zobrazi diff bez zapisu do DB."},
                    {"name": "interactive", "flag": "--interactive", "type": "bool", "label": "--interactive", "help": "CLI rezim s potvrdenim kazdej ISSN skupiny. Vo web UI nie je vhodny na automaticke planovanie."},
                    {"name": "limit", "flag": "--limit", "type": "int", "label": "--limit", "default": 0, "help": "Maximalny pocet zaznamov. 0 znamena vsetky."},
                    {"name": "issn", "flag": "--issn", "type": "text", "label": "--issn", "default": "", "help": "Spracuje iba konkretnu ISSN/ISBN skupinu."},
                ),
            },
            {"name": "journals-status", "description": "Vypise statistiky normalizacie.", "help": "Prehlad journal_norm_status hodnot.", "badge": "info", "options": ()},
        ),
    },
    {
        "title": "Deduplikacia",
        "commands": (
            {"name": "dedup-setup", "description": "Vytvori tabulku dedup_histoire.", "help": "Jednorazova priprava historie pred fyzickym zlucovanim duplicit.", "badge": "raz", "options": ()},
            {
                "name": "deduplicate",
                "description": "Najde, zluci alebo oznaci duplicity.",
                "help": "Kombinuje presnu zhodu, obsahovu zhodu a fuzzy titulovu zhodu. Fyzicke merge operacie kopiruju historiu do dedup_histoire.",
                "options": (
                    {"name": "by", "flag": "--by", "type": "text", "label": "--by", "default": "dc.identifier.doi", "help": "Stlpec pre presnu zhodu, napriklad dc.identifier.doi alebo dc.title."},
                    {"name": "threshold", "flag": "--threshold", "type": "float", "label": "--threshold", "default": 0, "help": "Jaro-Winkler prah pre fuzzy titul. 0 pouzije hodnotu z .env."},
                    {"name": "no_fuzzy", "flag": "--no-fuzzy", "type": "bool", "label": "--no-fuzzy", "help": "Vypne fuzzy titulovu fallback fazu."},
                    {"name": "dry_run", "flag": "--dry-run", "type": "bool", "label": "--dry-run", "help": "Iba vypise vysledky, bez zapisu do DB."},
                ),
            },
            {"name": "dedup-status", "description": "Vypise statistiky deduplikacie.", "help": "Prehlad duplicit a historie deduplikacie.", "badge": "info", "options": ()},
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
