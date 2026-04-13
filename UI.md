# UTB Metadata Review - Webove rozhranie

Aktualizovane: 2026-04-13

Tento dokument popisuje aktualny stav Flask UI pre kontrolu metadat, detail zaznamu a spustanie pipeline.

## Spustenie

```bash
uv run flask --app web run --debug
```

Predvolena adresa je `http://127.0.0.1:5000`.

## Navigacia

- `/` - zoznam zaznamov na kontrolu a sekcia zmien cakajucich na schvalenie.
- `/record/<resource_id>` - detail zaznamu, editacia poli, navrhy oprav, Crossref stlpec, panel autorov.
- `/pipeline` - graficke spustanie a planovanie CLI pipeline.

## Zoznam zaznamov

Domovska stranka zobrazuje neskontrolovane zaznamy, vyhladavanie, triedenie a sekciu "Zmeny cakajuce na schvalenie". Pending sekcia cita zaznamy, ktore boli ulozene do zasobnika a este neboli schvalene knihovnikom.

## Detail zaznamu

Detail ma lavu cast s vyhladavanim internych autorov a hlavnu tabulku metadat. Repozitarovy stlpec je editovatelny; WoS a Scopus stlpce su tiez editovatelne pre rucne porovnanie a korekcie. Crossref stlpec sa doplna asynchronne cez `/api/crossref/<resource_id>`.

Aktualne spravanie:

- Navrhy v stlpci Repozitar maju prioritu: LLM vysledky (`author_llm_result`, `date_llm_result`) -> validacne navrhy -> heuristicke/queue fallback hodnoty.
- Ulozenie do zasobnika funguje cez tlacidlo aj cez klavesovu skratku `Ctrl+S` na Linuxe/Windows a `Cmd+S` na macOS.
- `utb.faculty` a `utb.ou` pouzivaju nativny select picker, aby neboli orezane overflow pravidlami tabulky.
- Author panel ma scrollbar odsadeny od trojbodkoveho menu autora.
- Scrollbary v projekte su sirsie a pouzivaju `scrollbar-gutter: stable`, aby menej zasahovali do obsahu.

## Crossref

Detail UI pouziva DOI-level Crossref Works endpoint:

```text
https://api.crossref.org/works/{doi}
```

Pre mapovanie do hlavnej tabulky sa pouzivaju najma `title`, `author`, `publisher`, `container-title`, `published-print`/`published-online`/`published`/`issued`, `volume`, `issue`, `page`, `ISSN`, `DOI`, `URL`, `type`, `abstract` a `language`. Extra sekcia doplna napriklad funding, licencie, linky, ISSN typ, referencie a pocet citovani.

## Pipeline stranka

Pipeline UI je generovane z `web/blueprints/pipeline/catalog.py` a backend obsluhuje `web/blueprints/pipeline/routes.py`.

Stranka obsahuje:

- detailny popis procesu pipeline,
- logicke sekcie prikazov: Inicializacia, Validacia, Autori, Datumy, Zurnaly, Deduplikacia,
- riadok pre kazdy CLI prikaz zo `src/cli/__main__.py`,
- checkbox pre oznacenie prikazu,
- nazov prikazu a ikonku napovedy,
- volitelne argumenty s napovedou ako checkboxy, textove polia alebo selecty,
- tlacidlo `Spustit teraz` pri kazdom prikaze,
- hromadne spustenie oznacenych prikazov,
- planovanie oznacenych prikazov na konkretny cas,
- zoznam planovanych/spustenych behov navrchu stranky s moznostou odstranenia,
- terminalovy vystup streamovany cez SSE.

Planovane behy sa ukladaju do:

```text
data/pipeline_schedules.json
```

Backend spusta background scheduler thread, ktory priebezne kontroluje, ci uz nastal cas niektoreho naplanovaneho behu. Beh potom spusti sekvencne prikazy, ulozi vystup a nastavi stav na `done` alebo `error`.

Bezpecnost:

- Backend akceptuje len prikazy a flagy z katalogu.
- Prikazy sa spustaju ako `python -m src.cli <command>`.
- `bootstrap --drop` a `journals-apply` dostavaju automaticke stdin potvrdenie, aby sa web beh nezasekol na CLI promptoch.

## JavaScript

Hlavny subor detailu je `web/static/js/detail.js`.

Dolezite funkcie:

- `initPanel()` - rozbalenie/zbalenie author panelu.
- `initColumnResize()` - resize stlpcov.
- `initColumnDrag()` - drag-and-drop preusporiadanie stlpcov.
- `initRepozitarCells()` - editacia repozitarovych buniek.
- `initSourceCells()` - editacia WoS/Scopus buniek.
- `initSaveShortcut()` - `Ctrl+S`/`Cmd+S` ulozenie do zasobnika.
- `renderRepozitarCell()` - vykreslenie diffu, navrhu a upraveneho stavu.
- `window.acceptFix(td)` - prijatie navrhu do zasobnika zmien.
- `window.saveAllChanges()` - POST na `/record/<id>/save-fields`.
- `loadCrossref()` - nacitanie Crossref Works dat.

## Backend moduly

```text
web/
  blueprints/
    records/routes.py      - zoznam, detail, save-fields, approve
    api/authors.py         - GET/POST/DELETE /api/authors
    api/crossref.py        - GET /api/crossref/<resource_id>
    pipeline/routes.py     - GET /pipeline, POST /pipeline/run, schedules API
  services/
    records_service.py
    queue_service.py       - zostavenie detailu a priorita navrhov
    authors_service.py
    crossref_service.py    - Crossref Works klient
```
