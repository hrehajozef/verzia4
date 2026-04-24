# UTB Metadata Review - Web UI

Aktualizovane: 2026-04-20

## Spustenie

```bash
uv run python app.py
```

Predvolena adresa je `http://127.0.0.1:5000`.

## Navigacia

- `/` - zoznam zaznamov na kontrolu, vyhladavanie, triedenie, grupovanie duplicity a zmeny cakajuce na schvalenie.
- `/record/<resource_id>` - detail zaznamu, editacia poli, navrhy oprav, Crossref stlpec a panel autorov.
- `/settings/pipeline` - pipeline prikazy v nastaveniach.
- `/settings/row-order` - nastavenie poradia riadkov na detaile.

Samostatna navigacna polozka `Pipeline` bola odstranena. Pipeline je sucast nastaveni.

## Detail zaznamu

Detail ma lavu cast s vyhladavanim autorov a hlavnu tabulku metadat. Stlpce Repozitar, WoS a Scopus su editovatelne; Crossref sa nacitava asynchronne cez `/api/crossref/<resource_id>`.

Aktualne spravanie:

- Repozitarovy stlpec preferuje LLM navrhy pred validacnymi a heuristickymi fallbackmi.
- `dc.contributor.author`, fakulty a ustavy sa zobrazia po jednej hodnote na riadok.
- Vnutorny zapis viacerych hodnot pouziva oddelovac `||` bez medzier.
- Ulozenie do zasobnika funguje tlacidlom aj skratkou `Ctrl+S` na Windows/Linux a `Cmd+S` na macOS.
- Scrollbar v paneli autorov ma odsadenie od tlacidla s troma bodkami.
- Scrollbary v projekte su sirsie a pouzivaju stabilny gutter.
- Poradie riadkov detailu sa da menit v UI nastaveniach.

## Pipeline V Nastaveniach

Pipeline UI je generovane z `web/blueprints/pipeline/catalog.py` a backend obsluhuje `web/blueprints/pipeline/routes.py`.

Stranka obsahuje:

- detailny popis procesu pipeline,
- logicke sekcie prikazov,
- riadok pre kazdy povoleny CLI prikaz,
- checkbox pre vyber prikazu,
- vysvetlivky k prikazom a k volitelnym flagom,
- textove, ciselne, select a boolean vstupy podla typu option,
- tlacidlo `Spustit teraz` pri kazdom prikaze,
- hromadne spustenie oznacenych prikazov,
- terminalovy vystup streamovany cez SSE,
- navod pre Windows Task Scheduler a Linux cron.

Co tam uz nie je:

- interny casovac,
- datetime input na planovanie,
- zoznam naplanovanych behov,
- schedule API,
- `data/pipeline_schedules.json`,
- background scheduler thread.

Bezpecnost:

- Backend akceptuje len prikazy a flagy z katalogu.
- Prikazy sa spustaju ako `python -m src.cli <command>`.
- `bootstrap-local-db --drop` a `apply-journal-normalization` dostavaju automaticke stdin potvrdenie, aby sa web beh nezasekol na CLI promptoch.

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

## Backend Moduly

```text
web/
  blueprints/
    records/routes.py      - zoznam, detail, save-fields, approve
    api/authors.py         - API pre author search/picker
    api/crossref.py        - GET /api/crossref/<resource_id>
    pipeline/routes.py     - GET /settings/pipeline, POST /settings/pipeline/run
  services/
    records_service.py
    queue_service.py       - zostavenie detailu a priorita navrhov
    authors_service.py
    crossref_service.py    - Crossref Works klient
```
