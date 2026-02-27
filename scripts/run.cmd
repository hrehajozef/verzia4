@echo off
REM scripts\run.cmd
REM -------------------------------------------------------
REM Pomocný CMD skript pre spustenie pipeline príkazov.
REM Predpokladá aktivovaný venv alebo použitie .venv priamo.
REM
REM Použitie:
REM   run.cmd bootstrap
REM   run.cmd import-authors
REM   run.cmd heuristics
REM   run.cmd llm
REM   run.cmd status
REM   run.cmd test
REM -------------------------------------------------------

setlocal

REM Cesta k Python v .venv
set PYTHON=.venv\Scripts\python.exe
if not exist %PYTHON% (
    echo CHYBA: .venv\Scripts\python.exe neexistuje.
    echo Spusti najprv: powershell -File scripts\setup_windows.ps1
    exit /b 1
)

if "%1"=="" goto usage
if "%1"=="bootstrap" goto bootstrap
if "%1"=="import-authors" goto import_authors
if "%1"=="heuristics" goto heuristics
if "%1"=="llm" goto llm
if "%1"=="status" goto status
if "%1"=="test" goto test
goto usage

:bootstrap
echo [Spustam] bootstrap...
%PYTHON% -m src.cli bootstrap %2 %3 %4
goto end

:import_authors
echo [Spustam] import-authors...
%PYTHON% -m src.cli import-authors --csv autori_utb_oficial_utf8.csv %2 %3
goto end

:heuristics
echo [Spustam] heuristics...
%PYTHON% -m src.cli heuristics %2 %3 %4 %5
goto end

:llm
echo [Spustam] llm...
%PYTHON% -m src.cli llm %2 %3 %4 %5
goto end

:status
echo [Spustam] status...
%PYTHON% -m src.cli status
goto end

:test
echo [Spustam] pytest...
%PYTHON% -m pytest tests/ -v
goto end

:usage
echo Pouzitie: run.cmd [bootstrap|import-authors|heuristics|llm|status|test]
echo.
echo  bootstrap       - Skopiruje tabulku a prida vystupne stlpce
echo  import-authors  - Importuje internych autorov z CSV
echo  heuristics      - Spusti heuristicke spracovanie
echo  llm             - Spusti LLM fazu
echo  status          - Zobrazi statistiky spracovania
echo  test            - Spusti unit testy
goto end

:end
endlocal
