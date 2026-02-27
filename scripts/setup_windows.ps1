# scripts/setup_windows.ps1
# -------------------------------------------------------
# Pomocný skript pre Windows (PowerShell).
# Vytvorí virtuálne prostredie, nainštaluje závislosti
# a pripraví .env súbor.
#
# Spustenie (z koreňa projektu):
#   powershell -ExecutionPolicy Bypass -File scripts\setup_windows.ps1
# -------------------------------------------------------

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot

Write-Host "=== UTB Metadata Pipeline - Setup ===" -ForegroundColor Cyan
Write-Host "Projektový adresár: $ProjectRoot"
Set-Location $ProjectRoot

# 1. Skontroluj Python
Write-Host "`n[1/5] Kontrolujem Python..." -ForegroundColor Yellow
$pythonVersion = python --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "Python nie je nainštalovaný alebo nie je v PATH."
    exit 1
}
Write-Host "      $pythonVersion" -ForegroundColor Green

# 2. Vytvor virtuálne prostredie
Write-Host "`n[2/5] Vytváram virtuálne prostredie .venv..." -ForegroundColor Yellow
if (Test-Path ".venv") {
    Write-Host "      .venv už existuje, preskakujem."
} else {
    python -m venv .venv
    Write-Host "      .venv vytvorené." -ForegroundColor Green
}

# 3. Nainštaluj závislosti
Write-Host "`n[3/5] Inštalujem závislosti z requirements.txt..." -ForegroundColor Yellow
& ".venv\Scripts\python.exe" -m pip install --upgrade pip --quiet
& ".venv\Scripts\pip.exe" install -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    Write-Error "Inštalácia závislostí zlyhala."
    exit 1
}
Write-Host "      Závislosti nainštalované." -ForegroundColor Green

# 4. Priprav .env súbor
Write-Host "`n[4/5] Pripravujem .env súbor..." -ForegroundColor Yellow
if (Test-Path ".env") {
    Write-Host "      .env už existuje, preskakujem. Skontroluj nastavenia ručne." -ForegroundColor Yellow
} else {
    Copy-Item ".env.example" ".env"
    Write-Host "      .env bol vytvorený z .env.example." -ForegroundColor Green
    Write-Host "      UPOZORNENIE: Uprav .env a nastav správne prihlasovacie údaje!" -ForegroundColor Red
}

# 5. Spusti testy (bez DB testov)
Write-Host "`n[5/5] Spúšťam unit testy (bez DB)..." -ForegroundColor Yellow
& ".venv\Scripts\python.exe" -m pytest tests/ -v --ignore=tests/test_db.py 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "      Niektoré testy zlyhali - skontroluj výstup vyššie." -ForegroundColor Red
} else {
    Write-Host "      Všetky testy prešli." -ForegroundColor Green
}

Write-Host "`n=== Setup dokončený ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Ďalšie kroky:" -ForegroundColor White
Write-Host "  1. Uprav .env a nastav DB prihlasovacie údaje"
Write-Host "  2. Aktivuj venv: .venv\Scripts\activate"
Write-Host "  3. Spusti bootstrap: python -m src.cli bootstrap"
Write-Host "  4. Importuj autorov: python -m src.cli import-authors --csv autori_utb_oficial_utf8.csv"
Write-Host "  5. Spusti heuristiky: python -m src.cli heuristics"
Write-Host ""
