"""Pipeline stránka – spúšťanie CLI príkazov cez webové rozhranie."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from flask import Response, render_template, request, stream_with_context

from web.blueprints.pipeline import bp

# Koreňový adresár projektu (dva levely nad web/blueprints/pipeline/)
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Povolené príkazy (bezpečnostná whitelist)
ALLOWED_COMMANDS: set[str] = {
    "bootstrap",
    "import-authors",
    "queue-setup",
    "validate-setup",
    "validate",
    "apply-fixes",
    "validate-status",
    "heuristics",
    "heuristics-llm",
    "heuristics-compare",
    "heuristics-status",
    "dates-setup",
    "dates",
    "dates-llm",
    "dates-status",
    "dedup-setup",
    "deduplicate",
    "dedup-status",
    "journals-setup",
    "journals-lookup",
    "journals-status",
}

# Povolené prepínače (whitelist)
ALLOWED_FLAGS: set[str] = {
    "--drop", "--limit", "--batch-size", "--revalidate", "--preview", "--dry-run",
    "--reprocess", "--reprocess-errors", "--normalize", "--provider",
    "--no-fuzzy", "--threshold", "--by", "--include-dash",
}


def _build_cmd(command: str, args: dict) -> list[str]:
    """Zostaví príkazový riadok pre subprocess."""
    cmd = [sys.executable, "-m", "src.cli", command]
    for flag, value in args.items():
        if flag not in ALLOWED_FLAGS:
            continue
        if value in ("", None):
            continue
        if value is True or value == "on":
            cmd.append(flag)
        else:
            cmd.extend([flag, str(value)])
    return cmd


@bp.route("/pipeline")
def index():
    return render_template("pipeline/index.html")


@bp.route("/pipeline/run", methods=["POST"])
def run_command():
    """SSE endpoint – spustí CLI príkaz a streamuje výstup."""
    command = request.form.get("command", "").strip()
    if command not in ALLOWED_COMMANDS:
        return Response("data: [CHYBA] Nepovolený príkaz\n\n", mimetype="text/event-stream")

    # Zbierame len povolené prepínače z formulára
    extra_args = {}
    for flag in ALLOWED_FLAGS:
        key = flag.lstrip("-").replace("-", "_")
        val = request.form.get(key)
        if val is not None:
            extra_args[flag] = val

    cmd = _build_cmd(command, extra_args)

    def generate():
        yield f"data: Spúšťam: {' '.join(cmd)}\n\n"
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            for line in proc.stdout:
                line = line.rstrip("\n")
                yield f"data: {line}\n\n"
            proc.wait()
            rc = proc.returncode
            yield f"data: \n\n"
            if rc == 0:
                yield "data: [HOTOVO] Príkaz dokončený úspešne.\n\n"
            else:
                yield f"data: [CHYBA] Príkaz skončil s kódom {rc}.\n\n"
        except Exception as exc:
            yield f"data: [VÝNIMKA] {exc}\n\n"
        yield "event: done\ndata: done\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
