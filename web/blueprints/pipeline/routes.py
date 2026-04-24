"""Pipeline settings page - runs whitelisted CLI commands from the web UI."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from flask import Response, render_template, request, stream_with_context

from web.blueprints.pipeline import bp
from web.blueprints.pipeline.catalog import (
    EXTERNAL_SCHEDULER_GUIDE,
    PIPELINE_SECTIONS,
    PIPELINE_STEPS,
    allowed_flags,
    command_map,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]

COMMANDS = command_map()
ALLOWED_COMMANDS: set[str] = set(COMMANDS)
ALLOWED_FLAGS: set[str] = allowed_flags()


def _option_map(command: str) -> dict[str, dict]:
    return {option["name"]: option for option in COMMANDS[command]["options"]}


def _coerce_option(option: dict, raw_value: object) -> object | None:
    if raw_value is None:
        return None

    option_type = option["type"]
    if option_type == "bool":
        if isinstance(raw_value, bool):
            return True if raw_value else None
        return True if str(raw_value).lower() in {"1", "true", "on", "yes"} else None

    value = str(raw_value).strip()
    if value == "":
        return None

    if option_type == "int":
        return int(value)
    if option_type == "float":
        return float(value)
    if option_type == "select":
        allowed = {choice[0] for choice in option.get("choices", ())}
        if value not in allowed:
            raise ValueError(f"Nepovolená hodnota pre {option['label']}: {value}")
    return value


def _normalize_args(command: str, raw_args: dict | None) -> dict[str, object]:
    """Return whitelisted CLI args in the form {--flag: value}."""
    if command not in ALLOWED_COMMANDS:
        raise ValueError(f"Nepovolený príkaz: {command}")

    raw_args = raw_args or {}
    normalized: dict[str, object] = {}
    for name, option in _option_map(command).items():
        if name not in raw_args:
            continue
        value = _coerce_option(option, raw_args.get(name))
        if value is None:
            continue
        flag = option["flag"]
        if flag in ALLOWED_FLAGS:
            normalized[flag] = value
    return normalized


def _build_cmd(command: str, args: dict[str, object]) -> list[str]:
    cmd = [sys.executable, "-m", "src.cli", command]
    for flag, value in args.items():
        if flag not in ALLOWED_FLAGS:
            continue
        if value is True:
            cmd.append(flag)
        else:
            cmd.extend([flag, str(value)])
    return cmd


def _stdin_for_command(command: str, args: dict[str, object]) -> str | None:
    """Automatic confirmations for CLI commands that would otherwise block the web run."""
    if command == "bootstrap-local-db" and args.get("--drop") is True:
        return "y\n"
    if command == "apply-journal-normalization" and not args.get("--preview"):
        return "y\n" * (1000 if args.get("--interactive") else 1)
    return None


def _iter_sse_for_command(command: str, args: dict[str, object]):
    cmd = _build_cmd(command, args)
    yield f"data: Spúšťam: {' '.join(cmd)}\n\n"
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        stdin_text = _stdin_for_command(command, args)
        if stdin_text and proc.stdin:
            try:
                proc.stdin.write(stdin_text)
                proc.stdin.close()
            except BrokenPipeError:
                pass
        elif proc.stdin:
            proc.stdin.close()

        assert proc.stdout is not None
        for line in proc.stdout:
            yield f"data: {line.rstrip()}\n\n"
        proc.wait()
        yield "data: \n\n"
        if proc.returncode == 0:
            yield "data: [HOTOVO] Príkaz dokončený úspešne.\n\n"
        else:
            yield f"data: [CHYBA] Príkaz skončil s kódom {proc.returncode}.\n\n"
    except Exception as exc:
        yield f"data: [VÝNIMKA] {exc}\n\n"


@bp.route("/settings/pipeline")
def index():
    return render_template(
        "pipeline/index.html",
        pipeline_steps=PIPELINE_STEPS,
        pipeline_sections=PIPELINE_SECTIONS,
        scheduler_guide=EXTERNAL_SCHEDULER_GUIDE,
    )


@bp.route("/settings/pipeline/run", methods=["POST"])
def run_command():
    """SSE endpoint - runs a CLI command or a command sequence and streams output."""
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        command_items = payload.get("commands", [])
    else:
        command = request.form.get("command", "").strip()
        args = {key: value for key, value in request.form.items() if key != "command"}
        command_items = [{"command": command, "args": args}]

    try:
        normalized_items = [
            {
                "command": str(item.get("command", "")).strip(),
                "args": _normalize_args(str(item.get("command", "")).strip(), item.get("args", {})),
            }
            for item in command_items
        ]
    except (TypeError, ValueError) as exc:
        return Response(f"data: [CHYBA] {exc}\n\nevent: done\ndata: done\n\n", mimetype="text/event-stream")

    def generate():
        for item in normalized_items:
            yield from _iter_sse_for_command(item["command"], item["args"])
        yield "event: done\ndata: done\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
