"""Pipeline stranka - spustanie CLI prikazov cez webove rozhranie."""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from flask import Response, jsonify, render_template, request, stream_with_context

from web.blueprints.pipeline import bp
from web.blueprints.pipeline.catalog import (
    PIPELINE_SECTIONS,
    PIPELINE_STEPS,
    allowed_flags,
    command_map,
)

# Korenovy adresar projektu (dva levely nad web/blueprints/pipeline/)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCHEDULE_FILE = PROJECT_ROOT / "data" / "pipeline_schedules.json"

COMMANDS = command_map()
ALLOWED_COMMANDS: set[str] = set(COMMANDS)
ALLOWED_FLAGS: set[str] = allowed_flags()

_schedule_lock = threading.Lock()
_scheduler_started = False


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
            raise ValueError(f"Nepovolena hodnota pre {option['label']}: {value}")
    return value


def _normalize_args(command: str, raw_args: dict | None) -> dict[str, object]:
    """Vrati whitelistovane argumenty vo formate {--flag: value}."""
    if command not in ALLOWED_COMMANDS:
        raise ValueError(f"Nepovoleny prikaz: {command}")

    raw_args = raw_args or {}
    normalized: dict[str, object] = {}
    for name, option in _option_map(command).items():
        if name not in raw_args:
            continue
        value = _coerce_option(option, raw_args.get(name))
        if value is None:
            continue
        flag = option["flag"]
        if flag not in ALLOWED_FLAGS:
            continue
        normalized[flag] = value
    return normalized


def _build_cmd(command: str, args: dict[str, object]) -> list[str]:
    """Zostavi prikazovy riadok pre subprocess."""
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
    """Automaticke potvrdenia pre CLI prikazy, ktore sa cez web nesmu zaseknut na input()."""
    if command == "bootstrap" and args.get("--drop") is True:
        return "y\n"
    if command == "journals-apply" and not args.get("--preview"):
        # Batch rezim pyta jedno potvrdenie. Interaktivny rezim pyta po skupinach;
        # posleme dost odpovedi pre bezne davky, aby web beh nezostal visiet.
        return "y\n" * (1000 if args.get("--interactive") else 1)
    return None


def _iter_sse_for_command(command: str, args: dict[str, object]):
    cmd = _build_cmd(command, args)
    yield f"data: Spustam: {' '.join(cmd)}\n\n"
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            text=True,
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
            line = line.rstrip("\n")
            yield f"data: {line}\n\n"
        proc.wait()
        rc = proc.returncode
        yield "data: \n\n"
        if rc == 0:
            yield "data: [HOTOVO] Prikaz dokonceny uspesne.\n\n"
        else:
            yield f"data: [CHYBA] Prikaz skoncil s kodom {rc}.\n\n"
    except Exception as exc:
        yield f"data: [VYNIMKA] {exc}\n\n"


def _run_command_capture(command: str, args: dict[str, object]) -> tuple[int, str]:
    cmd = _build_cmd(command, args)
    proc = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        input=_stdin_for_command(command, args),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    return proc.returncode, proc.stdout


def _read_schedules_unlocked() -> list[dict]:
    if not SCHEDULE_FILE.exists():
        return []
    try:
        return json.loads(SCHEDULE_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []


def _write_schedules_unlocked(schedules: list[dict]) -> None:
    SCHEDULE_FILE.parent.mkdir(parents=True, exist_ok=True)
    SCHEDULE_FILE.write_text(
        json.dumps(schedules, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_schedules() -> list[dict]:
    with _schedule_lock:
        return _read_schedules_unlocked()


def _save_schedules(schedules: list[dict]) -> None:
    with _schedule_lock:
        _write_schedules_unlocked(schedules)


def _mark_schedule(schedule_id: str, **updates) -> None:
    with _schedule_lock:
        schedules = _read_schedules_unlocked()
        for item in schedules:
            if item["id"] == schedule_id:
                item.update(updates)
                break
        _write_schedules_unlocked(schedules)


def _parse_run_at(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    run_at = datetime.fromisoformat(normalized)
    if run_at.tzinfo is None:
        run_at = run_at.replace(tzinfo=datetime.now().astimezone().tzinfo)
    return run_at


def _scheduler_loop() -> None:
    while True:
        now = datetime.now(timezone.utc)
        due: list[dict] = []

        with _schedule_lock:
            schedules = _read_schedules_unlocked()
            for item in schedules:
                if item.get("status") != "scheduled":
                    continue
                run_at = _parse_run_at(item["run_at"]).astimezone(timezone.utc)
                if run_at <= now:
                    item["status"] = "running"
                    item["started_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
                    due.append(item.copy())
            if due:
                _write_schedules_unlocked(schedules)

        for item in due:
            output_parts: list[str] = []
            success = True
            for command_item in item.get("commands", []):
                command = command_item["command"]
                args = command_item.get("args", {})
                output_parts.append(f"$ {' '.join(_build_cmd(command, args))}\n")
                rc, out = _run_command_capture(command, args)
                output_parts.append(out)
                if rc != 0:
                    success = False
                    output_parts.append(f"\n[CHYBA] Prikaz skoncil s kodom {rc}.\n")
                    break
            _mark_schedule(
                item["id"],
                status="done" if success else "error",
                finished_at=datetime.now().astimezone().isoformat(timespec="seconds"),
                output="".join(output_parts)[-12000:],
            )

        time.sleep(10)


def ensure_scheduler_started() -> None:
    global _scheduler_started
    if _scheduler_started:
        return
    thread = threading.Thread(target=_scheduler_loop, name="pipeline-scheduler", daemon=True)
    thread.start()
    _scheduler_started = True


def _public_schedules() -> list[dict]:
    schedules = _load_schedules()
    schedules.sort(key=lambda item: item.get("run_at", ""), reverse=False)
    return schedules[-20:]


@bp.route("/pipeline")
def index():
    ensure_scheduler_started()
    return render_template(
        "pipeline/index.html",
        pipeline_steps=PIPELINE_STEPS,
        pipeline_sections=PIPELINE_SECTIONS,
        schedules=_public_schedules(),
    )


@bp.route("/pipeline/schedules", methods=["GET"])
def schedules():
    ensure_scheduler_started()
    return jsonify({"schedules": _public_schedules()})


@bp.route("/pipeline/schedules", methods=["POST"])
def create_schedule():
    ensure_scheduler_started()
    payload = request.get_json(silent=True) or {}
    run_at_raw = str(payload.get("run_at", "")).strip()
    commands_raw = payload.get("commands", [])
    if not run_at_raw or not commands_raw:
        return jsonify({"error": "Vyber prikazy a cas spustenia."}), 400

    try:
        parsed_run_at = _parse_run_at(run_at_raw)
        commands: list[dict] = []
        for command_item in commands_raw:
            command = str(command_item.get("command", "")).strip()
            args = _normalize_args(command, command_item.get("args", {}))
            commands.append({"command": command, "args": args})
    except (TypeError, ValueError) as exc:
        return jsonify({"error": str(exc)}), 400

    schedule_item = {
        "id": uuid4().hex,
        "run_at": parsed_run_at.astimezone().isoformat(timespec="seconds"),
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "status": "scheduled",
        "commands": commands,
    }

    schedules = _load_schedules()
    schedules.append(schedule_item)
    _save_schedules(schedules)
    return jsonify({"schedule": schedule_item}), 201


@bp.route("/pipeline/schedules/<schedule_id>", methods=["DELETE"])
def delete_schedule(schedule_id: str):
    schedules = [item for item in _load_schedules() if item["id"] != schedule_id]
    _save_schedules(schedules)
    return jsonify({"ok": True})


@bp.route("/pipeline/run", methods=["POST"])
def run_command():
    """SSE endpoint - spusti CLI prikaz alebo sekvenciu prikazov a streamuje vystup."""
    command_items: list[dict]

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
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
