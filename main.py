"""Entry point: 24/7 folder watcher, drift check, and ntfy.sh alerts (Phase 4–5)."""

from __future__ import annotations

import random
import time
from pathlib import Path

import requests
from rich.console import Console
from rich.panel import Panel
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from dicom_parser import parse_dicom
from rule_engine import evaluate, load_rules

console = Console()
ROOT = Path(__file__).resolve().parent
DROPZONE = ROOT / "dicom_dropzone"

NTFY_URL = "https://ntfy.sh/arash-rad-alerts-2026"


def send_push_notification(title: str, message: str) -> None:
    """POST alert to ntfy.sh (body = message; Title and Tags headers per ntfy API)."""
    try:
        resp = requests.post(
            NTFY_URL,
            data=message.encode("utf-8"),
            headers={
                "Title": title,
                "Tags": "rotating_light,warning",
            },
            timeout=30,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        console.print(f"[yellow]ntfy.sh notification failed: {exc}[/yellow]")


def print_scan_report(path: Path, parsed: dict, result: dict) -> None:
    """Rich output for one parsed file + evaluation (same style as manual runs)."""
    te = parsed.get("te_ms")
    tr = parsed.get("tr_ms")
    te_s = "None" if te is None else f"{float(te):g}"
    tr_s = "None" if tr is None else f"{float(tr):g}"

    body_lines = [
        f"[bold]File:[/bold] {path}",
        f"[bold]Series:[/bold] {parsed.get('series_description')}  "
        f"[bold]TE:[/bold] {te_s}  [bold]TR:[/bold] {tr_s}",
        "",
    ]

    status = result["status"]
    tag = status.upper()
    if status == "pass":
        body_lines.append(f"[green bold]Drift check: {tag}[/green bold]")
    elif status == "error":
        body_lines.append(f"[red bold]Drift check: {tag}[/red bold]")
    else:
        body_lines.append(f"[yellow bold]Drift check: {tag}[/yellow bold]")

    for msg in result["messages"]:
        if status == "pass":
            body_lines.append(f"[green]{msg}[/green]")
        elif status == "error":
            body_lines.append(f"[red]{msg}[/red]")
        else:
            body_lines.append(f"[yellow]{msg}[/yellow]")

    grade = result.get("clinical_grade")
    if grade:
        body_lines.append("")
        body_lines.append(f"[bold]Clinical grade:[/bold] {grade}")
    eff = result.get("efficiency_score")
    if eff is not None:
        body_lines.append(f"[bold]Efficiency score:[/bold] {eff}/100")

    console.print(Panel("\n".join(body_lines), title="[bold]New scan[/bold]", expand=False))
    console.print()


class DicomDropHandler(FileSystemEventHandler):
    """React to new files in the dropzone; only processes .dcm after a short settle delay."""

    def __init__(self, rules: dict) -> None:
        super().__init__()
        self._rules = rules

    def on_created(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() != ".dcm":
            return

        time.sleep(random.uniform(1.0, 2.0))

        if not path.is_file():
            console.print(f"[dim]Skipped (not a file after wait): {path}[/dim]")
            return

        try:
            parsed = parse_dicom(path)
            result = evaluate(parsed, self._rules)
            print_scan_report(path, parsed, result)

            status = result["status"]
            if status in ("fail", "error"):
                body = "\n".join([str(path), "", *result["messages"]])
                if status == "error":
                    title = "DICOM read error"
                else:
                    title = "Protocol drift detected"
                send_push_notification(title, body)
        except Exception as exc:
            console.print(f"[red]Error processing {path}: {type(exc).__name__}: {exc}[/red]")


def main() -> None:
    DROPZONE.mkdir(parents=True, exist_ok=True)
    rules = load_rules()

    console.print(
        "[bold green]Agent running 24/7. Monitoring dicom_dropzone for new scans... "
        "Press Ctrl+C to exit.[/bold green]"
    )
    console.print(f"[dim]Watching:[/dim] {DROPZONE}\n")

    handler = DicomDropHandler(rules)
    observer = Observer()
    observer.schedule(handler, str(DROPZONE), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopping observer. Goodbye![/yellow]")
        observer.stop()
    observer.join(timeout=10)


if __name__ == "__main__":
    main()
