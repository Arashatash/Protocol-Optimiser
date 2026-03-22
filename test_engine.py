"""Simulate good and bad parsed scans through the rule engine (Phase 3)."""

from __future__ import annotations

from rich.console import Console
from rich.panel import Panel

from rule_engine import evaluate, load_rules


def main() -> None:
    console = Console()
    rules = load_rules()

    good_scan = {
        "study_description": "MRI Brain MS",
        "series_description": "SAG 3D FLAIR",
        "tr_ms": 9000.0,
        "te_ms": 100.0,
    }

    bad_scan = {
        "study_description": "MRI Brain MS",
        "series_description": "SAG 3D FLAIR",
        "tr_ms": 9000.0,
        "te_ms": 70.0,
    }

    error_scan = {
        "study_description": "Unknown",
        "series_description": "Unknown",
        "tr_ms": None,
        "te_ms": None,
        "read_error": "FileNotFoundError: not there",
    }

    cases = [
        ("Good scan (TE/TR in range)", good_scan),
        ("Bad scan (TE below min)", bad_scan),
        ("Read error", error_scan),
    ]

    for title, parsed in cases:
        result = evaluate(parsed, rules)
        lines = [
            f"[bold]status:[/bold] {result['status']}",
            "[bold]messages:[/bold]",
        ]
        for m in result["messages"]:
            lines.append(f"  - {m}")
        console.print(Panel("\n".join(lines), title=title, expand=False))
        console.print()


if __name__ == "__main__":
    main()
