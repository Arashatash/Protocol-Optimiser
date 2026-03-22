"""Run parse_dicom() on a sample .dcm file (Phase 2 smoke test)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rich.console import Console
from rich.table import Table

from dicom_parser import parse_dicom


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract Study/Series Description, TR, and TE from a DICOM file.",
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Path to a .dcm file",
    )
    args = parser.parse_args()
    console = Console()

    if not args.path.is_file():
        console.print(f"[red]Not a file:[/red] {args.path}")
        return 1

    result = parse_dicom(args.path)

    table = Table(title="DICOM header fields", show_header=True, header_style="bold")
    table.add_column("Field", style="cyan")
    table.add_column("Value")

    table.add_row("Study Description (0008,1030)", str(result.get("study_description")))
    table.add_row("Series Description (0008,103E)", str(result.get("series_description")))
    tr = result.get("tr_ms")
    te = result.get("te_ms")
    table.add_row("TR ms (0018,0080)", "None" if tr is None else f"{tr:g}")
    table.add_row("TE ms (0018,0081)", "None" if te is None else f"{te:g}")

    err = result.get("read_error")
    if err:
        table.add_row("[red]read_error[/red]", str(err))

    console.print(table)
    return 1 if err else 0


if __name__ == "__main__":
    sys.exit(main())
