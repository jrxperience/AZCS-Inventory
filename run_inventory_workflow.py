from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path
import subprocess
import sys


BASE_DIR = Path(__file__).resolve().parent
CURRENT_BASELINE_DIR = BASE_DIR / "square_ready" / "BASELINE_CURRENT"
UPLOAD_PATH = CURRENT_BASELINE_DIR / "UPLOAD_THIS_TO_SQUARE.csv"
VALIDATION_PATH = CURRENT_BASELINE_DIR / "WORKFLOW_VALIDATION.txt"

STEPS = (
    ("master inventory", "build_master_inventory.py"),
    ("sales match audit", "build_sales_match_audit.py"),
    ("pricing recommendations", "build_pricing_recommendations.py"),
)


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).replace("\xa0", " ").strip()


def run_step(name: str, command: list[str]) -> None:
    result = subprocess.run(
        command,
        cwd=BASE_DIR,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Workflow step failed: {name}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )
    print(f"[ok] {name}")
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(f"[warn] {name}")
        print(result.stderr.strip())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full AZCS inventory workflow.")
    parser.add_argument("--export", dest="export_path", help="Optional path to the current Square export CSV or XLSX.")
    parser.add_argument("--template", dest="template_path", help="Optional path to the Square import template CSV.")
    parser.add_argument("--run-tag", dest="run_tag", help="Optional run tag for the baseline import outputs.")
    return parser.parse_args()


def read_upload_records(path: Path) -> tuple[int, list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.reader(handle))
    header_index = next(index for index, row in enumerate(rows) if "SKU" in row)
    headers = rows[header_index]
    records = [
        dict(zip(headers, row[: len(headers)] + [""] * max(0, len(headers) - len(row))))
        for row in rows[header_index + 1 :]
        if any(clean_text(cell) for cell in row)
    ]
    return header_index, headers, records


def validate_upload(path: Path) -> list[str]:
    header_index, _, records = read_upload_records(path)
    active_rows = [record for record in records if clean_text(record.get("Archived")).upper() != "Y"]
    duplicate_active_skus = Counter(
        clean_text(record.get("SKU")).upper() for record in active_rows if clean_text(record.get("SKU"))
    )
    duplicate_active_names = Counter(
        clean_text(record.get("Item Name")).upper() for record in active_rows if clean_text(record.get("Item Name"))
    )
    deepest_counts: Counter[str] = Counter()
    for record in active_rows:
        categories = [clean_text(part) for part in clean_text(record.get("Categories")).split(",") if clean_text(part)]
        deepest = max(categories, key=lambda value: (value.count(">"), len(value))) if categories else ""
        deepest_counts[deepest] += 1

    lines = [
        f"Upload file: {path}",
        f"Template prefix rows: {header_index}",
        f"Total rows: {len(records)}",
        f"Active rows: {len(active_rows)}",
        f"Archive rows: {len(records) - len(active_rows)}",
        f"Duplicate active SKUs: {sum(1 for count in duplicate_active_skus.values() if count > 1)}",
        f"Duplicate active item names: {sum(1 for count in duplicate_active_names.values() if count > 1)}",
        f"Max deepest category size: {max(deepest_counts.values()) if deepest_counts else 0}",
        f"Tucker active rows: {sum(1 for record in active_rows if clean_text(record.get('Reporting Category')) == 'Tucker')}",
    ]

    if any(count > 1 for count in duplicate_active_skus.values()):
        raise RuntimeError("Validation failed: duplicate active SKUs are still present in the final upload.")
    if deepest_counts and max(deepest_counts.values()) > 200:
        raise RuntimeError("Validation failed: at least one deepest category bucket is larger than 200 items.")

    return lines


def main() -> None:
    args = parse_args()

    for name, script_name in STEPS:
        run_step(name, [sys.executable, script_name])

    baseline_command = [sys.executable, "build_baseline_square_inventory_import.py"]
    if args.export_path:
        baseline_command.extend(["--export", args.export_path])
    if args.template_path:
        baseline_command.extend(["--template", args.template_path])
    if args.run_tag:
        baseline_command.extend(["--run-tag", args.run_tag])
    run_step("baseline Square import", baseline_command)

    if not UPLOAD_PATH.exists():
        raise FileNotFoundError(f"Expected final upload file was not created: {UPLOAD_PATH}")

    validation_lines = validate_upload(UPLOAD_PATH)
    CURRENT_BASELINE_DIR.mkdir(parents=True, exist_ok=True)
    VALIDATION_PATH.write_text("\n".join(validation_lines), encoding="utf-8")
    print("[ok] workflow validation")
    print("\n".join(validation_lines))


if __name__ == "__main__":
    main()
