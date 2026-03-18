from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

try:
    from openpyxl import Workbook
except ImportError:  # pragma: no cover - optional dependency
    Workbook = None

from build_receiving_import import (
    choose_location_columns,
    format_quantity,
    normalize_cell,
    normalize_name,
    parse_decimal,
    read_square_export_rows,
    resolve_latest_square_export,
)


BASE_DIR = Path(__file__).resolve().parent
TO_IMPORT_DIR = BASE_DIR / "to_import"
OUTPUT_DIR = BASE_DIR / "outputs"

STRATEGIC_IMPORT_PATH = OUTPUT_DIR / "square_master_inventory_strategic_pricing.csv"
FRIENDLY_IMPORT_PATH = TO_IMPORT_DIR / "catalog_import_current.csv"

SEEDED_IMPORT_PATH = OUTPUT_DIR / "square_seed_stock_import.csv"
SEEDED_IMPORT_XLSX_PATH = OUTPUT_DIR / "square_seed_stock_import.xlsx"
SEEDED_AUDIT_PATH = OUTPUT_DIR / "stock_seed_audit.csv"
SEEDED_ISSUES_PATH = OUTPUT_DIR / "stock_seed_issues.csv"
SEEDED_SUMMARY_PATH = OUTPUT_DIR / "stock_seed_summary.txt"

TARGET_LOCATION = "AZCS"


@dataclass
class CatalogLookup:
    sku_set: set[str]
    vendor_code_map: dict[str, set[str]]
    gtin_map: dict[str, set[str]]
    name_map: dict[str, set[str]]
    row_index_by_sku: dict[str, int]


def read_catalog_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def resolve_catalog_path() -> Path:
    if FRIENDLY_IMPORT_PATH.exists():
        return FRIENDLY_IMPORT_PATH
    if STRATEGIC_IMPORT_PATH.exists():
        return STRATEGIC_IMPORT_PATH
    raise FileNotFoundError("Could not find the current strategic import file.")


def build_catalog_lookup(rows: list[dict[str, str]]) -> CatalogLookup:
    sku_set: set[str] = set()
    vendor_code_map: dict[str, set[str]] = defaultdict(set)
    gtin_map: dict[str, set[str]] = defaultdict(set)
    name_map: dict[str, set[str]] = defaultdict(set)
    row_index_by_sku: dict[str, int] = {}

    for index, row in enumerate(rows):
        sku = normalize_cell(row.get("SKU", ""))
        if not sku:
            continue
        sku_set.add(sku)
        row_index_by_sku[sku] = index

        vendor_code = normalize_cell(row.get("Default Vendor Code", ""))
        if vendor_code:
            vendor_code_map[vendor_code].add(sku)

        gtin = normalize_cell(row.get("GTIN", ""))
        if gtin:
            gtin_map[gtin].add(sku)

        for field in ("Item Name", "Customer-facing Name"):
            name_key = normalize_name(row.get(field, ""))
            if name_key:
                name_map[name_key].add(sku)

    return CatalogLookup(
        sku_set=sku_set,
        vendor_code_map=dict(vendor_code_map),
        gtin_map=dict(gtin_map),
        name_map=dict(name_map),
        row_index_by_sku=row_index_by_sku,
    )


def resolve_unique_candidate(candidates: set[str]) -> tuple[str | None, str]:
    if len(candidates) == 1:
        return next(iter(candidates)), "unique"
    if len(candidates) > 1:
        return None, "ambiguous"
    return None, "missing"


def resolve_catalog_sku(export_row: dict[str, str], lookup: CatalogLookup) -> tuple[str | None, str]:
    sku = normalize_cell(export_row.get("SKU", ""))
    if sku:
        if sku in lookup.sku_set:
            return sku, "sku"

    vendor_code = normalize_cell(export_row.get("Default Vendor Code", ""))
    if vendor_code:
        resolved, status = resolve_unique_candidate(lookup.vendor_code_map.get(vendor_code, set()))
        if resolved:
            return resolved, "vendor_code"
        if status != "missing":
            return None, f"vendor_code_{status}"

    gtin = normalize_cell(export_row.get("GTIN", ""))
    if gtin:
        resolved, status = resolve_unique_candidate(lookup.gtin_map.get(gtin, set()))
        if resolved:
            return resolved, "gtin"
        if status != "missing":
            return None, f"gtin_{status}"

    for field in ("Item Name", "Customer-facing Name"):
        name_key = normalize_name(export_row.get(field, ""))
        if not name_key:
            continue
        resolved, status = resolve_unique_candidate(lookup.name_map.get(name_key, set()))
        if resolved:
            return resolved, field.lower().replace(" ", "_")
        if status != "missing":
            return None, f"{field.lower().replace(' ', '_')}_{status}"

    return None, "no_match"


def detect_source_quantity_column(export_rows: list[dict[str, str]], target_location: str) -> tuple[str, str]:
    if not export_rows:
        raise ValueError("The Square export file has no item rows.")

    fieldnames = [key for key in export_rows[0].keys() if not key.startswith("__")]
    quantity_columns = [field for field in fieldnames if field.startswith("Current Quantity ")]
    candidates: list[tuple[int, int, str, str]] = []
    for column in quantity_columns:
        location = normalize_cell(column.replace("Current Quantity ", ""))
        if location == normalize_cell(target_location):
            continue
        nonblank = 0
        nonzero = 0
        for row in export_rows:
            value = normalize_cell(row.get(column, ""))
            if not value:
                continue
            nonblank += 1
            if value not in {"0", "0.0", "0.00"}:
                nonzero += 1
        candidates.append((nonzero, nonblank, location, column))

    if not candidates:
        raise ValueError(f"Could not find a source current quantity column outside the target location {target_location}.")

    _, _, location, column = max(candidates, key=lambda item: (item[0], item[1], item[2]))
    return location, column


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_xlsx(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> str:
    if Workbook is None:
        return "openpyxl not available"
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Seeded Import"
    sheet.append(fieldnames)
    for row in rows:
        sheet.append([row.get(field, "") for field in fieldnames])
    try:
        workbook.save(path)
    except PermissionError:
        return f"file locked: {path}"
    return str(path)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    export_path = resolve_latest_square_export()
    export_rows = read_square_export_rows(export_path)
    source_location, source_current_col = detect_source_quantity_column(export_rows, TARGET_LOCATION)

    catalog_path = resolve_catalog_path()
    catalog_rows = read_catalog_rows(catalog_path)
    lookup = build_catalog_lookup(catalog_rows)

    catalog_fieldnames = list(catalog_rows[0].keys()) if catalog_rows else []
    _, target_new_col, target_enabled_col = choose_location_columns(catalog_fieldnames, TARGET_LOCATION)

    issues: list[dict[str, str]] = []
    audit_rows: list[dict[str, str]] = []
    matched_export_rows = 0
    seeded_nonblank_rows = 0
    seeded_nonzero_rows = 0
    total_seeded_quantity = Decimal("0")
    seen_catalog_skus: set[str] = set()

    for export_row in export_rows:
        resolved_sku, match_type = resolve_catalog_sku(export_row, lookup)
        if not resolved_sku:
            issues.append(
                {
                    "source_file": export_path.name,
                    "row_number": normalize_cell(export_row.get("__row_number", "")),
                    "issue_type": "unmatched_export_row",
                    "sku": normalize_cell(export_row.get("SKU", "")),
                    "details": (
                        "Could not map the live Square export row into the current AZCS import catalog by "
                        f"SKU/vendor code/GTIN/name. Match status: {match_type}."
                    ),
                }
            )
            continue

        source_qty = parse_decimal(
            export_row.get(source_current_col, ""),
            source_current_col,
            export_path,
            int(export_row.get("__row_number", "0") or "0"),
            issues,
        )
        if source_qty is None:
            continue

        matched_export_rows += 1
        if source_qty != 0:
            seeded_nonzero_rows += 1
        seeded_nonblank_rows += 1
        total_seeded_quantity += source_qty

        if resolved_sku in seen_catalog_skus:
            issues.append(
                {
                    "source_file": export_path.name,
                    "row_number": normalize_cell(export_row.get("__row_number", "")),
                    "issue_type": "duplicate_catalog_match",
                    "sku": resolved_sku,
                    "details": "More than one live export row resolved to the same AZCS catalog SKU. Last matching row kept.",
                }
            )
        seen_catalog_skus.add(resolved_sku)

        row = catalog_rows[lookup.row_index_by_sku[resolved_sku]]
        row[target_new_col] = format_quantity(source_qty)
        if target_enabled_col:
            row[target_enabled_col] = "Y"

        audit_rows.append(
            {
                "Catalog SKU": resolved_sku,
                "Catalog Item Name": normalize_cell(row.get("Item Name", "")),
                "Export SKU": normalize_cell(export_row.get("SKU", "")),
                "Export Item Name": normalize_cell(export_row.get("Item Name", "")),
                "Match Type": match_type,
                "Source Location": source_location,
                "Source Current Quantity Column": source_current_col,
                "Seeded New Quantity Column": target_new_col,
                "Seeded Quantity": format_quantity(source_qty),
            }
        )

    audit_fieldnames = [
        "Catalog SKU",
        "Catalog Item Name",
        "Export SKU",
        "Export Item Name",
        "Match Type",
        "Source Location",
        "Source Current Quantity Column",
        "Seeded New Quantity Column",
        "Seeded Quantity",
    ]

    write_csv(SEEDED_IMPORT_PATH, catalog_fieldnames, catalog_rows)
    xlsx_status = write_xlsx(SEEDED_IMPORT_XLSX_PATH, catalog_fieldnames, catalog_rows)
    write_csv(SEEDED_AUDIT_PATH, audit_fieldnames, audit_rows)
    write_csv(SEEDED_ISSUES_PATH, ["source_file", "row_number", "issue_type", "sku", "details"], issues)

    summary_lines = [
        f"Catalog source: {catalog_path}",
        f"Square export source: {export_path}",
        f"Seeded import CSV: {SEEDED_IMPORT_PATH}",
        f"Seeded import Excel: {xlsx_status}",
        f"Seed audit: {SEEDED_AUDIT_PATH}",
        f"Seed issues: {SEEDED_ISSUES_PATH}",
        f"Detected live source location: {source_location}",
        f"Source current quantity column: {source_current_col}",
        f"Seeded target new quantity column: {target_new_col}",
        f"Export item rows processed: {len(export_rows)}",
        f"Catalog rows in import: {len(catalog_rows)}",
        f"Matched export rows seeded into catalog: {matched_export_rows}",
        f"Catalog rows with seeded quantity values: {seeded_nonblank_rows}",
        f"Catalog rows with non-zero seeded quantities: {seeded_nonzero_rows}",
        f"Total quantity copied from live source location: {format_quantity(total_seeded_quantity)}",
        f"Issue rows: {len(issues)}",
        "Workflow note: this is a full catalog import file with only the target location New Quantity column seeded from the live Square export.",
        "Workflow note: unmatched live export rows were left out instead of forced into the new AZCS catalog.",
    ]
    SEEDED_SUMMARY_PATH.write_text("\n".join(summary_lines), encoding="utf-8")
    print("\n".join(summary_lines))


if __name__ == "__main__":
    main()
