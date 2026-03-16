from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "inputs"
DELIVERY_DIR = INPUT_DIR / "deliveries"
ADJUSTMENT_DIR = INPUT_DIR / "adjustments"
PRICE_UPDATE_DIR = INPUT_DIR / "price_updates"
OUTPUT_DIR = BASE_DIR / "outputs"

MASTER_PATH = OUTPUT_DIR / "square_master_inventory.csv"
LEGACY_MASTER_PATH = BASE_DIR / "square_master_inventory.csv"
SNAPSHOT_PATH = OUTPUT_DIR / "current_stock_snapshot.csv"
PRICING_SNAPSHOT_PATH = OUTPUT_DIR / "current_pricing_snapshot.csv"
SQUARE_STOCK_UPDATE_PATH = OUTPUT_DIR / "square_inventory_quantity_update.csv"
SQUARE_PRICE_UPDATE_PATH = OUTPUT_DIR / "square_catalog_price_update.csv"
ISSUES_PATH = OUTPUT_DIR / "stock_transaction_issues.csv"
SUMMARY_PATH = OUTPUT_DIR / "stock_snapshot_summary.txt"


@dataclass
class StockTotals:
    received_qty: Decimal = Decimal("0")
    adjusted_qty: Decimal = Decimal("0")
    last_delivery_date: str = ""
    last_activity_date: str = ""
    last_received_unit_cost: str = ""


@dataclass
class PriceTotals:
    current_price: str = ""
    last_price_update_date: str = ""
    last_price_reason: str = ""


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def parse_decimal(value: str, field_name: str, path: Path, row_number: int, issues: list[dict[str, str]]) -> Decimal | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        issues.append(
            {
                "source_file": path.name,
                "row_number": str(row_number),
                "issue_type": "invalid_number",
                "sku": "",
                "details": f"Could not parse {field_name} value '{text}'.",
            }
        )
        return None


def parse_date(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return text


def format_quantity(value: Decimal) -> str:
    normalized = value.normalize()
    return format(normalized, "f").rstrip("0").rstrip(".") if "." in format(normalized, "f") else format(normalized, "f")


def resolve_master_path() -> Path:
    if MASTER_PATH.exists():
        return MASTER_PATH
    if LEGACY_MASTER_PATH.exists():
        return LEGACY_MASTER_PATH
    raise FileNotFoundError("square_master_inventory.csv was not found in outputs/ or the repo root.")


def choose_quantity_column(fieldnames: list[str]) -> str:
    preferred = [name for name in fieldnames if name.startswith("New Quantity ")]
    if preferred:
        return preferred[0]
    raise ValueError("Could not find a 'New Quantity ...' column in the Square inventory file.")


def choose_enabled_column(fieldnames: list[str], quantity_column: str) -> str | None:
    suffix = quantity_column.replace("New Quantity ", "")
    candidate = f"Enabled {suffix}"
    return candidate if candidate in fieldnames else None


def load_price_updates(master_rows: list[dict[str, str]], issues: list[dict[str, str]]) -> dict[str, PriceTotals]:
    totals: dict[str, PriceTotals] = {}
    known_skus = {row.get("SKU", "").strip() for row in master_rows if row.get("SKU", "").strip()}

    for path in sorted(PRICE_UPDATE_DIR.glob("*.csv")):
        rows = read_csv_rows(path)
        for row_number, row in enumerate(rows, start=2):
            sku = str(row.get("SKU", "")).strip()
            new_price = parse_decimal(row.get("New Price", ""), "New Price", path, row_number, issues)
            tx_date = parse_date(row.get("Transaction Date", ""))
            reason = str(row.get("Reason", "")).strip()
            if not sku or new_price is None:
                continue
            if sku not in known_skus:
                issues.append(
                    {
                        "source_file": path.name,
                        "row_number": str(row_number),
                        "issue_type": "unknown_sku",
                        "sku": sku,
                        "details": "Price update row SKU does not exist in the master inventory.",
                    }
                )
                continue

            current = totals.get(sku, PriceTotals())
            replace_current = False
            if not current.last_price_update_date:
                replace_current = True
            elif tx_date and tx_date >= current.last_price_update_date:
                replace_current = True

            if replace_current:
                totals[sku] = PriceTotals(
                    current_price=format_quantity(new_price),
                    last_price_update_date=tx_date,
                    last_price_reason=reason,
                )

    return totals


def load_stock_totals(master_rows: list[dict[str, str]]) -> tuple[dict[str, StockTotals], list[dict[str, str]]]:
    totals: dict[str, StockTotals] = defaultdict(StockTotals)
    issues: list[dict[str, str]] = []
    known_skus = {row.get("SKU", "").strip() for row in master_rows if row.get("SKU", "").strip()}

    for path in sorted(DELIVERY_DIR.glob("*.csv")):
        rows = read_csv_rows(path)
        for row_number, row in enumerate(rows, start=2):
            sku = str(row.get("SKU", "")).strip()
            qty = parse_decimal(row.get("Quantity Received", ""), "Quantity Received", path, row_number, issues)
            unit_cost = str(row.get("Unit Cost", "")).strip()
            tx_date = parse_date(row.get("Transaction Date", ""))
            if not sku or qty is None:
                continue
            if sku not in known_skus:
                issues.append(
                    {
                        "source_file": path.name,
                        "row_number": str(row_number),
                        "issue_type": "unknown_sku",
                        "sku": sku,
                        "details": "Delivery row SKU does not exist in the master inventory.",
                    }
                )
                continue
            totals[sku].received_qty += qty
            totals[sku].last_received_unit_cost = unit_cost
            if tx_date and tx_date > totals[sku].last_delivery_date:
                totals[sku].last_delivery_date = tx_date
            if tx_date and tx_date > totals[sku].last_activity_date:
                totals[sku].last_activity_date = tx_date

    for path in sorted(ADJUSTMENT_DIR.glob("*.csv")):
        rows = read_csv_rows(path)
        for row_number, row in enumerate(rows, start=2):
            sku = str(row.get("SKU", "")).strip()
            qty = parse_decimal(row.get("Quantity Change", ""), "Quantity Change", path, row_number, issues)
            tx_date = parse_date(row.get("Transaction Date", ""))
            if not sku or qty is None:
                continue
            if sku not in known_skus:
                issues.append(
                    {
                        "source_file": path.name,
                        "row_number": str(row_number),
                        "issue_type": "unknown_sku",
                        "sku": sku,
                        "details": "Adjustment row SKU does not exist in the master inventory.",
                    }
                )
                continue
            totals[sku].adjusted_qty += qty
            if tx_date and tx_date > totals[sku].last_activity_date:
                totals[sku].last_activity_date = tx_date

    return totals, issues


def build_snapshot_rows(master_rows: list[dict[str, str]], totals: dict[str, StockTotals]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for master_row in master_rows:
        sku = master_row.get("SKU", "").strip()
        stock = totals.get(sku, StockTotals())
        current_qty = stock.received_qty + stock.adjusted_qty
        rows.append(
            {
                "SKU": sku,
                "Item Name": master_row.get("Item Name", ""),
                "Default Vendor Name": master_row.get("Default Vendor Name", ""),
                "Default Unit Cost": master_row.get("Default Unit Cost", ""),
                "Price": master_row.get("Price", ""),
                "Quantity Received": format_quantity(stock.received_qty),
                "Quantity Adjusted": format_quantity(stock.adjusted_qty),
                "Current Quantity": format_quantity(current_qty),
                "Last Delivery Date": stock.last_delivery_date,
                "Last Activity Date": stock.last_activity_date,
                "Last Received Unit Cost": stock.last_received_unit_cost,
            }
        )
    return rows


def build_pricing_snapshot_rows(master_rows: list[dict[str, str]], price_totals: dict[str, PriceTotals]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for master_row in master_rows:
        sku = master_row.get("SKU", "").strip()
        update = price_totals.get(sku, PriceTotals())
        base_price = master_row.get("Price", "")
        current_price = update.current_price or base_price
        rows.append(
            {
                "SKU": sku,
                "Item Name": master_row.get("Item Name", ""),
                "Default Vendor Name": master_row.get("Default Vendor Name", ""),
                "Default Unit Cost": master_row.get("Default Unit Cost", ""),
                "Master Price": base_price,
                "Current Selling Price": current_price,
                "Last Price Update Date": update.last_price_update_date,
                "Last Price Update Reason": update.last_price_reason,
            }
        )
    return rows


def write_snapshot(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "SKU",
        "Item Name",
        "Default Vendor Name",
        "Default Unit Cost",
        "Price",
        "Quantity Received",
        "Quantity Adjusted",
        "Current Quantity",
        "Last Delivery Date",
        "Last Activity Date",
        "Last Received Unit Cost",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_pricing_snapshot(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "SKU",
        "Item Name",
        "Default Vendor Name",
        "Default Unit Cost",
        "Master Price",
        "Current Selling Price",
        "Last Price Update Date",
        "Last Price Update Reason",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_square_update(path: Path, master_rows: list[dict[str, str]], totals: dict[str, StockTotals], fieldnames: list[str]) -> tuple[str, str | None]:
    quantity_column = choose_quantity_column(fieldnames)
    enabled_column = choose_enabled_column(fieldnames, quantity_column)

    updated_rows: list[dict[str, str]] = []
    for row in master_rows:
        updated = dict(row)
        sku = row.get("SKU", "").strip()
        stock = totals.get(sku, StockTotals())
        current_qty = stock.received_qty + stock.adjusted_qty
        updated[quantity_column] = format_quantity(current_qty)
        if enabled_column and sku:
            updated[enabled_column] = "Y"
        updated_rows.append(updated)

    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    return quantity_column, enabled_column


def write_square_price_update(
    path: Path,
    master_rows: list[dict[str, str]],
    price_totals: dict[str, PriceTotals],
    fieldnames: list[str],
) -> int:
    updated_rows: list[dict[str, str]] = []
    changed_count = 0
    for row in master_rows:
        updated = dict(row)
        sku = row.get("SKU", "").strip()
        update = price_totals.get(sku)
        if update and update.current_price:
            if update.current_price != str(row.get("Price", "")).strip():
                changed_count += 1
            updated["Price"] = update.current_price
        updated_rows.append(updated)

    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    return changed_count


def write_issues(path: Path, issues: list[dict[str, str]]) -> None:
    fieldnames = ["source_file", "row_number", "issue_type", "sku", "details"]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(issues)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    master_path = resolve_master_path()
    master_rows = read_csv_rows(master_path)
    if not master_rows:
        raise ValueError("Master inventory is empty.")

    fieldnames = list(master_rows[0].keys())
    totals, issues = load_stock_totals(master_rows)
    price_totals = load_price_updates(master_rows, issues)
    snapshot_rows = build_snapshot_rows(master_rows, totals)
    pricing_snapshot_rows = build_pricing_snapshot_rows(master_rows, price_totals)
    quantity_column, enabled_column = write_square_update(SQUARE_STOCK_UPDATE_PATH, master_rows, totals, fieldnames)
    price_change_count = write_square_price_update(SQUARE_PRICE_UPDATE_PATH, master_rows, price_totals, fieldnames)

    write_snapshot(SNAPSHOT_PATH, snapshot_rows)
    write_pricing_snapshot(PRICING_SNAPSHOT_PATH, pricing_snapshot_rows)
    write_issues(ISSUES_PATH, issues)

    total_received = sum((stock.received_qty for stock in totals.values()), Decimal("0"))
    total_adjusted = sum((stock.adjusted_qty for stock in totals.values()), Decimal("0"))
    summary_lines = [
        f"Master inventory source: {master_path}",
        f"Stock snapshot: {SNAPSHOT_PATH}",
        f"Square quantity update file: {SQUARE_STOCK_UPDATE_PATH}",
        f"Pricing snapshot: {PRICING_SNAPSHOT_PATH}",
        f"Square price update file: {SQUARE_PRICE_UPDATE_PATH}",
        f"Issues file: {ISSUES_PATH}",
        f"Delivery files processed: {len(list(DELIVERY_DIR.glob('*.csv')))}",
        f"Adjustment files processed: {len(list(ADJUSTMENT_DIR.glob('*.csv')))}",
        f"Price update files processed: {len(list(PRICE_UPDATE_DIR.glob('*.csv')))}",
        f"SKUs with any stock activity: {len(totals)}",
        f"SKUs with price overrides: {len(price_totals)}",
        f"Total quantity received: {format_quantity(total_received)}",
        f"Total quantity adjusted: {format_quantity(total_adjusted)}",
        f"Square quantity column updated: {quantity_column}",
        f"Square enabled column updated: {enabled_column or '[none found]'}",
        f"Square price rows changed: {price_change_count}",
        f"Issue rows: {len(issues)}",
    ]
    SUMMARY_PATH.write_text("\n".join(summary_lines), encoding="utf-8")
    print("\n".join(summary_lines))


if __name__ == "__main__":
    main()
