from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pdfplumber
from openpyxl import load_workbook
from pypdf import PdfReader


BASE_DIR = Path(__file__).resolve().parent
TEMPLATE_PATH = BASE_DIR / "Square Import Template.csv"
OUTPUT_DIR = BASE_DIR

MASTER_OUT_PATH = OUTPUT_DIR / "square_master_inventory.csv"
REVIEW_OUT_PATH = OUTPUT_DIR / "square_master_inventory_overlap_review.csv"
SUMMARY_OUT_PATH = OUTPUT_DIR / "square_master_inventory_summary.txt"

CENT = Decimal("0.01")
MONEY_RE = re.compile(r"\$?\s*\d[\d,\s]*\.\d{2}")


@dataclass
class SourceItem:
    vendor: str
    source_file: str
    item_name: str
    sku: str = ""
    gtin: str = ""
    description: str = ""
    category: str = ""
    reporting_category: str = ""
    default_unit_cost: Decimal | None = None
    price: Decimal | None = None
    vendor_code: str = ""
    notes: list[str] = field(default_factory=list)
    generated_sku: bool = False


@dataclass
class ReviewIssue:
    issue_type: str
    vendor: str
    source_file: str
    item_name: str = ""
    sku: str = ""
    gtin: str = ""
    category: str = ""
    default_unit_cost: str = ""
    price: str = ""
    details: str = ""


def clean_text(value: object) -> str:
    text = str(value or "")
    replacements = {
        "\u2019": "'",
        "\u2018": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2013": "-",
        "\u2014": "-",
        "\u2022": "",
        "\uf0b7": "",
        "\u00bd": "1/2",
        "\xa0": " ",
        "\n": " ",
        "\r": " ",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" -")


def clean_code(value: object) -> str:
    text = clean_text(value).upper()
    text = re.sub(r"\s*-\s*", "-", text)
    text = re.sub(r"\s+", "", text)
    return text


def normalize_name(value: object) -> str:
    return re.sub(r"[^A-Z0-9]+", "", clean_text(value).upper())


def normalize_digits(value: object) -> str:
    return re.sub(r"\D+", "", str(value or ""))


def normalize_sku(value: object) -> str:
    return re.sub(r"\s+", "", clean_text(value).upper())


def parse_money(value: object) -> Decimal | None:
    if value is None:
        return None
    text = clean_text(value)
    if not text:
        return None
    text = text.replace("$", "").replace(",", "").replace(" ", "")
    if not text:
        return None
    try:
        return Decimal(text).quantize(CENT, rounding=ROUND_HALF_UP)
    except Exception:
        return None


def extract_money_values(value: object) -> list[Decimal]:
    text = clean_text(value)
    results: list[Decimal] = []
    for match in MONEY_RE.findall(text):
        money = parse_money(match)
        if money is not None:
            results.append(money)
    return results


def format_money(value: Decimal | None) -> str:
    if value is None:
        return ""
    return str(value.quantize(CENT, rounding=ROUND_HALF_UP))


def strip_product_markers(name: str) -> tuple[str, list[str]]:
    notes: list[str] = []
    cleaned = clean_text(name)
    while cleaned.endswith(("+", "*")):
        marker = cleaned[-1]
        cleaned = cleaned[:-1].rstrip()
        if marker == "+":
            notes.append("Source sheet marks this as a hazmat item.")
        elif marker == "*":
            notes.append("Source sheet marks this item with a special shipping note.")
    return cleaned, notes


def valid_gtin(value: object) -> str:
    digits = normalize_digits(value)
    return digits if len(digits) in {8, 12, 13, 14} else ""


def build_description(*parts: str) -> str:
    cleaned_parts = [clean_text(part) for part in parts if clean_text(part)]
    return " | ".join(cleaned_parts)


def make_category(vendor: str, category: str = "") -> str:
    if category:
        return f"{vendor} > {clean_text(category)}"
    return vendor


def read_csv_any(path: Path) -> list[list[str]]:
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            with path.open("r", encoding=encoding, newline="") as handle:
                return list(csv.reader(handle))
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error:
        raise last_error
    raise RuntimeError(f"Could not read {path}")


def load_square_headers(path: Path) -> list[str]:
    rows = read_csv_any(path)
    for row in rows:
        if row and clean_text(row[0]) == "Token":
            return [clean_text(cell) for cell in row]
    raise ValueError(f"Could not find Square header row in {path}")


def parse_barrens(path: Path) -> tuple[list[SourceItem], list[ReviewIssue]]:
    rows = read_csv_any(path)
    header_row = next(i for i, row in enumerate(rows) if row and clean_text(row[0]) == "StockCode")
    items: list[SourceItem] = []
    current_category = ""

    for row in rows[header_row + 1 :]:
        first = clean_text(row[0] if len(row) > 0 else "")
        second = clean_text(row[1] if len(row) > 1 else "")
        if not first and not second:
            continue

        price = parse_money(row[2] if len(row) > 2 else None)
        if first and not second and price is None:
            current_category = first
            continue

        if not first or not second or price is None:
            continue

        items.append(
            SourceItem(
                vendor="Barrens",
                source_file=path.name,
                item_name=second,
                sku=first,
                description=build_description(second, f"Category: {current_category}" if current_category else ""),
                category=make_category("Barrens", current_category),
                reporting_category="Barrens",
                default_unit_cost=price,
                vendor_code=first,
            )
        )

    return items, []


def parse_mpwsr(path: Path) -> tuple[list[SourceItem], list[ReviewIssue]]:
    rows = read_csv_any(path)
    headers = [clean_text(value) for value in rows[0]]
    index = {name: headers.index(name) for name in headers}
    items: list[SourceItem] = []

    for row in rows[1:]:
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))

        sku = clean_text(row[index["Name"]])
        item_name = clean_text(row[index["Description"]])
        cost = parse_money(row[index["Dealers"]])
        price = parse_money(row[index["Base Price"]])
        if not item_name:
            continue

        items.append(
            SourceItem(
                vendor="MPWSR",
                source_file=path.name,
                item_name=item_name,
                sku=sku,
                description=item_name,
                category=make_category("MPWSR"),
                reporting_category="MPWSR",
                default_unit_cost=cost,
                price=price,
                vendor_code=sku,
            )
        )

    return items, []


def parse_inseco(path: Path) -> tuple[list[SourceItem], list[ReviewIssue]]:
    workbook = load_workbook(path, read_only=True, data_only=True)
    sheet = workbook[workbook.sheetnames[0]]
    items: list[SourceItem] = []
    current_category = ""

    for row in sheet.iter_rows(values_only=True):
        cells = list(row[:4])
        values = [clean_text(value) for value in cells]
        non_empty = [value for value in values if value]
        if not non_empty:
            continue

        if len(non_empty) == 1:
            text = non_empty[0]
            if "DISTRIBUTOR PRICE LIST" in text.upper():
                continue
            if text.upper() in {"SKU", "PRODUCT#", "PRICE"}:
                continue
            current_category = text
            continue

        item_name = clean_text(cells[0])
        gtin = valid_gtin(cells[1])
        sku = clean_text(cells[2])
        cost = parse_money(cells[3])
        if not item_name or cost is None:
            continue

        items.append(
            SourceItem(
                vendor="INSECO",
                source_file=path.name,
                item_name=item_name,
                sku=sku,
                gtin=gtin,
                description=build_description(item_name, f"Category: {current_category}" if current_category else ""),
                category=make_category("INSECO", current_category),
                reporting_category="INSECO",
                default_unit_cost=cost,
                vendor_code=sku,
            )
        )

    workbook.close()
    return items, []


def parse_jracenstein(path: Path) -> tuple[list[SourceItem], list[ReviewIssue]]:
    workbook = load_workbook(path, read_only=True, data_only=True)
    sheet = workbook[workbook.sheetnames[0]]
    rows = list(sheet.iter_rows(values_only=True))
    headers = [clean_text(value) for value in rows[0]]
    index = {name: headers.index(name) for name in headers}
    items: list[SourceItem] = []

    for row in rows[1:]:
        if not row:
            continue
        sku = clean_text(row[index["Code"]] if len(row) > index["Code"] else "")
        item_name = clean_text(row[index["Model"]] if len(row) > index["Model"] else "")
        category = clean_text(row[index["Category"]] if len(row) > index["Category"] else "")
        case_qty = clean_text(row[index["Case"]] if len(row) > index["Case"] else "")
        price = parse_money(row[index["2026 List Price"]] if len(row) > index["2026 List Price"] else None)
        cost = parse_money(row[index["2026 Distributor Price"]] if len(row) > index["2026 Distributor Price"] else None)
        if not item_name:
            continue

        description = build_description(
            item_name,
            f"Category: {category}" if category else "",
            f"Case pack: {case_qty}" if case_qty else "",
        )
        items.append(
            SourceItem(
                vendor="JRacenstein",
                source_file=path.name,
                item_name=item_name,
                sku=sku,
                description=description,
                category=make_category("JRacenstein", category),
                reporting_category="JRacenstein",
                default_unit_cost=cost,
                price=price,
                vendor_code=sku,
            )
        )

    workbook.close()
    return items, []


def infer_be_category(description: str) -> str:
    text = clean_text(description).upper()
    if text.startswith("HW"):
        return "Hot Water Equipment"
    if text.startswith("CW"):
        return "Cold Water Equipment"
    if text.startswith("PW"):
        return "Pressure Washers"
    return "Equipment"


def parse_be(path: Path) -> tuple[list[SourceItem], list[ReviewIssue]]:
    items: list[SourceItem] = []

    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue

            for row in table:
                row = list(row or [])
                if len(row) < 6:
                    continue
                stockcode = clean_text(row[0])
                if not stockcode or stockcode == "Stockcode":
                    continue
                description = clean_text(f"{clean_text(row[1])} {clean_text(row[2])}")
                upc = valid_gtin(row[4])
                cost = parse_money(row[5])
                if not description:
                    continue

                category = infer_be_category(description)
                items.append(
                    SourceItem(
                        vendor="BE",
                        source_file=path.name,
                        item_name=description,
                        sku=stockcode,
                        gtin=upc,
                        description=description,
                        category=make_category("BE", category),
                        reporting_category="BE",
                        default_unit_cost=cost,
                        vendor_code=stockcode,
                    )
                )

    return items, []


def parse_trident(path: Path) -> tuple[list[SourceItem], list[ReviewIssue]]:
    items: list[SourceItem] = []
    current_section = ""
    current_pack = ""

    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue

            for row in table:
                row = list(row or [])
                row += [""] * (15 - len(row))
                name_cell = clean_text(row[1])
                joined = " ".join(clean_text(cell) for cell in row if clean_text(cell))

                if not name_cell and not joined:
                    continue

                if name_cell in {"Sealers", "Cleaners", "Sand"} or name_cell.startswith("2 Part Sealers"):
                    current_section = name_cell
                    if "5 Gallon Pail" in joined:
                        current_pack = "5 Gallon Pail"
                    elif "4 Gallon Case" in joined:
                        current_pack = "4 Gallon Case (per gallon)"
                    elif "50 lb bag" in joined:
                        current_pack = "50 lb bag"
                    elif "Kits - Dealer" in joined:
                        current_pack = "Kit"
                    continue

                if "Pricing" in joined or "Free Shipping" in joined or name_cell.startswith("$2,500"):
                    continue
                if name_cell.startswith("Sand:"):
                    continue
                if name_cell.startswith("*Full Pallet") or name_cell.startswith("**Mixed Pallet"):
                    continue

                dealer_prices = extract_money_values(row[4])
                direct_price = parse_money(row[7])
                vendor_number = clean_text(row[10])
                product_number = clean_code(row[13])
                if not name_cell or not dealer_prices:
                    continue

                base_name, notes = strip_product_markers(name_cell)
                item_name = f"{base_name} - {current_pack}" if current_pack else base_name
                cost = max(dealer_prices)
                if len(dealer_prices) > 1:
                    notes.append("Default Unit Cost uses the higher dealer tier shown in the source sheet.")

                items.append(
                    SourceItem(
                        vendor="Trident",
                        source_file=path.name,
                        item_name=item_name,
                        sku=product_number,
                        description=build_description(base_name, f"Section: {current_section}", f"Pack: {current_pack}" if current_pack else ""),
                        category=make_category("Trident", current_section),
                        reporting_category="Trident",
                        default_unit_cost=cost,
                        price=direct_price,
                        vendor_code=vendor_number or product_number,
                        notes=notes,
                    )
                )

    return items, []


def append_eaco_item(
    items: list[SourceItem],
    vendor: str,
    source_file: str,
    base_name: str,
    section: str,
    size_label: str,
    prices: list[Decimal],
    notes: list[str] | None = None,
) -> None:
    if not prices:
        return
    if len(prices) >= 3:
        cost = max(prices[:2])
    else:
        cost = max(prices)
    price = prices[2] if len(prices) >= 3 else None
    item_name = f"{base_name} - {size_label}"
    items.append(
        SourceItem(
            vendor=vendor,
            source_file=source_file,
            item_name=item_name,
            description=build_description(base_name, f"Section: {section}", f"Pack: {size_label}"),
            category=make_category(vendor, section),
            reporting_category=vendor,
            default_unit_cost=cost,
            price=price,
            notes=list(notes or []),
        )
    )


def parse_eaco_new_construction(path: Path) -> tuple[list[SourceItem], list[ReviewIssue]]:
    items: list[SourceItem] = []
    issues: list[ReviewIssue] = []
    current_section = "New Construction & Restoration"

    with pdfplumber.open(path) as pdf:
        table = pdf.pages[0].extract_table()
        if not table:
            issues.append(
                ReviewIssue(
                    issue_type="parse_failure",
                    vendor="EacoChem",
                    source_file=path.name,
                    details="Could not extract table from the EacoChem new construction PDF.",
                )
            )
            return items, issues

        for row in table[2:]:
            row = list(row or [])
            row += [""] * (10 - len(row))
            name_cell = clean_text(row[0])
            if not name_cell:
                continue
            if name_cell == "Sealers":
                current_section = "Sealers"
                continue

            base_name, notes = strip_product_markers(name_cell)
            drum_prices = extract_money_values(" ".join(clean_text(cell) for cell in row[1:4]))
            pail_prices = extract_money_values(" ".join(clean_text(cell) for cell in row[4:7]))
            gallon_prices = extract_money_values(" ".join(clean_text(cell) for cell in row[7:10]))
            gallon_label = "2 Gallon" if "2Gal" in " ".join(clean_text(cell) for cell in row[7:10]) else "1 Gallon"

            if drum_prices:
                append_eaco_item(items, "EacoChem", path.name, base_name, current_section, "55 Gallon Drum", drum_prices, notes)
            if pail_prices:
                append_eaco_item(items, "EacoChem", path.name, base_name, current_section, "5 Gallon Pail", pail_prices, notes)
            if gallon_prices:
                append_eaco_item(items, "EacoChem", path.name, base_name, current_section, gallon_label, gallon_prices, notes)

            if not drum_prices and not pail_prices and not gallon_prices:
                issues.append(
                    ReviewIssue(
                        issue_type="ambiguous_source_row",
                        vendor="EacoChem",
                        source_file=path.name,
                        item_name=base_name,
                        category=current_section,
                        details="No usable prices were extracted from this EacoChem row.",
                    )
                )

    return items, issues


def parse_eaco_fleet(path: Path) -> tuple[list[SourceItem], list[ReviewIssue]]:
    items: list[SourceItem] = []
    issues: list[ReviewIssue] = []
    reader = PdfReader(str(path))
    text = reader.pages[0].extract_text() or ""
    capture = False

    for raw_line in text.splitlines():
        line = clean_text(raw_line)
        if not line:
            continue
        if "per Case" in line:
            capture = True
            continue
        if not capture:
            continue
        if line.startswith("***GLORY") or line.startswith("Rev:") or "EaCo Chem" in line:
            break

        prices = extract_money_values(line)
        if not prices:
            continue

        first_price_match = MONEY_RE.search(line)
        if not first_price_match:
            continue

        name_text = clean_text(line[: first_price_match.start()])
        base_name, notes = strip_product_markers(name_text)

        if len(prices) >= 8:
            append_eaco_item(items, "EacoChem", path.name, base_name, "Fleet Wash Products", "55 Gallon Drum", prices[0:3], notes)
            append_eaco_item(items, "EacoChem", path.name, base_name, "Fleet Wash Products", "5 Gallon Pail", prices[3:6], notes)
            append_eaco_item(
                items,
                "EacoChem",
                path.name,
                base_name,
                "Fleet Wash Products",
                "1 Gallon",
                [prices[6], prices[6], prices[7]],
                notes,
            )
        elif len(prices) == 6:
            append_eaco_item(items, "EacoChem", path.name, base_name, "Fleet Wash Products", "55 Gallon Drum", prices[0:3], notes)
            append_eaco_item(items, "EacoChem", path.name, base_name, "Fleet Wash Products", "5 Gallon Pail", prices[3:6], notes)
        elif len(prices) == 1:
            items.append(
                SourceItem(
                    vendor="EacoChem",
                    source_file=path.name,
                    item_name=base_name,
                    description=build_description(base_name, "Section: Fleet Wash Products", "Special pack pricing from source sheet"),
                    category=make_category("EacoChem", "Fleet Wash Products"),
                    reporting_category="EacoChem",
                    default_unit_cost=prices[0],
                    notes=notes + ["This source row only provided one price; selling price was left blank."],
                )
            )
        else:
            issues.append(
                ReviewIssue(
                    issue_type="ambiguous_source_row",
                    vendor="EacoChem",
                    source_file=path.name,
                    item_name=base_name,
                    details=f"Unexpected number of price columns ({len(prices)}) in fleet sheet line: {line}",
                )
            )

    return items, issues


def dedupe_same_source(items: list[SourceItem]) -> tuple[list[SourceItem], int]:
    seen: set[tuple[str, ...]] = set()
    kept: list[SourceItem] = []
    skipped = 0
    for item in items:
        key = (
            item.source_file,
            normalize_sku(item.sku),
            valid_gtin(item.gtin),
            normalize_name(item.item_name),
            format_money(item.default_unit_cost),
            format_money(item.price),
        )
        if key in seen:
            skipped += 1
            continue
        seen.add(key)
        kept.append(item)
    return kept, skipped


def item_priority_key(item: SourceItem) -> tuple[int, Decimal, int, Decimal, int, int]:
    return (
        1 if item.default_unit_cost is not None else 0,
        item.default_unit_cost or Decimal("-1"),
        1 if item.price is not None else 0,
        item.price or Decimal("-1"),
        1 if valid_gtin(item.gtin) else 0,
        1 if normalize_sku(item.sku) else 0,
    )


def merge_group_items(group_items: list[SourceItem]) -> SourceItem:
    best_item = max(group_items, key=item_priority_key)
    merged_item = replace(best_item, notes=list(best_item.notes))

    available_costs = [item.default_unit_cost for item in group_items if item.default_unit_cost is not None]
    available_prices = [item.price for item in group_items if item.price is not None]
    highest_cost = max(available_costs) if available_costs else None
    prices_at_highest_cost = [
        item.price
        for item in group_items
        if item.default_unit_cost == highest_cost and item.price is not None
    ]

    merged_item.default_unit_cost = highest_cost
    if prices_at_highest_cost:
        merged_item.price = max(prices_at_highest_cost)
    elif merged_item.price is None and available_prices:
        merged_item.price = max(available_prices)

    if not valid_gtin(merged_item.gtin):
        merged_item.gtin = next((item.gtin for item in group_items if valid_gtin(item.gtin)), "")
    if not normalize_sku(merged_item.sku):
        merged_item.sku = next((item.sku for item in group_items if normalize_sku(item.sku)), "")
    if not clean_text(merged_item.vendor_code):
        merged_item.vendor_code = next((item.vendor_code for item in group_items if clean_text(item.vendor_code)), merged_item.sku)
    if not clean_text(merged_item.description):
        merged_item.description = max((item.description for item in group_items), key=len, default="")
    if not clean_text(merged_item.category):
        merged_item.category = next((item.category for item in group_items if clean_text(item.category)), "")
    if not clean_text(merged_item.reporting_category):
        merged_item.reporting_category = next((item.reporting_category for item in group_items if clean_text(item.reporting_category)), "")

    return merged_item


def extract_case_pack(description: str) -> str:
    match = re.search(r"Case pack:\s*([^|]+)", description)
    return clean_text(match.group(1)) if match else ""


def disambiguation_label(item: SourceItem) -> str:
    case_pack = extract_case_pack(item.description)
    if case_pack:
        return f"Case {case_pack}"
    gtin = valid_gtin(item.gtin)
    if gtin:
        return f"GTIN {gtin}"
    sku = clean_text(item.sku)
    if sku:
        return f"SKU {sku}"
    return clean_text(item.source_file)


def merge_duplicate_items(items: list[SourceItem]) -> tuple[list[SourceItem], list[ReviewIssue], int]:
    parent = list(range(len(items)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    def union_groups(groups: dict[str, list[int]]) -> None:
        for indexes in groups.values():
            unique_indexes = sorted(set(indexes))
            if len(unique_indexes) <= 1:
                continue
            first = unique_indexes[0]
            for index in unique_indexes[1:]:
                union(first, index)

    def union_name_groups(groups: dict[str, list[int]]) -> None:
        for indexes in groups.values():
            unique_indexes = sorted(set(indexes))
            if len(unique_indexes) <= 1:
                continue
            for left_pos, left in enumerate(unique_indexes):
                for right in unique_indexes[left_pos + 1 :]:
                    if items[left].vendor != items[right].vendor:
                        union(left, right)

    sku_groups: dict[str, list[int]] = defaultdict(list)
    gtin_groups: dict[str, list[int]] = defaultdict(list)
    name_groups: dict[str, list[int]] = defaultdict(list)

    for index, item in enumerate(items):
        sku_key = normalize_sku(item.sku)
        if sku_key:
            sku_groups[f"{item.vendor}|{sku_key}"].append(index)
        gtin_key = valid_gtin(item.gtin)
        if gtin_key:
            gtin_groups[gtin_key].append(index)
        name_key = normalize_name(item.item_name)
        if name_key and len(name_key) >= 8:
            name_groups[name_key].append(index)

    union_groups(sku_groups)
    union_groups(gtin_groups)
    union_name_groups(name_groups)

    grouped_indexes: dict[int, list[int]] = defaultdict(list)
    for index in range(len(items)):
        grouped_indexes[find(index)].append(index)

    merged_items: list[SourceItem] = []
    merge_issues: list[ReviewIssue] = []
    merged_groups = 0

    for root in sorted(grouped_indexes):
        indexes = grouped_indexes[root]
        group_items = [items[index] for index in indexes]
        if len(group_items) == 1:
            merged_items.append(group_items[0])
            continue

        merged_groups += 1
        merged_item = merge_group_items(group_items)
        highest_cost = merged_item.default_unit_cost

        reason_parts: list[str] = []
        gtin_matches = {valid_gtin(item.gtin) for item in group_items if valid_gtin(item.gtin)}
        name_matches = {normalize_name(item.item_name) for item in group_items if normalize_name(item.item_name)}
        if len(gtin_matches) == 1 and next(iter(gtin_matches), ""):
            reason_parts.append("exact GTIN match")
        if len(name_matches) == 1 and next(iter(name_matches), ""):
            reason_parts.append("exact item-name match")
        vendor_sku_pairs = {(item.vendor, normalize_sku(item.sku)) for item in group_items if normalize_sku(item.sku)}
        if len(vendor_sku_pairs) < len([item for item in group_items if normalize_sku(item.sku)]):
            reason_parts.append("same-vendor SKU duplicate")

        source_list = "; ".join(
            f"{item.vendor} | {item.source_file} | SKU={item.sku or '[generated]'} | Cost={format_money(item.default_unit_cost)}"
            for item in sorted(group_items, key=lambda item: (item.vendor, item.source_file, item.sku, item.item_name))
        )
        reason_text = ", ".join(reason_parts) if reason_parts else "duplicate item match"
        merge_issues.append(
            ReviewIssue(
                issue_type="merged_duplicate",
                vendor=merged_item.vendor,
                source_file=merged_item.source_file,
                item_name=merged_item.item_name,
                sku=merged_item.sku,
                gtin=merged_item.gtin,
                category=merged_item.category,
                default_unit_cost=format_money(merged_item.default_unit_cost),
                price=format_money(merged_item.price),
                details=f"Merged {len(group_items)} source rows by {reason_text}. Kept the highest cost option {format_money(highest_cost)}. Sources: {source_list}",
            )
        )
        merged_items.append(merged_item)

    return merged_items, merge_issues, merged_groups


def resolve_same_vendor_name_collisions(items: list[SourceItem]) -> tuple[list[SourceItem], list[ReviewIssue], int, int]:
    groups: dict[tuple[str, str], list[SourceItem]] = defaultdict(list)
    for item in items:
        groups[(item.vendor, normalize_name(item.item_name))].append(item)

    resolved_items: list[SourceItem] = []
    review_issues: list[ReviewIssue] = []
    merged_groups = 0
    renamed_rows = 0

    for (vendor, name_key), group_items in groups.items():
        if not name_key or len(group_items) == 1:
            resolved_items.extend(group_items)
            continue

        costs = [item.default_unit_cost for item in group_items if item.default_unit_cost is not None]
        safe_to_merge = False
        if costs:
            min_cost = min(costs)
            max_cost = max(costs)
            safe_to_merge = bool(min_cost) and max_cost <= min_cost * Decimal("1.5")
        gtins = {valid_gtin(item.gtin) for item in group_items if valid_gtin(item.gtin)}
        if len(gtins) == 1 and next(iter(gtins), ""):
            safe_to_merge = True

        if safe_to_merge:
            merged_groups += 1
            merged_item = merge_group_items(group_items)
            resolved_items.append(merged_item)
            review_issues.append(
                ReviewIssue(
                    issue_type="merged_same_vendor_duplicate",
                    vendor=merged_item.vendor,
                    source_file=merged_item.source_file,
                    item_name=merged_item.item_name,
                    sku=merged_item.sku,
                    gtin=merged_item.gtin,
                    category=merged_item.category,
                    default_unit_cost=format_money(merged_item.default_unit_cost),
                    price=format_money(merged_item.price),
                    details="Merged same-vendor duplicate names and kept the higher cost option. Sources: "
                    + "; ".join(
                        f"SKU={item.sku or '[generated]'} | Cost={format_money(item.default_unit_cost)}"
                        for item in sorted(group_items, key=lambda item: (item.sku, item.item_name))
                    ),
                )
            )
            continue

        used_labels: set[str] = set()
        for item in group_items:
            renamed_item = replace(item, notes=list(item.notes))
            label = disambiguation_label(renamed_item)
            candidate = label
            suffix = 2
            while candidate in used_labels:
                candidate = f"{label} #{suffix}"
                suffix += 1
            used_labels.add(candidate)
            renamed_item.item_name = f"{clean_text(renamed_item.item_name)} [{candidate}]"
            resolved_items.append(renamed_item)
            renamed_rows += 1

        review_issues.append(
            ReviewIssue(
                issue_type="renamed_same_vendor_duplicate",
                vendor=vendor,
                source_file=group_items[0].source_file,
                item_name=group_items[0].item_name,
                category=group_items[0].category,
                details="Kept separate items with the same vendor/name by renaming them using case pack, GTIN, or SKU.",
            )
        )

    return resolved_items, review_issues, merged_groups, renamed_rows


def generate_unique_skus(items: list[SourceItem]) -> int:
    used: set[str] = set()
    updated = 0

    for item in items:
        sku = clean_code(item.sku)
        if sku and sku not in used:
            item.sku = sku
            used.add(sku)
            continue

        prefix = re.sub(r"[^A-Z0-9]+", "", item.vendor.upper())[:6] or "ITEM"
        base = re.sub(r"[^A-Z0-9]+", "-", clean_text(item.item_name).upper()).strip("-")
        base = re.sub(r"-{2,}", "-", base)
        base = base[:32].strip("-") or "PRODUCT"

        counter = 1
        candidate = f"{prefix}-{base}"
        while candidate in used:
            counter += 1
            suffix = f"-{counter}"
            shortened = base[: max(8, 32 - len(suffix))].rstrip("-") or "PRODUCT"
            candidate = f"{prefix}-{shortened}{suffix}"

        item.sku = candidate
        item.generated_sku = True
        used.add(candidate)
        updated += 1

    return updated


def build_square_row(item: SourceItem, fieldnames: list[str]) -> dict[str, str]:
    row = {field: "" for field in fieldnames}
    row["Token"] = ""
    row["Item Name"] = clean_text(item.item_name)
    row["Customer-facing Name"] = clean_text(item.item_name)
    row["Variation Name"] = "Regular"
    row["SKU"] = item.sku
    row["Description"] = clean_text(item.description)
    row["Categories"] = clean_text(item.category)
    row["Reporting Category"] = clean_text(item.reporting_category)
    row["GTIN"] = valid_gtin(item.gtin)
    row["Square Online Item Visibility"] = "Hidden"
    row["Item Type"] = "Physical"
    row["Shipping Enabled"] = "N"
    row["Self-serve Ordering Enabled"] = "N"
    row["Delivery Enabled"] = "N"
    row["Pickup Enabled"] = "N"
    row["Price"] = format_money(item.price)
    row["Archived"] = "N"
    row["Sellable"] = "Y" if item.price is not None else "N"
    row["Contains Alcohol"] = "N"
    row["Stockable"] = "Y"
    row["Skip Detail Screen in POS"] = "N"
    row["Default Unit Cost"] = format_money(item.default_unit_cost)
    row["Default Vendor Name"] = item.vendor
    row["Default Vendor Code"] = clean_text(item.vendor_code or item.sku)
    return row


def write_master_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_review_rows(review_issues: list[ReviewIssue]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    for issue in review_issues:
        rows.append(
            {
                "issue_type": issue.issue_type,
                "vendor": issue.vendor,
                "source_file": issue.source_file,
                "item_name": clean_text(issue.item_name),
                "sku": clean_text(issue.sku),
                "gtin": valid_gtin(issue.gtin),
                "category": clean_text(issue.category),
                "default_unit_cost": issue.default_unit_cost,
                "price": issue.price,
                "details": clean_text(issue.details),
            }
        )

    return rows


def write_review_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "issue_type",
        "vendor",
        "source_file",
        "item_name",
        "sku",
        "gtin",
        "category",
        "default_unit_cost",
        "price",
        "details",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def summarize(
    counts_by_vendor: Counter[str],
    total_source_items: int,
    review_rows: list[dict[str, str]],
    included_rows: int,
    generated_skus: int,
    skipped_duplicates: int,
    merged_groups: int,
    renamed_rows: int,
) -> str:
    lines = [
        f"Square master inventory: {MASTER_OUT_PATH}",
        f"Overlap review file: {REVIEW_OUT_PATH}",
        f"Source items normalized: {total_source_items}",
        f"Rows included in Square import: {included_rows}",
        f"Rows sent to review: {len(review_rows)}",
        f"Duplicate item groups merged into one inventory row: {merged_groups}",
        f"Rows renamed to avoid duplicate item names: {renamed_rows}",
        f"Generated replacement SKUs: {generated_skus}",
        f"Duplicate rows skipped inside the same source file: {skipped_duplicates}",
        "Counts by vendor:",
    ]
    for vendor in sorted(counts_by_vendor):
        lines.append(f"  {vendor}: {counts_by_vendor[vendor]}")
    lines.append("Notes:")
    lines.append("  - Default Unit Cost uses dealer/distributor pricing when available.")
    lines.append("  - Price uses list/direct/retail pricing when the source file provided it.")
    lines.append("  - Items without a selling price were imported as Stockable=Y and Sellable=N.")
    lines.append("  - EacoChem Price List.pdf was not used because it duplicates the cleaner EacoChem source sheets.")
    return "\n".join(lines)


def main() -> None:
    square_headers = load_square_headers(TEMPLATE_PATH)

    source_items: list[SourceItem] = []
    parser_issues: list[ReviewIssue] = []

    source_parsers = [
        (BASE_DIR / "Barrens Pricelist 2025.csv", parse_barrens),
        (BASE_DIR / "MPWSR Price List May '25.csv", parse_mpwsr),
        (BASE_DIR / "2025 Dealer Pricing .xlsx", parse_inseco),
        (BASE_DIR / "2026 Price List - Distributors.xlsx", parse_jracenstein),
        (BASE_DIR / "BE PriceList 2025- USD.pdf", parse_be),
        (BASE_DIR / "Trident Dealer Price Sheet 2025.pdf", parse_trident),
        (BASE_DIR / "2025 Distributor New Construction and Restoration Products Pricing.pdf", parse_eaco_new_construction),
        (BASE_DIR / "2025 Distr Fleet Distributor Fleet Distributor.pdf", parse_eaco_fleet),
    ]

    for path, parser in source_parsers:
        items, issues = parser(path)
        source_items.extend(items)
        parser_issues.extend(issues)

    source_items, skipped_duplicates = dedupe_same_source(source_items)
    source_items, merge_issues, merged_groups = merge_duplicate_items(source_items)
    source_items, rename_issues, same_vendor_merged_groups, renamed_rows = resolve_same_vendor_name_collisions(source_items)
    counts_by_vendor = Counter(item.vendor for item in source_items)
    generated_skus = generate_unique_skus(source_items)

    master_rows = [build_square_row(item, square_headers) for item in source_items]
    review_rows = build_review_rows(merge_issues + rename_issues + parser_issues)

    write_master_csv(MASTER_OUT_PATH, square_headers, master_rows)
    write_review_csv(REVIEW_OUT_PATH, review_rows)

    summary = summarize(
        counts_by_vendor=counts_by_vendor,
        total_source_items=len(source_items),
        review_rows=review_rows,
        included_rows=len(master_rows),
        generated_skus=generated_skus,
        skipped_duplicates=skipped_duplicates,
        merged_groups=merged_groups + same_vendor_merged_groups,
        renamed_rows=renamed_rows,
    )
    SUMMARY_OUT_PATH.write_text(summary, encoding="utf-8")
    print(summary)


if __name__ == "__main__":
    main()
