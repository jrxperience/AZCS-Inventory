from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
MASTER_PATH = BASE_DIR / "outputs" / "square_master_inventory.csv"
EXPORT_PATH = Path(r"C:\Users\JRAZC\Downloads\MLT3E97CHP443_catalog-2026-03-20-2210.csv")
TEMPLATE_PATH = Path(r"C:\Users\JRAZC\Downloads\MLT3E97CHP443_catalog-2026-03-20-2039.csv")
TO_IMPORT_DIR = BASE_DIR / "to_import"
PACKAGE_DIR = BASE_DIR / "square_ready" / "BASELINE_2026-03-20_1543"
OUTPUT_NAME = "BASELINE_SQUARE_INVENTORY_IMPORT_2026-03-20_1543.csv"
ARCHIVE_NAME = "BASELINE_DUPLICATE_ARCHIVE_2026-03-20_1543.csv"
CATEGORY_PLAN_NAME = "BASELINE_CATEGORY_PLAN_2026-03-20_1543.csv"
SUMMARY_NAME = "BASELINE_SUMMARY_2026-03-20_1543.txt"
README_NAME = "README.txt"
PRIMARY_UPLOAD_NAME = "UPLOAD_THIS_TO_SQUARE.csv"
RANGE_SIZE = 200
SEMANTIC_FLAT_VENDORS = {"Barrens", "MPWSR"}

LOCATION_PREFIXES = (
    "Enabled ",
    "Current Quantity ",
    "New Quantity ",
    "Stock Alert Enabled ",
    "Stock Alert Count ",
    "Price ",
)

VENDOR_ALIASES = {
    "BARENS": "BARRENS",
    "EACOCHEM": "EACOCHEM",
    "EACOCHEMINC.": "EACOCHEM",
    "EACOCHEMINC": "EACOCHEM",
    "FRONT9": "FRONT9",
    "INSECO": "INSECO",
    "J.RACENSTEIN": "JRACENSTEIN",
    "JRACENSTEIN": "JRACENSTEIN",
    "TUCKER": "TUCKER",
    "EBC": "ENVIROBIOCLEANER",
}


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).replace("\xa0", " ").strip()


def read_csv_rows(path: Path) -> list[list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [list(row) for row in csv.reader(handle)]


def find_header_index(rows: list[list[str]], required: str) -> int:
    for index, row in enumerate(rows):
        if required in row:
            return index
    raise ValueError(f"Could not find a header row containing {required!r} in {path_display(rows)}.")


def path_display(rows: list[list[str]]) -> str:
    return f"{len(rows)} rows"


def rows_to_dicts(headers: list[str], rows: list[list[str]]) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for row in rows:
        if not any(clean_text(cell) for cell in row):
            continue
        padded = row[: len(headers)] + [""] * max(0, len(headers) - len(row))
        records.append({headers[index]: clean_text(padded[index]) for index in range(len(headers))})
    return records


def read_records(path: Path) -> tuple[list[list[str]], int, list[str], list[dict[str, str]]]:
    rows = read_csv_rows(path)
    header_index = find_header_index(rows, "SKU")
    headers = [clean_text(cell) for cell in rows[header_index]]
    records = rows_to_dicts(headers, rows[header_index + 1 :])
    return rows, header_index, headers, records


def normalize_sku(value: str) -> str:
    return clean_text(value).upper()


def normalize_name(value: str) -> str:
    return " ".join(clean_text(value).upper().split())


def parse_categories(value: str) -> list[str]:
    return [segment for segment in (clean_text(part) for part in clean_text(value).split(",")) if segment]


def deepest_category(value: str) -> str:
    categories = parse_categories(value)
    if not categories:
        return ""
    return max(categories, key=lambda item: (item.count(">"), len(item)))


def first_category_root(value: str) -> str:
    categories = parse_categories(value)
    if not categories:
        return ""
    root = categories[0].split(">")[0]
    return clean_text(root)


def vendor_for_record(record: dict[str, str]) -> str:
    vendor = clean_text(record.get("Default Vendor Name"))
    if vendor:
        return vendor
    reporting_category = clean_text(record.get("Reporting Category"))
    if reporting_category:
        return reporting_category
    category_root = first_category_root(record.get("Categories", ""))
    if category_root:
        return category_root
    return "Uncategorized"


def normalize_vendor(value: str) -> str:
    cleaned = normalize_name(value).replace(" ", "")
    return VENDOR_ALIASES.get(cleaned, cleaned)


def record_text(record: dict[str, str]) -> str:
    return f"{clean_text(record.get('Item Name'))} {clean_text(record.get('Description'))}".upper()


def has_any(text: str, *parts: str) -> bool:
    return any(part in text for part in parts)


def looks_like_complete_pump(text: str) -> bool:
    blocked_parts = (
        " REPAIR KIT",
        " REBUILD KIT",
        " SERVICE KIT",
        " SEAL",
        " O-RING",
        " ORING",
        " PACKING",
        " VALVE KIT",
        " PLUNGER",
        " MANIFOLD",
        " PISTON",
        " CHECK VALVE",
        " GUN",
        " HOSE",
        " NOZZLE",
        " SKID",
        " TRAILER",
        " WASHER",
    )
    if any(part in text for part in blocked_parts):
        return False
    if "GPM" not in text or "PSI" not in text:
        return False
    return True


def infer_semantic_flat_vendor_category(vendor: str, record: dict[str, str]) -> str:
    text = record_text(record)

    if has_any(
        text,
        "SURFACE CLEANER",
        "SURFACE BROOM",
        "WHIRLAWAY",
        "HURRICANE",
        "BEAST",
        "MOSMATIC",
        "FLIPPER",
        "UNDERCARRIAGE",
        "ROTARY ARM",
        "SPRAY ARM",
        "FSC",
        "DIRT KILLER",
        "HAMMERHEAD",
        "BARRACUDA",
        "PIRANHA",
        "ROTO EZE",
        "ULTRA CLEAN",
        "GROUND FORCE",
        "MAXIMA",
        "BIG GUY",
    ):
        return "Surface Cleaners & Parts"

    if has_any(text, "SOFTWASH", "SOFT WASH", "FOAM CANNON", "FOAMER", "FOAM-JET", "XJET", "PROPORTIONER", "DOWNSTREAMER"):
        return "Soft Wash & Foam"

    if has_any(
        text,
        "TRUCK MOUNT",
        "SKID",
        "TRAILER",
        "PRESSURE WASHER",
        "WASHER SKID",
        "SOFTWASH SYSTEM",
        "SOFT WASH SYSTEM",
        "BANDIT",
        "RAMPAGE",
        "WHISPER WASH",
        "CART",
        "DELCO CWE",
    ):
        return "Pressure Washer Systems & Skids"

    if has_any(
        text,
        "HOSE REEL",
        "REEL SWIVEL",
        "HAND CRANK",
        "STACK KIT FOR REEL",
        "STACKING KIT",
        "REEL GUIDE",
        "DHRA REEL",
        "A-FRAME REEL",
        "REEL SIDE PANEL",
        "ALUMINUM REEL",
        "TITAN REEL",
        "COXREEL",
    ):
        return "Hose Reels & Parts"

    if has_any(
        text,
        "LANCE",
        " WAND",
        "WANDS",
        "WAND PIPE",
        "TRIGGER GUN",
        "SPRAY GUN",
        "DUMP GUN",
        "HAND GUN",
        "OPEN GUN",
        "TRIGGERJET",
        "TELESCOPIC EXTENSION",
        "NOZZLE HOLDER",
    ):
        return "Guns & Wands"

    if has_any(
        text,
        "NOZZLE",
        "SPRAY TIP",
        "SPRAY TIPS",
        "TURBO",
        "ROTOJET",
        "VEEJET",
        "QC-MEG",
        "SOLID CONE",
        "HOLLOW CONE",
        "SOAP TIP",
        "INJECTOR",
        "ORIFICE",
        "SHOOTER TIP",
        "SANDBLASTER",
    ):
        return "Nozzles & Injectors"

    if has_any(
        text,
        "HOSE",
        "POLYBRAID",
        "CLEARBRAID",
        "WHIP LINE",
        "JUMPER LINE",
        "BRAIDED HOSE",
        "HOSE FERRULE",
        "HOSE SPLICER",
        "GARDEN HOSE",
        "PONASPRAY",
    ):
        return "Hoses"

    if has_any(text, " TANK", "FLOAT TANK", "FLOAT VALVE", "METERING VALVE", "LID", "STRAP SET", "BULKHEAD", "BATTERY BOX", "LEGTANK"):
        return "Tanks & Chemical Delivery"

    if has_any(
        text,
        "BRIGHTENER",
        "SEALER",
        "CLEAR SEAL",
        "WATER SHIELD",
        "URE SEAL",
        "URE SHADES",
        "COBBLE",
        "BRICK RESTORATION",
        "SOLVENT BASE",
        "CONCENTRATE",
        "TAGAWAY",
        "TAGINATOR",
        "DEGREASER",
        "CLEAN UP",
        "OXALIC ACID",
        "TEXTURE EZE",
        "POWER BRITE",
        "STONE SHOW",
        "CRYSTAL CLEAR H20",
        "WATER SHADES",
        "H2COOL",
        "RED DEVIL SOOT REMOVER",
        "TOTAL PREP",
        "TOTAL ETCH",
        "TOTAL WASH",
        "QUICK STRIP",
        "RUST ERASER",
        "ASSASSIN",
        "PINK STUFF",
        "WHITEWATER EFFLO",
        "TIDALWAVE",
        "POINT BREAK",
        "SEAWALL",
        "NEUTRAPODS",
        "NUTOPP",
        "UPCHARGE",
        "ALUMINUM BRIGHTENER",
    ):
        return "Chemicals & Sealers"

    if looks_like_complete_pump(text) or has_any(
        text,
        "TS2021",
        "TS1621",
        "TT941",
        "XWA-",
        "XW-M",
        "XMV",
        "FW2-",
        "FWS2-",
        "EZ2536",
        "EZ2542",
        "T9281",
        "T9051",
        "TT9061",
        "LWD2020E",
        "LWS2020E",
        "P217RX",
        "P218RX",
        "P219RX",
        "P321R",
        "P316RX",
        "P317RX",
        "P318R",
        "P56W",
        "TW 8030S",
        "TW 5050S",
        "RQW22G26",
        "TSP1619",
        "TSS1021",
        "RRA4G30N",
        "7CP6165CSG118",
    ):
        return "Complete Pumps"

    if has_any(
        text,
        "HONDA",
        "BRIGGS",
        "VANGUARD",
        "PREDATOR",
        "KOHLER",
        "GX390",
        "GX340",
        "GX270",
        "GX690",
        "GX240",
        "GX200",
        "GX160",
        "IGX800",
        "AIR CLEANER",
        "MUFFLER",
        "SPARK PLUG",
        "CARB",
        "CARBURETOR",
        "GOVERNOR",
        "ROCKER",
        "CAMSHAFT",
        "ENGINE",
        "STARTER",
        "THROTTLE CONTROL",
        "CHOKE LEVER",
        "FUEL LINE",
        "FUEL TUBE",
        "OIL COOLER",
        "RECOIL START",
        "DIP STICK GX",
    ):
        return "Engines & Engine Parts"

    if has_any(
        text,
        "SWITCH",
        "RELAY",
        "THERMOSTAT",
        "BLOWER",
        "ELECTRODE",
        "INSULATOR",
        "BUSS BAR",
        "SOLENOID",
        "PIGTAIL",
        "IGNITION",
        "RECTIFIER",
        "ECU",
        "HOUR METER",
        "METER ASSY",
        "FAN COVER",
        "FAN HOUSING",
        "BURNER",
        "COIL WRAP",
        "IGNITOR",
        "BATTERY CABLE",
        "BATTERY BOX",
        "CABLE LUG",
    ) or (" GAUGE" in text or text.startswith("GAUGE ")):
        return "Electrical & Burner Controls"

    if has_any(
        text,
        "UNLOADER",
        "RELIEF VALVE",
        "THERMAL RELIEF",
        "FLOW SWITCH",
        "PRESSURE RELIEF",
        "REGULATOR",
        "POP OFF",
        "PILOT ASSIST",
        "BYPASS",
        "PRESSURE SWITCH",
        "SHUTOFF VALVE",
        "DRAIN VALVE",
        "BALL VALVE",
        "BACKFLOW PREVENTER",
        "EASY START VALVE",
        "PRIME VALVE",
        "RUPTURE DISK",
        "ACCUMULATOR",
    ):
        return "Unloaders & Flow Control"

    if has_any(
        text,
        "FILTER",
        "STRAINER",
        "MESH",
        "SCREEN",
        "ROCK CATCHER",
        "SEPERATOR",
        "SEPARATOR",
        "SITE GLASS",
        "SIGHT GLASS",
        "BREATHER VENT",
        "AIR CLEANER COVER",
    ):
        return "Filters & Strainers"

    if has_any(
        text,
        "CAMLOCK",
        "COUPLER",
        "PLUG",
        "BUSHING",
        "NIPPLE",
        "ADAPTER",
        "ELBOW",
        " TEE",
        "TEE ",
        "UNION",
        "SWIVEL",
        "BARB",
        " MPT",
        " FPT",
        " JIC",
        " ORB",
        " BSPP",
        " BSP",
        "SOCKET",
        " CAP",
        "CAP ",
        "REDUCER",
        "FITTING",
        "COUPLING",
        "FLANGE",
        "BUTRESS",
        "MICRO VALVE",
        "POLY PIPE CROSS",
    ):
        return "Fittings & Adapters"

    if has_any(
        text,
        "SEAL",
        "O-RING",
        "ORING",
        "GASKET",
        "PACKING",
        "DIAPHRAGM",
        "VALVE KIT",
        "VALVE ASSY",
        "SEAT",
        "PLUNGER",
        "CERAMIC",
        "CRANKCASE",
        "MANIFOLD",
        "CHECK VALVE",
        "HEADRING",
        "REPAIR KIT",
        "REBUILD KIT",
        "SERVICE KIT",
        "VALVE SPRING",
        "PISTON",
        "GUIDING PISTON",
        "CONNECTING ROD",
        "SLEEVE",
        "QUAD RING",
        "DIPSTICK",
        "DIP STICK",
        "INTERMEDIATE RING",
        "BACK-UP RING",
        "BARRIER SLINGER",
        "RING NUT",
        "RING (",
    ):
        return "Pump Repair Parts"

    if has_any(
        text,
        "BELT",
        "SHEAVE",
        "PULLEY",
        "BEARING",
        "BRAKE",
        "LOCK PIN",
        "BRACKET",
        "GUARD",
        "PANEL",
        "BASE",
        "FRAME",
        "WHEEL",
        "CASTER",
        "ROLLER",
        "RAIL KIT",
        "SPROCKET",
        "KEYWAY",
        "CROSS BRACE",
        "PICK TOOL",
        "THREADLOCK",
        "TEFLON TAPE",
        "CABLE TIE",
        "LUG",
        "RATCHET STRAP",
        "TOOL",
        "HANDLE",
        "BRUSH",
        "KNOB",
        "WICK",
        "SHIM",
        "SPRING",
        "BALL ",
        "RETAINER RING",
        "ANTI-EXT. RING",
        "LONG LIFE RING",
        "WASHER",
        "NUT",
        "BOLT",
        " PIN",
        "PIN ",
        "CLAMP",
    ):
        return "Drive Components & Hardware"

    if "PUMP" in text:
        return "Pump Repair Parts"

    return "Accessories & Misc"


def base_category_path(record: dict[str, str], vendor: str) -> str:
    deepest = deepest_category(record.get("Categories", ""))
    if vendor in SEMANTIC_FLAT_VENDORS and (not deepest or deepest == vendor):
        return f"{vendor} > {infer_semantic_flat_vendor_category(vendor, record)}"
    if deepest:
        if deepest == vendor or deepest.startswith(f"{vendor} >"):
            return deepest
        if vendor:
            return f"{vendor} > {deepest}"
        return deepest
    return vendor or "Uncategorized"


def unique_nonempty(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = clean_text(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def fill_score(record: dict[str, str]) -> tuple[int, int, int, str]:
    nonempty = sum(1 for value in record.values() if clean_text(value))
    not_archived = 1 if clean_text(record.get("Archived")).upper() != "Y" else 0
    has_category = 1 if clean_text(record.get("Categories")) else 0
    token = clean_text(record.get("Token"))
    return (not_archived, has_category, nonempty, token)


def pick_canonical_export_row(group: list[dict[str, str]]) -> dict[str, str]:
    return max(group, key=fill_score)


def is_location_field(header: str) -> bool:
    return any(header.startswith(prefix) for prefix in LOCATION_PREFIXES)


def merge_master_and_export(
    master_row: dict[str, str],
    export_row: dict[str, str] | None,
    template_headers: list[str],
) -> dict[str, str]:
    merged: dict[str, str] = {}
    for header in template_headers:
        if header == "Reference Handle":
            merged[header] = ""
            continue
        if header == "Token":
            merged[header] = clean_text(export_row.get(header) if export_row else "")
            continue
        if is_location_field(header):
            merged[header] = clean_text(
                (export_row.get(header) if export_row and clean_text(export_row.get(header)) else master_row.get(header, ""))
            )
            continue
        master_value = clean_text(master_row.get(header, ""))
        export_value = clean_text(export_row.get(header, "")) if export_row else ""
        merged[header] = master_value or export_value
    merged["Archived"] = "N"
    return merged


def clone_to_template(record: dict[str, str], template_headers: list[str]) -> dict[str, str]:
    return {header: clean_text(record.get(header, "")) for header in template_headers}


def sort_key(record: dict[str, str]) -> tuple[str, str, str]:
    return (
        normalize_name(record.get("Item Name", "")),
        normalize_sku(record.get("SKU", "")),
        clean_text(record.get("Token", "")),
    )


def chunk_label(start_index: int, end_index: int) -> str:
    return f"{start_index:03d}-{end_index:03d}"


def assign_categories(records: list[dict[str, str]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    base_metadata: dict[str, tuple[str, str]] = {}
    for record in records:
        vendor = vendor_for_record(record)
        base_path = base_category_path(record, vendor)
        grouped[base_path].append(record)
        base_metadata[base_path] = (vendor, base_path)

    category_plan_rows: list[dict[str, str]] = []
    for base_path in sorted(grouped):
        vendor, resolved_base_path = base_metadata[base_path]
        group_records = sorted(grouped[base_path], key=sort_key)
        if len(group_records) <= RANGE_SIZE:
            for record in group_records:
                record["Categories"] = ", ".join(unique_nonempty([vendor, resolved_base_path]))
                record["Reporting Category"] = vendor
            category_plan_rows.append(
                {
                    "Vendor": vendor,
                    "Base Category": resolved_base_path,
                    "Assigned Category": resolved_base_path,
                    "Items": str(len(group_records)),
                    "First Item": clean_text(group_records[0].get("Item Name")),
                    "Last Item": clean_text(group_records[-1].get("Item Name")),
                }
            )
            continue

        for chunk_start in range(0, len(group_records), RANGE_SIZE):
            chunk = group_records[chunk_start : chunk_start + RANGE_SIZE]
            start_number = chunk_start + 1
            end_number = chunk_start + len(chunk)
            range_path = f"{resolved_base_path} > {chunk_label(start_number, end_number)}"
            for record in chunk:
                record["Categories"] = ", ".join(unique_nonempty([vendor, resolved_base_path, range_path]))
                record["Reporting Category"] = vendor
            category_plan_rows.append(
                {
                    "Vendor": vendor,
                    "Base Category": resolved_base_path,
                    "Assigned Category": range_path,
                    "Items": str(len(chunk)),
                    "First Item": clean_text(chunk[0].get("Item Name")),
                    "Last Item": clean_text(chunk[-1].get("Item Name")),
                }
            )
    return records, category_plan_rows


def write_csv(path: Path, headers: list[str], records: list[dict[str, str]], template_prefix_rows: list[list[str]] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        for row in template_prefix_rows or []:
            writer.writerow(row)
        writer.writerow(headers)
        for record in records:
            writer.writerow([clean_text(record.get(header, "")) for header in headers])


def build_archive_row(record: dict[str, str], template_headers: list[str]) -> dict[str, str]:
    archive_row = clone_to_template(record, template_headers)
    archive_row["Reference Handle"] = ""
    archive_row["Archived"] = "Y"
    archive_row["Square Online Item Visibility"] = "Hidden"
    return archive_row


def main() -> None:
    if not MASTER_PATH.exists():
        raise FileNotFoundError(f"Missing master inventory: {MASTER_PATH}")
    if not EXPORT_PATH.exists():
        raise FileNotFoundError(f"Missing Square export: {EXPORT_PATH}")
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Missing Square template: {TEMPLATE_PATH}")

    template_rows, template_header_index, template_headers, _ = read_records(TEMPLATE_PATH)
    _, _, _, export_records = read_records(EXPORT_PATH)
    _, _, _, master_records = read_records(MASTER_PATH)
    managed_vendors = {
        normalize_vendor(vendor_for_record(record))
        for record in master_records
        if normalize_vendor(vendor_for_record(record)) and normalize_vendor(vendor_for_record(record)) != "UNCATEGORIZED"
    }

    export_by_sku: dict[str, list[dict[str, str]]] = defaultdict(list)
    blank_sku_records: list[dict[str, str]] = []
    for record in export_records:
        sku = normalize_sku(record.get("SKU", ""))
        if not sku:
            if clean_text(record.get("Archived")).upper() != "Y":
                blank_sku_records.append(clone_to_template(record, template_headers))
            continue
        export_by_sku[sku].append(record)

    canonical_export_by_sku: dict[str, dict[str, str]] = {}
    archive_by_token: dict[str, dict[str, str]] = {}
    duplicate_review_rows: list[dict[str, str]] = []
    for sku, group in export_by_sku.items():
        canonical = pick_canonical_export_row(group)
        canonical_export_by_sku[sku] = canonical
        canonical_token = clean_text(canonical.get("Token"))
        for record in group:
            token = clean_text(record.get("Token"))
            if record is canonical:
                continue
            duplicate_review_rows.append(
                {
                    "SKU": sku,
                    "Item Name": clean_text(record.get("Item Name")),
                    "Canonical Token": canonical_token,
                    "Duplicate Token": token,
                    "Vendor": vendor_for_record(record),
                    "Archived In Import": "Y" if token else "N",
                }
            )
            if token:
                archive_by_token[token] = record

    active_rows: list[dict[str, str]] = []
    matched_master_rows = 0
    managed_export_only_archived = 0
    for master_row in master_records:
        sku = normalize_sku(master_row.get("SKU", ""))
        export_row = canonical_export_by_sku.pop(sku, None) if sku else None
        if export_row:
            matched_master_rows += 1
        active_rows.append(merge_master_and_export(master_row, export_row, template_headers))

    export_only_rows = 0
    blank_sku_rows_kept = 0
    for sku in sorted(canonical_export_by_sku):
        record = clone_to_template(canonical_export_by_sku[sku], template_headers)
        record["Reference Handle"] = ""
        if clean_text(record.get("Archived")).upper() == "Y":
            continue
        vendor_key = normalize_vendor(vendor_for_record(record))
        if vendor_key in managed_vendors:
            token = clean_text(record.get("Token"))
            if token:
                archive_by_token[token] = record
                managed_export_only_archived += 1
            continue
        active_rows.append(record)
        export_only_rows += 1

    for record in blank_sku_records:
        record["Reference Handle"] = ""
        vendor_key = normalize_vendor(vendor_for_record(record))
        if vendor_key in managed_vendors:
            token = clean_text(record.get("Token"))
            if token:
                archive_by_token[token] = record
                managed_export_only_archived += 1
            continue
        active_rows.append(record)
        blank_sku_rows_kept += 1

    active_rows, category_plan_rows = assign_categories(active_rows)
    active_rows = sorted(active_rows, key=sort_key)
    archive_rows = sorted((build_archive_row(record, template_headers) for record in archive_by_token.values()), key=sort_key)
    full_import_rows = active_rows + archive_rows

    template_prefix_rows = template_rows[:template_header_index]

    package_import_path = PACKAGE_DIR / OUTPUT_NAME
    primary_upload_path = PACKAGE_DIR / PRIMARY_UPLOAD_NAME
    backup_import_path = TO_IMPORT_DIR / "inventory-baseline_2026-03-20_1543.csv"
    archive_path = PACKAGE_DIR / ARCHIVE_NAME
    category_plan_path = PACKAGE_DIR / CATEGORY_PLAN_NAME
    duplicate_review_path = PACKAGE_DIR / "BASELINE_DUPLICATE_REVIEW_2026-03-20_1543.csv"
    summary_path = PACKAGE_DIR / SUMMARY_NAME
    readme_path = PACKAGE_DIR / README_NAME

    write_csv(package_import_path, template_headers, full_import_rows, template_prefix_rows)
    write_csv(primary_upload_path, template_headers, full_import_rows, template_prefix_rows)
    write_csv(backup_import_path, template_headers, full_import_rows, template_prefix_rows)
    write_csv(
        archive_path,
        template_headers,
        archive_rows,
        template_prefix_rows,
    )
    write_csv(category_plan_path, list(category_plan_rows[0].keys()) if category_plan_rows else [], category_plan_rows)
    write_csv(
        duplicate_review_path,
        list(duplicate_review_rows[0].keys()) if duplicate_review_rows else [],
        duplicate_review_rows,
    )

    summary_lines = [
        f"Template: {TEMPLATE_PATH}",
        f"Current Square export: {EXPORT_PATH}",
        f"Master inventory source: {MASTER_PATH}",
        f"Final active rows: {len(active_rows)}",
        f"Archive rows for duplicate tokens: {len(archive_rows)}",
        f"Rows in full import file: {len(full_import_rows)}",
        f"Master rows matched to export by SKU: {matched_master_rows}",
        f"Master rows missing from export and added fresh: {len(master_records) - matched_master_rows}",
        f"Export-only active rows kept: {export_only_rows + blank_sku_rows_kept}",
        f"Managed-vendor export-only rows archived: {managed_export_only_archived}",
        f"Duplicate SKU groups found in export: {len(duplicate_review_rows)}",
        f"Category chunks written: {len(category_plan_rows)}",
        f"Main import file: {primary_upload_path}",
    ]
    summary_path.write_text("\n".join(summary_lines), encoding="utf-8")
    readme_path.write_text(
        "\n".join(
            [
                "Use UPLOAD_THIS_TO_SQUARE.csv for the next Square baseline import.",
                "BASELINE_DUPLICATE_ARCHIVE_2026-03-20_1543.csv is the archive-only slice from the same import.",
                "BASELINE_DUPLICATE_REVIEW_2026-03-20_1543.csv lists the duplicate/current-token cleanup decisions.",
                "BASELINE_CATEGORY_PLAN_2026-03-20_1543.csv lists the new <=200-item category chunks.",
            ]
        ),
        encoding="utf-8",
    )

    print("\n".join(summary_lines))


if __name__ == "__main__":
    main()
