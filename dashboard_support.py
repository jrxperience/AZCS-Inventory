from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "inputs"
OUTPUT_DIR = BASE_DIR / "outputs"
RUNS_DIR = BASE_DIR / "runs"
LATEST_DIR = BASE_DIR / "latest"
ASSETS_DIR = BASE_DIR / "assets"
TO_IMPORT_DIR = BASE_DIR / "to_import"
TO_REVIEW_DIR = BASE_DIR / "to_review"
SQUARE_READY_DIR = BASE_DIR / "square_ready"
SQUARE_READY_CURRENT_DIR = SQUARE_READY_DIR / "CURRENT"
BASELINE_CURRENT_DIR = SQUARE_READY_DIR / "BASELINE_CURRENT"
SQUARE_READY_VERSIONS_DIR = SQUARE_READY_DIR / "VERSIONS"
ICON_PNG_PATH = ASSETS_DIR / "azcs_inventory_icon.png"
ICON_ICO_PATH = ASSETS_DIR / "azcs_inventory_icon.ico"


@dataclass(frozen=True)
class InputFolder:
    key: str
    label: str
    path: Path
    description: str


@dataclass(frozen=True)
class Workflow:
    key: str
    name: str
    script_name: str
    description: str
    input_keys: tuple[str, ...]
    output_files: tuple[str, ...]
    output_root: Path = OUTPUT_DIR


@dataclass
class RunResult:
    workflow: Workflow
    returncode: int
    stdout: str
    stderr: str
    started_at: datetime
    finished_at: datetime
    run_dir: Path
    latest_dir: Path
    copied_outputs: list[Path]
    missing_outputs: list[str]

    @property
    def success(self) -> bool:
        return self.returncode == 0


INPUT_FOLDERS: dict[str, InputFolder] = {
    "price_lists": InputFolder(
        key="price_lists",
        label="Price Lists",
        path=INPUT_DIR / "price_lists",
        description="Vendor price lists used to rebuild the master catalog.",
    ),
    "manual_catalog": InputFolder(
        key="manual_catalog",
        label="Manual Catalog",
        path=INPUT_DIR,
        description="Supplemental catalog rows kept in inputs/manual_catalog_items.csv for items confirmed from sales or vendor websites.",
    ),
    "square_exports": InputFolder(
        key="square_exports",
        label="Square Exports",
        path=INPUT_DIR / "square_exports",
        description="Fresh Square item-library exports used as the live baseline for after-hours receiving.",
    ),
    "deliveries": InputFolder(
        key="deliveries",
        label="Deliveries",
        path=INPUT_DIR / "deliveries",
        description="Packing slips, order details, and receiving files for stock updates.",
    ),
    "adjustments": InputFolder(
        key="adjustments",
        label="Adjustments",
        path=INPUT_DIR / "adjustments",
        description="Manual same-night quantity corrections or recount adjustments.",
    ),
    "sales": InputFolder(
        key="sales",
        label="Sales Reports",
        path=INPUT_DIR / "sales",
        description="Square Sales by item exports for sales-aware pricing.",
    ),
    "price_updates": InputFolder(
        key="price_updates",
        label="Price Updates",
        path=INPUT_DIR / "price_updates",
        description="Manual selling-price overrides that should land in the pricing snapshots.",
    ),
    "pricing_overrides": InputFolder(
        key="pricing_overrides",
        label="Pricing Overrides",
        path=INPUT_DIR / "pricing_overrides",
        description="Manual strategic pricing overrides by SKU.",
    ),
    "sales_match_overrides": InputFolder(
        key="sales_match_overrides",
        label="Sales Match Overrides",
        path=INPUT_DIR / "sales_match_overrides",
        description="Manual fixes for sales rows that do not match cleanly on their own.",
    ),
}


WORKFLOWS: dict[str, Workflow] = {
    "master_inventory": Workflow(
        key="master_inventory",
        name="Build Master Inventory",
        script_name="build_master_inventory.py",
        description="Rebuild the Square catalog from vendor price lists and website/image enrichment.",
        input_keys=("price_lists", "manual_catalog"),
        output_files=(
            "square_master_inventory.csv",
            "square_master_inventory_overlap_review.csv",
            "square_master_inventory_summary.txt",
            "product_enrichment_audit.csv",
            "inventory_database_with_images.csv",
            "inventory_database_with_images.xlsx",
            "product_image_match_audit.csv",
        ),
    ),
    "sales_match": Workflow(
        key="sales_match",
        name="Match Sales History",
        script_name="build_sales_match_audit.py",
        description="Match Square sales history back to catalog SKUs and flag weak matches for review.",
        input_keys=("sales", "sales_match_overrides"),
        output_files=(
            "sales_item_match_audit.csv",
            "sales_item_match_audit.xlsx",
            "sales_item_match_review.csv",
            "sales_catalog_signals.csv",
            "sales_match_summary.txt",
            "sales_match_issues.csv",
        ),
    ),
    "pricing": Workflow(
        key="pricing",
        name="Build Pricing Recommendations",
        script_name="build_pricing_recommendations.py",
        description="Generate catalog pricing recommendations, strategic imports, and price update files.",
        input_keys=("price_updates", "pricing_overrides", "sales"),
        output_files=(
            "pricing_recommendations.csv",
            "pricing_recommendations.xlsx",
            "square_master_inventory_strategic_pricing.csv",
            "square_catalog_strategic_price_update.csv",
            "pricing_strategy_summary.txt",
            "pricing_strategy_issues.csv",
        ),
    ),
    "final_upload": Workflow(
        key="final_upload",
        name="Build Final Square Upload",
        script_name="run_inventory_workflow.py",
        description="Run the full catalog workflow and produce the validated Square upload package.",
        input_keys=("price_lists", "manual_catalog", "sales", "sales_match_overrides", "pricing_overrides", "square_exports"),
        output_files=(
            "UPLOAD_THIS_TO_SQUARE.csv",
            "UPLOAD_THIS_TO_SQUARE.xlsx",
            "WORKFLOW_VALIDATION.txt",
            "BASELINE_SUMMARY.txt",
            "BASELINE_CATEGORY_PLAN.csv",
            "BASELINE_DUPLICATE_REVIEW.csv",
        ),
        output_root=BASELINE_CURRENT_DIR,
    ),
    "receiving": Workflow(
        key="receiving",
        name="Build After-Hours Receiving Import",
        script_name="build_receiving_import.py",
        description="Build a small Square-ready receiving import from a fresh Square export plus the current delivery batch.",
        input_keys=("square_exports", "deliveries", "adjustments"),
        output_files=(
            "square_receiving_update.csv",
            "square_receiving_update.xlsx",
            "receiving_update_audit.csv",
            "receiving_update_issues.csv",
            "receiving_update_summary.txt",
        ),
    ),
    "seed_stock": Workflow(
        key="seed_stock",
        name="Seed AZCS Stock From Square Export",
        script_name="build_seed_stock_import.py",
        description="Copy current stock from the live Square export into the AZCS New Quantity column for the full catalog import.",
        input_keys=("square_exports",),
        output_files=(
            "square_seed_stock_import.csv",
            "square_seed_stock_import.xlsx",
            "stock_seed_audit.csv",
            "stock_seed_issues.csv",
            "stock_seed_summary.txt",
        ),
    ),
    "stock_snapshot": Workflow(
        key="stock_snapshot",
        name="Build Stock Snapshots",
        script_name="build_stock_snapshot.py",
        description="Update stock and pricing snapshots from delivery, adjustment, and price update files.",
        input_keys=("deliveries", "adjustments", "price_updates"),
        output_files=(
            "current_stock_snapshot.csv",
            "current_pricing_snapshot.csv",
            "square_inventory_quantity_update.csv",
            "square_catalog_price_update.csv",
            "stock_transaction_issues.csv",
            "stock_snapshot_summary.txt",
        ),
    ),
}


PUBLISHED_OUTPUTS: dict[str, tuple[tuple[str, Path, str], ...]] = {
    "master_inventory": (
        ("square_master_inventory.csv", TO_IMPORT_DIR, "catalog_master_baseline.csv"),
        ("square_master_inventory_overlap_review.csv", TO_REVIEW_DIR, "catalog_overlap_review.csv"),
        ("square_master_inventory_summary.txt", TO_REVIEW_DIR, "catalog_build_summary.txt"),
        ("product_enrichment_audit.csv", TO_REVIEW_DIR, "catalog_enrichment_audit.csv"),
        ("inventory_database_with_images.csv", TO_REVIEW_DIR, "catalog_with_images.csv"),
        ("inventory_database_with_images.xlsx", TO_REVIEW_DIR, "catalog_with_images.xlsx"),
        ("product_image_match_audit.csv", TO_REVIEW_DIR, "catalog_image_audit.csv"),
    ),
    "sales_match": (
        ("sales_item_match_audit.csv", TO_REVIEW_DIR, "sales_match_audit.csv"),
        ("sales_item_match_audit.xlsx", TO_REVIEW_DIR, "sales_match_audit.xlsx"),
        ("sales_item_match_review.csv", TO_REVIEW_DIR, "sales_match_review.csv"),
        ("sales_catalog_signals.csv", TO_REVIEW_DIR, "sales_catalog_signals.csv"),
        ("sales_match_summary.txt", TO_REVIEW_DIR, "sales_match_summary.txt"),
        ("sales_match_issues.csv", TO_REVIEW_DIR, "sales_match_issues.csv"),
    ),
    "pricing": (
        ("square_master_inventory_strategic_pricing.csv", TO_IMPORT_DIR, "catalog_import_current.csv"),
        ("square_catalog_strategic_price_update.csv", TO_IMPORT_DIR, "catalog_price_update.csv"),
        ("pricing_recommendations.csv", TO_REVIEW_DIR, "pricing_recommendations.csv"),
        ("pricing_recommendations.xlsx", TO_REVIEW_DIR, "pricing_recommendations.xlsx"),
        ("pricing_strategy_summary.txt", TO_REVIEW_DIR, "pricing_summary.txt"),
        ("pricing_strategy_issues.csv", TO_REVIEW_DIR, "pricing_issues.csv"),
    ),
    "receiving": (
        ("square_receiving_update.csv", TO_IMPORT_DIR, "receiving_import.csv"),
        ("square_receiving_update.xlsx", TO_IMPORT_DIR, "receiving_import.xlsx"),
        ("receiving_update_audit.csv", TO_REVIEW_DIR, "receiving_audit.csv"),
        ("receiving_update_issues.csv", TO_REVIEW_DIR, "receiving_issues.csv"),
        ("receiving_update_summary.txt", TO_REVIEW_DIR, "receiving_summary.txt"),
    ),
    "seed_stock": (
        ("square_seed_stock_import.csv", TO_IMPORT_DIR, "catalog_import_current_with_stock.csv"),
        ("square_seed_stock_import.xlsx", TO_IMPORT_DIR, "catalog_import_current_with_stock.xlsx"),
        ("stock_seed_audit.csv", TO_REVIEW_DIR, "stock_seed_audit.csv"),
        ("stock_seed_issues.csv", TO_REVIEW_DIR, "stock_seed_issues.csv"),
        ("stock_seed_summary.txt", TO_REVIEW_DIR, "stock_seed_summary.txt"),
    ),
    "stock_snapshot": (
        ("square_inventory_quantity_update.csv", TO_IMPORT_DIR, "quantity_update_from_transactions.csv"),
        ("square_catalog_price_update.csv", TO_IMPORT_DIR, "catalog_price_update_from_transactions.csv"),
        ("current_stock_snapshot.csv", TO_REVIEW_DIR, "current_stock_snapshot.csv"),
        ("current_pricing_snapshot.csv", TO_REVIEW_DIR, "current_pricing_snapshot.csv"),
        ("stock_snapshot_summary.txt", TO_REVIEW_DIR, "stock_snapshot_summary.txt"),
        ("stock_transaction_issues.csv", TO_REVIEW_DIR, "stock_transaction_issues.csv"),
    ),
}

DATED_IMPORT_ALIASES: dict[str, str] = {
    "catalog_import_current.csv": "inventory",
    "catalog_master_baseline.csv": "inventory-baseline",
    "catalog_price_update.csv": "price-update",
    "receiving_import.csv": "receiving-import",
    "catalog_import_current_with_stock.csv": "inventory-with-stock",
    "catalog_import_current_with_stock.xlsx": "inventory-with-stock",
    "receiving_import.xlsx": "receiving-import",
    "quantity_update_from_transactions.csv": "quantity-update",
    "catalog_price_update_from_transactions.csv": "price-update-from-transactions",
}

WORKFLOW_RECOMMENDED_UPLOAD_ALIASES: dict[str, tuple[str, ...]] = {
    "master_inventory": ("inventory-baseline",),
    "pricing": ("inventory", "price-update"),
    "receiving": ("receiving-import",),
    "seed_stock": ("inventory-with-stock",),
    "stock_snapshot": ("quantity-update", "price-update-from-transactions"),
}

STANDARD_HANDOFF_TARGETS: dict[str, tuple[str, ...]] = {
    "master_inventory": ("UPLOAD_MASTER_BASELINE.csv",),
    "pricing": ("UPLOAD_PRICE_UPDATE.csv",),
    "final_upload": ("UPLOAD_INVENTORY.csv", "UPLOAD_INVENTORY.xlsx", "UPLOAD_VALIDATION.txt"),
    "receiving": ("UPLOAD_RECEIVING.csv", "UPLOAD_RECEIVING.xlsx"),
    "seed_stock": ("UPLOAD_INVENTORY_WITH_STOCK.csv", "UPLOAD_INVENTORY_WITH_STOCK.xlsx"),
    "stock_snapshot": ("UPLOAD_QUANTITY_UPDATE.csv", "UPLOAD_PRICE_UPDATE_FROM_TRANSACTIONS.csv"),
}

CURRENT_UPLOAD_PRIORITY: tuple[tuple[str, ...], ...] = (
    ("UPLOAD_INVENTORY_FULL_ACTIVE.csv", "UPLOAD_INVENTORY_FULL_ACTIVE.xlsx", "UPLOAD_INVENTORY_FULL_ACTIVE.txt"),
    ("UPLOAD_INVENTORY_CLEANED.csv", "UPLOAD_INVENTORY_CLEANED.xlsx", "UPLOAD_INVENTORY_CLEANED.txt"),
    ("UPLOAD_INVENTORY.csv", "UPLOAD_INVENTORY.xlsx", "UPLOAD_VALIDATION.txt"),
)

CURRENT_UPLOAD_VERSION_PREFIXES: dict[str, str] = {
    "UPLOAD_INVENTORY_FULL_ACTIVE.csv": "inventory",
    "UPLOAD_INVENTORY_CLEANED.csv": "inventory_cleaned",
    "UPLOAD_INVENTORY.csv": "inventory_base",
}

VERSIONED_UPLOAD_TARGETS: dict[str, tuple[str, ...]] = {
    "inventory": ("UPLOAD_INVENTORY_FULL_ACTIVE.csv", "UPLOAD_INVENTORY_FULL_ACTIVE.xlsx"),
    "inventory_cleaned": ("UPLOAD_INVENTORY_CLEANED.csv", "UPLOAD_INVENTORY_CLEANED.xlsx"),
    "inventory_base": ("UPLOAD_INVENTORY.csv", "UPLOAD_INVENTORY.xlsx"),
    "inventory_with_stock": ("UPLOAD_INVENTORY_WITH_STOCK.csv", "UPLOAD_INVENTORY_WITH_STOCK.xlsx"),
    "receiving": ("UPLOAD_RECEIVING.csv", "UPLOAD_RECEIVING.xlsx"),
}


def ensure_runtime_dirs() -> None:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_DIR.mkdir(parents=True, exist_ok=True)
    TO_IMPORT_DIR.mkdir(parents=True, exist_ok=True)
    TO_REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    SQUARE_READY_DIR.mkdir(parents=True, exist_ok=True)
    SQUARE_READY_CURRENT_DIR.mkdir(parents=True, exist_ok=True)
    SQUARE_READY_VERSIONS_DIR.mkdir(parents=True, exist_ok=True)
    for folder in INPUT_FOLDERS.values():
        folder.path.mkdir(parents=True, exist_ok=True)
    for workflow in WORKFLOWS.values():
        (LATEST_DIR / workflow.key).mkdir(parents=True, exist_ok=True)
    _write_workspace_guides()


def list_input_files(folder_key: str) -> list[Path]:
    folder = INPUT_FOLDERS[folder_key].path
    ensure_runtime_dirs()
    return sorted([path for path in folder.iterdir() if path.is_file()], key=lambda path: path.name.lower())


def copy_files_to_input(folder_key: str, source_paths: list[str]) -> list[Path]:
    ensure_runtime_dirs()
    destination_dir = INPUT_FOLDERS[folder_key].path
    copied: list[Path] = []
    for raw_path in source_paths:
        source = Path(raw_path)
        if not source.is_file():
            continue
        destination = destination_dir / source.name
        shutil.copy2(source, destination)
        copied.append(destination)
    return copied


def open_path(path: Path) -> None:
    os.startfile(str(path))  # type: ignore[attr-defined]


def get_recommended_upload_files(workflow_key: str) -> list[Path]:
    ensure_runtime_dirs()
    aliases = WORKFLOW_RECOMMENDED_UPLOAD_ALIASES.get(workflow_key, ())
    recommended: list[Path] = []
    for alias in aliases:
        matches = sorted(TO_IMPORT_DIR.glob(f"{alias}_*"), key=lambda path: path.stat().st_mtime, reverse=True)
        if matches:
            recommended.append(matches[0])
    return recommended


def get_standard_handoff_files(workflow_key: str) -> list[Path]:
    ensure_runtime_dirs()
    targets = STANDARD_HANDOFF_TARGETS.get(workflow_key, ())
    return [path for path in (SQUARE_READY_CURRENT_DIR / name for name in targets) if path.exists()]


def get_current_upload_package() -> list[Path]:
    ensure_runtime_dirs()
    for candidates in CURRENT_UPLOAD_PRIORITY:
        paths = [SQUARE_READY_CURRENT_DIR / name for name in candidates if (SQUARE_READY_CURRENT_DIR / name).exists()]
        if paths:
            return paths
    return []


def get_current_upload_version_label() -> str:
    ensure_runtime_dirs()
    package = get_current_upload_package()
    if not package:
        return ""
    lead_name = package[0].name
    prefix = CURRENT_UPLOAD_VERSION_PREFIXES.get(lead_name, "")
    if not prefix:
        return ""
    matches = sorted(SQUARE_READY_VERSIONS_DIR.glob(f"{prefix}_v*.csv"), key=lambda path: path.stat().st_mtime, reverse=True)
    return matches[0].name if matches else ""


def _clear_directory_contents(path: Path) -> None:
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_file():
            child.unlink()
        elif child.is_dir():
            shutil.rmtree(child)


def _write_workspace_guides() -> None:
    import_lines = [
        "Use this folder for the simple, current import files.",
        "catalog_import_current.csv = full current master import with pricing applied.",
        "catalog_master_baseline.csv = baseline master catalog without the strategic pricing overlay.",
        "catalog_price_update.csv = price-only Square update file from the pricing workflow.",
        "receiving_import.csv = after-hours receiving import built from a fresh Square export.",
        "catalog_import_current_with_stock.csv = full current import with AZCS stock seeded from the latest live Square export.",
        "quantity_update_from_transactions.csv = quantity update file from the stock snapshot workflow.",
        "catalog_price_update_from_transactions.csv = price update file from the stock snapshot workflow.",
        "Dated copies are also written here with names like inventory_2026-03-20_134500.csv so the newest Square upload file is easier to spot.",
        f"For the cleanest final handoff, open {SQUARE_READY_CURRENT_DIR}.",
    ]
    review_lines = [
        "Use this folder for the simple, current review and workbook files.",
        "catalog_overlap_review.csv = duplicate and merge review items from the master build.",
        "pricing_recommendations.xlsx = the pricing workbook to review first.",
        "sales_match_review.csv = unresolved sales rows that still need manual review.",
        "receiving_audit.csv = what changed in the current receiving import.",
        "stock_seed_audit.csv = which live export rows were copied into the seeded AZCS import.",
        "current_stock_snapshot.csv = current internal stock snapshot from transaction logs.",
    ]
    (TO_IMPORT_DIR / "README.txt").write_text("\n".join(import_lines), encoding="utf-8")
    (TO_REVIEW_DIR / "README.txt").write_text("\n".join(review_lines), encoding="utf-8")


def _publish_friendly_outputs(
    workflow_key: str,
    source_dir: Path,
    *,
    write_dated_aliases: bool = False,
    timestamp: str | None = None,
) -> list[Path]:
    published: list[Path] = []
    for source_name, destination_dir, published_name in PUBLISHED_OUTPUTS.get(workflow_key, ()):
        source = source_dir / source_name
        if not source.exists():
            continue
        destination_dir.mkdir(parents=True, exist_ok=True)
        target = destination_dir / published_name
        shutil.copy2(source, target)
        published.append(target)
        alias_stem = DATED_IMPORT_ALIASES.get(published_name)
        if destination_dir == TO_IMPORT_DIR and alias_stem and write_dated_aliases and timestamp:
            dated_name = f"{alias_stem}_{timestamp}{target.suffix.lower()}"
            shutil.copy2(source, destination_dir / dated_name)
            published.append(destination_dir / dated_name)
    return published


def _publish_square_ready_current() -> list[Path]:
    ensure_runtime_dirs()
    _clear_directory_contents(SQUARE_READY_CURRENT_DIR)

    mappings = (
        (OUTPUT_DIR / "square_master_inventory.csv", "UPLOAD_MASTER_BASELINE.csv"),
        (BASELINE_CURRENT_DIR / "UPLOAD_THIS_TO_SQUARE.csv", "UPLOAD_INVENTORY.csv"),
        (BASELINE_CURRENT_DIR / "UPLOAD_THIS_TO_SQUARE.xlsx", "UPLOAD_INVENTORY.xlsx"),
        (BASELINE_CURRENT_DIR / "WORKFLOW_VALIDATION.txt", "UPLOAD_VALIDATION.txt"),
        (BASELINE_CURRENT_DIR / "BASELINE_SUMMARY.txt", "UPLOAD_SUMMARY.txt"),
        (OUTPUT_DIR / "square_catalog_strategic_price_update.csv", "UPLOAD_PRICE_UPDATE.csv"),
        (OUTPUT_DIR / "square_receiving_update.csv", "UPLOAD_RECEIVING.csv"),
        (OUTPUT_DIR / "square_receiving_update.xlsx", "UPLOAD_RECEIVING.xlsx"),
        (OUTPUT_DIR / "square_seed_stock_import.csv", "UPLOAD_INVENTORY_WITH_STOCK.csv"),
        (OUTPUT_DIR / "square_seed_stock_import.xlsx", "UPLOAD_INVENTORY_WITH_STOCK.xlsx"),
        (OUTPUT_DIR / "square_inventory_quantity_update.csv", "UPLOAD_QUANTITY_UPDATE.csv"),
        (OUTPUT_DIR / "square_catalog_price_update.csv", "UPLOAD_PRICE_UPDATE_FROM_TRANSACTIONS.csv"),
    )

    published: list[Path] = []
    readme_lines = [
        "Use this folder first.",
        "UPLOAD_INVENTORY.csv = the current full Square upload file.",
        "UPLOAD_INVENTORY.xlsx = the same full upload package in Excel format for review.",
        "UPLOAD_VALIDATION.txt = the quick quality check for the current full upload.",
        "UPLOAD_SUMMARY.txt = the run summary for the current full upload.",
        "UPLOAD_MASTER_BASELINE.csv = the raw master catalog before the final baseline merge.",
        "UPLOAD_PRICE_UPDATE.csv = the current price-only Square update file.",
        "UPLOAD_RECEIVING.csv = the current after-hours receiving import file.",
        "UPLOAD_INVENTORY_WITH_STOCK.csv = the current stock-seeded full catalog import.",
        "UPLOAD_QUANTITY_UPDATE.csv = the current quantity-only update file from stock transactions.",
        "UPLOAD_PRICE_UPDATE_FROM_TRANSACTIONS.csv = the current price update file from stock transactions.",
    ]

    for source, target_name in mappings:
        if not source.exists():
            continue
        target = SQUARE_READY_CURRENT_DIR / target_name
        shutil.copy2(source, target)
        published.append(target)

    (SQUARE_READY_CURRENT_DIR / "README.txt").write_text("\n".join(readme_lines), encoding="utf-8")
    published.append(SQUARE_READY_CURRENT_DIR / "README.txt")
    return published


def _next_version_number(prefix: str, suffix: str) -> int:
    matches = sorted(SQUARE_READY_VERSIONS_DIR.glob(f"{prefix}_v*{suffix}"))
    version_numbers: list[int] = []
    for path in matches:
        stem = path.stem
        if "_v" not in stem:
            continue
        tail = stem.rsplit("_v", 1)[-1]
        if tail.isdigit():
            version_numbers.append(int(tail))
    return (max(version_numbers) + 1) if version_numbers else 1


def _publish_versioned_upload_aliases() -> list[Path]:
    ensure_runtime_dirs()
    published: list[Path] = []
    for prefix, candidate_names in VERSIONED_UPLOAD_TARGETS.items():
        source: Path | None = None
        for candidate_name in candidate_names:
            candidate = SQUARE_READY_CURRENT_DIR / candidate_name
            if candidate.exists():
                source = candidate
                break
        if source is None:
            continue
        version = _next_version_number(prefix, source.suffix.lower())
        target = SQUARE_READY_VERSIONS_DIR / f"{prefix}_v{version:03d}{source.suffix.lower()}"
        shutil.copy2(source, target)
        published.append(target)
    readme_lines = [
        "This folder keeps versioned upload snapshots with simple names.",
        "Examples: inventory_v001.csv, inventory_v002.csv, receiving_v001.csv.",
        "CURRENT still holds the stable latest files.",
        "Use VERSIONS when you want a clear history that is easier to distinguish at a glance.",
    ]
    (SQUARE_READY_VERSIONS_DIR / "README.txt").write_text("\n".join(readme_lines), encoding="utf-8")
    published.append(SQUARE_READY_VERSIONS_DIR / "README.txt")
    return published


def _trim_dated_import_aliases(keep: int = 3) -> None:
    for alias in sorted(set(DATED_IMPORT_ALIASES.values())):
        for suffix in (".csv", ".xlsx"):
            matches = sorted(
                (
                    path
                    for path in TO_IMPORT_DIR.glob(f"{alias}_*{suffix}")
                    if path.is_file() and path.stem != f"{alias}_current"
                ),
                key=lambda path: path.stat().st_mtime,
                reverse=True,
            )
            for path in matches[keep:]:
                path.unlink()


def _write_run_log(run_dir: Path, result: RunResult) -> None:
    lines = [
        f"Workflow: {result.workflow.name}",
        f"Script: {result.workflow.script_name}",
        f"Started: {result.started_at.isoformat(timespec='seconds')}",
        f"Finished: {result.finished_at.isoformat(timespec='seconds')}",
        f"Return code: {result.returncode}",
        f"Copied outputs: {len(result.copied_outputs)}",
        f"Missing outputs: {', '.join(result.missing_outputs) if result.missing_outputs else '[none]'}",
        "",
        "STDOUT",
        result.stdout.strip(),
        "",
        "STDERR",
        result.stderr.strip(),
        "",
    ]
    (run_dir / "run_log.txt").write_text("\n".join(lines), encoding="utf-8")


def run_workflow(workflow_key: str) -> RunResult:
    ensure_runtime_dirs()
    workflow = WORKFLOWS[workflow_key]

    # Security verification for script execution
    script_path = Path(workflow.script_name)
    if script_path.parts != (workflow.script_name,) or not (BASE_DIR / workflow.script_name).is_file():
        raise ValueError(f"Security error: invalid or missing script name '{workflow.script_name}'")

    started_at = datetime.now()
    completed = subprocess.run(
        [sys.executable, workflow.script_name],
        cwd=BASE_DIR,
        capture_output=True,
        text=True,
    )
    finished_at = datetime.now()

    timestamp = started_at.strftime("%Y-%m-%d_%H%M%S")
    run_dir = RUNS_DIR / timestamp / workflow.key
    latest_dir = LATEST_DIR / workflow.key
    run_dir.mkdir(parents=True, exist_ok=True)

    copied_outputs: list[Path] = []
    missing_outputs: list[str] = []

    if completed.returncode == 0:
        _clear_directory_contents(latest_dir)
        for output_name in workflow.output_files:
            source = workflow.output_root / output_name
            if not source.exists():
                missing_outputs.append(output_name)
                continue
            run_copy = run_dir / output_name
            latest_copy = latest_dir / output_name
            shutil.copy2(source, run_copy)
            shutil.copy2(source, latest_copy)
            copied_outputs.append(run_copy)

    result = RunResult(
        workflow=workflow,
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        started_at=started_at,
        finished_at=finished_at,
        run_dir=run_dir,
        latest_dir=latest_dir,
        copied_outputs=copied_outputs,
        missing_outputs=missing_outputs,
    )
    _write_run_log(run_dir, result)
    if result.success:
        shutil.copy2(run_dir / "run_log.txt", latest_dir / "run_log.txt")
        _publish_friendly_outputs(
            workflow.key,
            run_dir,
            write_dated_aliases=True,
            timestamp=run_dir.parent.name,
        )
        _publish_square_ready_current()
        _publish_versioned_upload_aliases()
        _trim_dated_import_aliases()
    return result


def list_latest_outputs(workflow_key: str) -> list[Path]:
    folder = LATEST_DIR / workflow_key
    ensure_runtime_dirs()
    return sorted([path for path in folder.iterdir() if path.is_file()], key=lambda path: path.name.lower())


def list_recent_runs(limit: int = 10) -> list[Path]:
    ensure_runtime_dirs()
    run_dirs = [path for path in RUNS_DIR.iterdir() if path.is_dir()]
    return sorted(run_dirs, key=lambda path: path.name, reverse=True)[:limit]


def publish_existing_outputs() -> list[Path]:
    ensure_runtime_dirs()
    published: list[Path] = []
    for workflow_key, workflow in WORKFLOWS.items():
        published.extend(_publish_friendly_outputs(workflow_key, workflow.output_root, write_dated_aliases=False))
    published.extend(_publish_square_ready_current())
    return published
