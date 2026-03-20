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
        input_keys=("price_lists",),
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


def ensure_runtime_dirs() -> None:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_DIR.mkdir(parents=True, exist_ok=True)
    TO_IMPORT_DIR.mkdir(parents=True, exist_ok=True)
    TO_REVIEW_DIR.mkdir(parents=True, exist_ok=True)
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


def _publish_friendly_outputs(workflow_key: str, run_dir: Path) -> None:
    for source_name, destination_dir, published_name in PUBLISHED_OUTPUTS.get(workflow_key, ()):
        source = run_dir / source_name
        if not source.exists():
            continue
        destination_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination_dir / published_name)


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
            source = OUTPUT_DIR / output_name
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
        _publish_friendly_outputs(workflow.key, run_dir)
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
    for workflow_key, mappings in PUBLISHED_OUTPUTS.items():
        for source_name, destination_dir, published_name in mappings:
            source = OUTPUT_DIR / source_name
            if not source.exists():
                continue
            destination_dir.mkdir(parents=True, exist_ok=True)
            target = destination_dir / published_name
            shutil.copy2(source, target)
            published.append(target)
    return published
