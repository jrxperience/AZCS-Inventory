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
        name="Master Inventory",
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
        name="Sales Match",
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
        name="Strategic Pricing",
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
        name="After-Hours Receiving",
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
    "stock_snapshot": Workflow(
        key="stock_snapshot",
        name="Stock Snapshot",
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


def ensure_runtime_dirs() -> None:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_DIR.mkdir(parents=True, exist_ok=True)
    for folder in INPUT_FOLDERS.values():
        folder.path.mkdir(parents=True, exist_ok=True)
    for workflow in WORKFLOWS.values():
        (LATEST_DIR / workflow.key).mkdir(parents=True, exist_ok=True)


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
    return result


def list_latest_outputs(workflow_key: str) -> list[Path]:
    folder = LATEST_DIR / workflow_key
    ensure_runtime_dirs()
    return sorted([path for path in folder.iterdir() if path.is_file()], key=lambda path: path.name.lower())


def list_recent_runs(limit: int = 10) -> list[Path]:
    ensure_runtime_dirs()
    run_dirs = [path for path in RUNS_DIR.iterdir() if path.is_dir()]
    return sorted(run_dirs, key=lambda path: path.name, reverse=True)[:limit]
