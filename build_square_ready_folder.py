from __future__ import annotations

import shutil
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
TO_IMPORT_DIR = BASE_DIR / "to_import"
OUTPUT_DIR = BASE_DIR / "outputs"
SQUARE_READY_DIR = BASE_DIR / "square_ready"
SQUARE_READY_BATCH_DIR = SQUARE_READY_DIR / "IMAGE_BATCHES"
PRIMARY_UPLOAD_NAME = "USE_THIS_FULL_INVENTORY_UPLOAD.csv"


def clear_directory(path: Path) -> None:
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_file():
            child.unlink()
        elif child.is_dir():
            shutil.rmtree(child)


def newest(pattern: str, folder: Path) -> Path | None:
    matches = [path for path in folder.glob(pattern) if path.is_file()]
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_mtime)


def copy_if_exists(source: Path | None, destination: Path) -> None:
    if source is None or not source.exists():
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def write_readme() -> None:
    lines = [
        "Use this folder only.",
        f"{PRIMARY_UPLOAD_NAME} = full item catalog upload for Square. Use this file first.",
        "UPLOAD_FULL_INVENTORY.csv = older standard copy if it is present and current.",
        "UPLOAD_PRICE_UPDATE.csv = price-only Square update file.",
        "UPLOAD_QUANTITY_UPDATE.csv = quantity-only Square update file.",
        "UPLOAD_RECEIVING.csv = after-hours receiving upload file.",
        "UPLOAD_FULL_INVENTORY_WITH_STOCK.csv = full catalog upload with AZCS stock seeded.",
        "IMAGE_MATCH_QUEUE.csv = master list for matching product images.",
        "IMAGE_BATCHES/ = 250-at-a-time image matching batches.",
    ]
    (SQUARE_READY_DIR / "README.txt").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    SQUARE_READY_DIR.mkdir(parents=True, exist_ok=True)
    clear_directory(SQUARE_READY_DIR)
    SQUARE_READY_DIR.mkdir(parents=True, exist_ok=True)

    latest_upload = newest("inventory_upload_*.csv", TO_IMPORT_DIR)
    copy_if_exists(latest_upload, SQUARE_READY_DIR / PRIMARY_UPLOAD_NAME)
    copy_if_exists(latest_upload, SQUARE_READY_DIR / "UPLOAD_FULL_INVENTORY.csv")
    copy_if_exists(TO_IMPORT_DIR / "catalog_import_current.csv", SQUARE_READY_DIR / "UPLOAD_FULL_INVENTORY_PRICING.csv")
    copy_if_exists(TO_IMPORT_DIR / "catalog_master_baseline.csv", SQUARE_READY_DIR / "UPLOAD_FULL_INVENTORY_BASELINE.csv")
    copy_if_exists(TO_IMPORT_DIR / "catalog_price_update.csv", SQUARE_READY_DIR / "UPLOAD_PRICE_UPDATE.csv")
    copy_if_exists(TO_IMPORT_DIR / "quantity_update_from_transactions.csv", SQUARE_READY_DIR / "UPLOAD_QUANTITY_UPDATE.csv")
    copy_if_exists(TO_IMPORT_DIR / "receiving_import.csv", SQUARE_READY_DIR / "UPLOAD_RECEIVING.csv")
    copy_if_exists(TO_IMPORT_DIR / "catalog_import_current_with_stock.csv", SQUARE_READY_DIR / "UPLOAD_FULL_INVENTORY_WITH_STOCK.csv")

    copy_if_exists(OUTPUT_DIR / "square_image_match_queue.csv", SQUARE_READY_DIR / "IMAGE_MATCH_QUEUE.csv")
    copy_if_exists(OUTPUT_DIR / "square_image_match_queue.xlsx", SQUARE_READY_DIR / "IMAGE_MATCH_QUEUE.xlsx")
    copy_if_exists(OUTPUT_DIR / "square_image_match_summary.txt", SQUARE_READY_DIR / "IMAGE_MATCH_SUMMARY.txt")

    source_batch_dir = OUTPUT_DIR / "square_image_match_batches"
    if source_batch_dir.exists():
        shutil.copytree(source_batch_dir, SQUARE_READY_BATCH_DIR, dirs_exist_ok=True)

    write_readme()

    print(f"Square-ready folder: {SQUARE_READY_DIR}")
    for path in sorted(SQUARE_READY_DIR.rglob("*")):
        print(path.relative_to(SQUARE_READY_DIR))


if __name__ == "__main__":
    main()
