from __future__ import annotations

import csv
import shutil
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
SQUARE_READY_DIR = BASE_DIR / "square_ready"
SOURCE_TEMPLATE_CSV = SQUARE_READY_DIR / "TUCKER" / "TUCKER_ONLY_INVENTORY.csv"
IMAGE_DATABASE_CSV = BASE_DIR / "outputs" / "inventory_database_with_images.csv"
PACKAGE_DIR = SQUARE_READY_DIR / "TUCKER_225"
PACKAGE_CSV = PACKAGE_DIR / "TUCKER_225_IMPORT.csv"
PACKAGE_IMAGE_DIR = PACKAGE_DIR / "IMAGES"
PACKAGE_MAP_CSV = PACKAGE_DIR / "TUCKER_225_IMAGE_MAP.csv"
PACKAGE_README = PACKAGE_DIR / "README.txt"
TARGET_COUNT = 225


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).replace("\xa0", " ").strip()


def clear_directory(path: Path) -> None:
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_file():
            child.unlink()
        elif child.is_dir():
            shutil.rmtree(child)


def read_csv_rows(path: Path) -> list[list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [list(row) for row in csv.reader(handle)]


def write_csv_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)


def read_dict_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [{key: clean_text(value) for key, value in row.items()} for row in csv.DictReader(handle)]


def write_dict_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    if not SOURCE_TEMPLATE_CSV.exists():
        raise FileNotFoundError(f"Missing {SOURCE_TEMPLATE_CSV}")
    if not IMAGE_DATABASE_CSV.exists():
        raise FileNotFoundError(f"Missing {IMAGE_DATABASE_CSV}")

    PACKAGE_DIR.mkdir(parents=True, exist_ok=True)
    clear_directory(PACKAGE_DIR)
    PACKAGE_DIR.mkdir(parents=True, exist_ok=True)
    PACKAGE_IMAGE_DIR.mkdir(parents=True, exist_ok=True)

    source_rows = read_csv_rows(SOURCE_TEMPLATE_CSV)
    header_index = next(index for index, row in enumerate(source_rows) if "SKU" in row)
    headers = source_rows[header_index]
    sku_index = headers.index("SKU")
    item_name_index = headers.index("Item Name")
    selected_rows = source_rows[header_index + 1 : header_index + 1 + TARGET_COUNT]
    package_rows = source_rows[: header_index + 1] + selected_rows
    write_csv_rows(PACKAGE_CSV, package_rows)

    image_rows = read_dict_rows(IMAGE_DATABASE_CSV)
    image_by_sku = {
        clean_text(row.get("SKU")): row
        for row in image_rows
        if clean_text(row.get("Default Vendor Name")) == "Tucker" and clean_text(row.get("SKU"))
    }

    copied = 0
    missing = 0
    map_rows: list[dict[str, str]] = []
    for position, row in enumerate(selected_rows, start=1):
        sku = clean_text(row[sku_index]) if len(row) > sku_index else ""
        item_name = clean_text(row[item_name_index]) if len(row) > item_name_index else ""
        image_row = image_by_sku.get(sku, {})
        image_path = Path(clean_text(image_row.get("Local Image Absolute Path")))
        image_name = image_path.name if image_path.name else ""
        status = "copied" if image_path.is_file() else "missing"
        if image_path.is_file():
            shutil.copy2(image_path, PACKAGE_IMAGE_DIR / image_name)
            copied += 1
        else:
            missing += 1
        map_rows.append(
            {
                "Row Number": str(position),
                "SKU": sku,
                "Item Name": item_name,
                "Image Filename": image_name,
                "Local Image Absolute Path": clean_text(image_row.get("Local Image Absolute Path")),
                "Status": status,
            }
        )

    write_dict_csv(
        PACKAGE_MAP_CSV,
        ["Row Number", "SKU", "Item Name", "Image Filename", "Local Image Absolute Path", "Status"],
        map_rows,
    )

    PACKAGE_README.write_text(
        "\n".join(
            [
                "Use this folder only.",
                f"TUCKER_225_IMPORT.csv = first {TARGET_COUNT} Tucker rows in Square import-template format.",
                f"IMAGES/ = corresponding local images copied for those first {TARGET_COUNT} rows.",
                f"TUCKER_225_IMAGE_MAP.csv = row-by-row image checklist.",
                f"Images copied: {copied}.",
                f"Images missing: {missing}.",
            ]
        ),
        encoding="utf-8",
    )

    print(f"Tucker 225 package: {PACKAGE_DIR}")
    print(f"Rows packaged: {len(selected_rows)}")
    print(f"Images copied: {copied}")
    print(f"Images missing: {missing}")


if __name__ == "__main__":
    main()
