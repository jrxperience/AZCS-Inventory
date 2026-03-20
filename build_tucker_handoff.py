from __future__ import annotations

import csv
import math
import shutil
from pathlib import Path

from openpyxl import Workbook


BASE_DIR = Path(__file__).resolve().parent
SQUARE_READY_DIR = BASE_DIR / "square_ready"
TUCKER_DIR = SQUARE_READY_DIR / "TUCKER"
FULL_UPLOAD_PATH = SQUARE_READY_DIR / "UPLOAD_FULL_INVENTORY.csv"
PRIMARY_FULL_UPLOAD_PATH = SQUARE_READY_DIR / "USE_THIS_FULL_INVENTORY_UPLOAD.csv"
IMAGE_QUEUE_PATH = SQUARE_READY_DIR / "IMAGE_MATCH_QUEUE.csv"
MASTER_INVENTORY_PATH = BASE_DIR / "outputs" / "square_master_inventory.csv"
IMAGE_DATABASE_PATH = BASE_DIR / "outputs" / "inventory_database_with_images.csv"
TUCKER_UPLOAD_PATH = TUCKER_DIR / "TUCKER_ONLY_INVENTORY.csv"
TUCKER_QUEUE_CSV_PATH = TUCKER_DIR / "TUCKER_IMAGE_QUEUE.csv"
TUCKER_QUEUE_XLSX_PATH = TUCKER_DIR / "TUCKER_IMAGE_QUEUE.xlsx"
TUCKER_PREP_CSV_PATH = TUCKER_DIR / "TUCKER_IMAGE_PREP_QUEUE.csv"
TUCKER_PREP_XLSX_PATH = TUCKER_DIR / "TUCKER_IMAGE_PREP_QUEUE.xlsx"
TUCKER_MISSING_IMAGE_PATH = TUCKER_DIR / "TUCKER_IMAGE_MISSING.csv"
TUCKER_SUMMARY_PATH = TUCKER_DIR / "TUCKER_IMAGE_SUMMARY.txt"
TUCKER_BATCH_DIR = TUCKER_DIR / "IMAGE_BATCHES"
TUCKER_PREP_BATCH_DIR = TUCKER_DIR / "IMAGE_PREP_BATCHES"
BATCH_SIZE = 250


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).replace("\xa0", " ").strip()


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


def write_dict_xlsx(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Tucker Images"
    sheet.append(fieldnames)
    for row in rows:
        sheet.append([row.get(field, "") for field in fieldnames])
    workbook.save(path)


def clear_directory(path: Path) -> None:
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_file():
            child.unlink()
        elif child.is_dir():
            shutil.rmtree(child)


def build_tucker_inventory() -> int:
    master_rows = read_csv_rows(MASTER_INVENTORY_PATH)
    master_header_index = next(index for index, row in enumerate(master_rows) if "Default Vendor Name" in row and "SKU" in row)
    master_headers = master_rows[master_header_index]
    master_vendor_index = master_headers.index("Default Vendor Name")
    master_sku_index = master_headers.index("SKU")
    tucker_skus = {
        clean_text(row[master_sku_index])
        for row in master_rows[master_header_index + 1 :]
        if len(row) > max(master_vendor_index, master_sku_index) and clean_text(row[master_vendor_index]) == "Tucker"
    }

    upload_source = PRIMARY_FULL_UPLOAD_PATH if PRIMARY_FULL_UPLOAD_PATH.exists() else FULL_UPLOAD_PATH
    upload_rows = read_csv_rows(upload_source)
    upload_header_index = next(index for index, row in enumerate(upload_rows) if "SKU" in row)
    upload_headers = upload_rows[upload_header_index]
    upload_sku_index = upload_headers.index("SKU")
    filtered = upload_rows[: upload_header_index + 1] + [
        row
        for row in upload_rows[upload_header_index + 1 :]
        if len(row) > upload_sku_index and clean_text(row[upload_sku_index]) in tucker_skus
    ]
    write_csv_rows(TUCKER_UPLOAD_PATH, filtered)
    return max(0, len(filtered) - (upload_header_index + 1))


def build_tucker_images() -> int:
    rows = read_dict_rows(IMAGE_QUEUE_PATH)
    filtered = [row for row in rows if clean_text(row.get("Default Vendor Name")) == "Tucker"]
    fieldnames = list(filtered[0].keys()) if filtered else []
    write_dict_csv(TUCKER_QUEUE_CSV_PATH, fieldnames, filtered)
    write_dict_xlsx(TUCKER_QUEUE_XLSX_PATH, fieldnames, filtered)

    clear_directory(TUCKER_BATCH_DIR)
    TUCKER_BATCH_DIR.mkdir(parents=True, exist_ok=True)
    total_batches = math.ceil(len(filtered) / BATCH_SIZE) if filtered else 0
    for batch_number in range(1, total_batches + 1):
        start = (batch_number - 1) * BATCH_SIZE
        end = start + BATCH_SIZE
        batch_rows = filtered[start:end]
        batch_label = f"batch_{batch_number:03d}"
        batch_folder = TUCKER_BATCH_DIR / batch_label
        image_folder = batch_folder / "images"
        batch_folder.mkdir(parents=True, exist_ok=True)
        image_folder.mkdir(parents=True, exist_ok=True)
        for row_index, row in enumerate(batch_rows, start=1):
            row["Batch Number"] = str(batch_number)
            row["Batch Row"] = str(row_index)
            image_path = Path(clean_text(row.get("Local Image Absolute Path")))
            if image_path.is_file():
                shutil.copy2(image_path, image_folder / image_path.name)
        write_dict_csv(batch_folder / f"{batch_label}.csv", fieldnames, batch_rows)

    lines = [
        f"Tucker inventory file: {TUCKER_UPLOAD_PATH}",
        f"Tucker image queue: {TUCKER_QUEUE_CSV_PATH}",
        f"Tucker image workbook: {TUCKER_QUEUE_XLSX_PATH}",
        f"Tucker batch folder: {TUCKER_BATCH_DIR}",
        f"Tucker image rows: {len(filtered)}",
        f"Tucker image batches: {total_batches}",
        "",
        "Use only this folder for Tucker review and image matching.",
    ]
    TUCKER_SUMMARY_PATH.write_text("\n".join(lines), encoding="utf-8")
    return len(filtered)


def build_tucker_image_prep() -> tuple[int, int, int]:
    rows = read_dict_rows(IMAGE_DATABASE_PATH)
    filtered = [row for row in rows if clean_text(row.get("Default Vendor Name")) == "Tucker"]
    prep_rows: list[dict[str, str]] = []
    missing_rows: list[dict[str, str]] = []
    for row in filtered:
        has_local = clean_text(row.get("Has Local Image")) == "Y"
        payload = {
            "Batch Number": "",
            "Batch Row": "",
            "SKU": clean_text(row.get("SKU")),
            "Item Name": clean_text(row.get("Item Name")),
            "Customer-facing Name": clean_text(row.get("Customer-facing Name")),
            "Reporting Category": clean_text(row.get("Reporting Category")),
            "Default Vendor Name": clean_text(row.get("Default Vendor Name")),
            "Default Vendor Code": clean_text(row.get("Default Vendor Code")),
            "Local Image Filename": Path(clean_text(row.get("Local Image Absolute Path"))).name,
            "Local Image Relative Path": clean_text(row.get("Local Image Relative Path")),
            "Local Image Absolute Path": clean_text(row.get("Local Image Absolute Path")),
            "Website Image URL": clean_text(row.get("Website Image URL")),
            "Has Local Image": clean_text(row.get("Has Local Image")),
            "Has Website Image": clean_text(row.get("Has Website Image")),
            "Has Any Image": clean_text(row.get("Has Any Image")),
        }
        if has_local:
            prep_rows.append(payload)
        else:
            missing_rows.append(payload)

    fieldnames = list(prep_rows[0].keys()) if prep_rows else [
        "Batch Number",
        "Batch Row",
        "SKU",
        "Item Name",
        "Customer-facing Name",
        "Reporting Category",
        "Default Vendor Name",
        "Default Vendor Code",
        "Local Image Filename",
        "Local Image Relative Path",
        "Local Image Absolute Path",
        "Website Image URL",
        "Has Local Image",
        "Has Website Image",
        "Has Any Image",
    ]
    write_dict_csv(TUCKER_PREP_CSV_PATH, fieldnames, prep_rows)
    write_dict_xlsx(TUCKER_PREP_XLSX_PATH, fieldnames, prep_rows)
    write_dict_csv(TUCKER_MISSING_IMAGE_PATH, fieldnames, missing_rows)

    clear_directory(TUCKER_PREP_BATCH_DIR)
    TUCKER_PREP_BATCH_DIR.mkdir(parents=True, exist_ok=True)
    total_batches = math.ceil(len(prep_rows) / BATCH_SIZE) if prep_rows else 0
    for batch_number in range(1, total_batches + 1):
        start = (batch_number - 1) * BATCH_SIZE
        end = start + BATCH_SIZE
        batch_rows = prep_rows[start:end]
        batch_label = f"batch_{batch_number:03d}"
        batch_folder = TUCKER_PREP_BATCH_DIR / batch_label
        image_folder = batch_folder / "images"
        batch_folder.mkdir(parents=True, exist_ok=True)
        image_folder.mkdir(parents=True, exist_ok=True)
        for row_index, row in enumerate(batch_rows, start=1):
            row["Batch Number"] = str(batch_number)
            row["Batch Row"] = str(row_index)
            image_path = Path(clean_text(row.get("Local Image Absolute Path")))
            if image_path.is_file():
                shutil.copy2(image_path, image_folder / image_path.name)
        write_dict_csv(batch_folder / f"{batch_label}.csv", fieldnames, batch_rows)

    return len(prep_rows), len(missing_rows), total_batches


def write_readme(tucker_rows: int, tucker_image_rows: int, prep_rows: int, missing_rows: int, prep_batches: int) -> None:
    lines = [
        "Use this folder only for Tucker.",
        f"TUCKER_ONLY_INVENTORY.csv = Tucker-only rows from the full Square upload file ({tucker_rows} rows).",
        f"TUCKER_IMAGE_QUEUE.csv = Tucker rows that already match the current Square export ({tucker_image_rows} rows).",
        f"TUCKER_IMAGE_PREP_QUEUE.csv = all Tucker rows with local images, ready for post-upload matching ({prep_rows} rows).",
        f"TUCKER_IMAGE_MISSING.csv = Tucker rows still missing a local image ({missing_rows} rows).",
        "IMAGE_BATCHES/ = Tucker rows already matched to the current Square export.",
        f"IMAGE_PREP_BATCHES/ = all Tucker image-prep batches ({prep_batches} batches).",
    ]
    (TUCKER_DIR / "README.txt").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    if not FULL_UPLOAD_PATH.exists() and not PRIMARY_FULL_UPLOAD_PATH.exists():
        raise FileNotFoundError(f"Missing {PRIMARY_FULL_UPLOAD_PATH}")
    if not IMAGE_QUEUE_PATH.exists():
        raise FileNotFoundError(f"Missing {IMAGE_QUEUE_PATH}")

    TUCKER_DIR.mkdir(parents=True, exist_ok=True)
    tucker_rows = build_tucker_inventory()
    tucker_image_rows = build_tucker_images()
    prep_rows, missing_rows, prep_batches = build_tucker_image_prep()
    write_readme(tucker_rows, tucker_image_rows, prep_rows, missing_rows, prep_batches)

    print(f"Tucker inventory rows: {tucker_rows}")
    print(f"Tucker image rows: {tucker_image_rows}")
    print(f"Tucker image prep rows: {prep_rows}")
    print(f"Tucker image missing rows: {missing_rows}")
    print(f"Tucker image prep batches: {prep_batches}")
    print(f"Tucker folder: {TUCKER_DIR}")


if __name__ == "__main__":
    main()
