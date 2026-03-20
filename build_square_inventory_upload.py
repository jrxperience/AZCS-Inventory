from __future__ import annotations

import csv
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
TO_IMPORT_DIR = BASE_DIR / "to_import"
OUTPUT_DIR = BASE_DIR / "outputs"
DEFAULT_SOURCE_PATH = OUTPUT_DIR / "square_master_inventory.csv"
DEFAULT_TEMPLATE_PATH = Path(r"C:\Users\JRAZC\Downloads\MLT3E97CHP443_catalog-2026-03-20-2039.csv")


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
    raise ValueError(f"Could not find a header row containing {required!r}.")


def rows_to_dicts(headers: list[str], rows: list[list[str]]) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for row in rows:
        if not any(clean_text(cell) for cell in row):
            continue
        padded = row[: len(headers)] + [""] * max(0, len(headers) - len(row))
        record = {headers[index]: clean_text(padded[index]) for index in range(len(headers))}
        records.append(record)
    return records


def main() -> None:
    if not DEFAULT_SOURCE_PATH.exists():
        raise FileNotFoundError(f"Missing source inventory file: {DEFAULT_SOURCE_PATH}")
    if not DEFAULT_TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Missing Square template file: {DEFAULT_TEMPLATE_PATH}")

    source_rows = read_csv_rows(DEFAULT_SOURCE_PATH)
    template_rows = read_csv_rows(DEFAULT_TEMPLATE_PATH)

    source_header_index = find_header_index(source_rows, "SKU")
    template_header_index = find_header_index(template_rows, "SKU")

    source_headers = [clean_text(value) for value in source_rows[source_header_index]]
    template_headers = [clean_text(value) for value in template_rows[template_header_index]]
    source_records = rows_to_dicts(source_headers, source_rows[source_header_index + 1 :])

    output_name = f"inventory_upload_{DEFAULT_TEMPLATE_PATH.stem.replace('MLT3E97CHP443_catalog-', '')}.csv"
    output_path = TO_IMPORT_DIR / output_name

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        for row in template_rows[: template_header_index]:
            writer.writerow(row)
        writer.writerow(template_headers)
        for record in source_records:
            output_row = [clean_text(record.get(header, "")) for header in template_headers]
            writer.writerow(output_row)

    print(f"Square inventory upload: {output_path}")
    print(f"Template used: {DEFAULT_TEMPLATE_PATH}")
    print(f"Rows written: {len(source_records)}")


if __name__ == "__main__":
    main()
