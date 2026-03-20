from __future__ import annotations

import csv
import re
import shutil
from collections import defaultdict
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
IMAGE_DATABASE_CSV = BASE_DIR / "outputs" / "inventory_database_with_images.csv"
TARGET_DIR = BASE_DIR / "square_ready" / "TUCKER_IMAGES_BY_CATEGORY"
MANIFEST_CSV = TARGET_DIR / "TUCKER_IMAGE_CATEGORY_MAP.csv"
README_PATH = TARGET_DIR / "README.txt"


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).replace("\xa0", " ").strip()


def safe_folder_name(value: str) -> str:
    cleaned = clean_text(value)
    if ">" in cleaned:
        cleaned = cleaned.split(">", 1)[1]
    cleaned = re.sub(r'[<>:"/\\|?*]+', "-", cleaned).strip(" .")
    return cleaned or "Uncategorized"


def clear_directory(path: Path) -> None:
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_file():
            child.unlink()
        elif child.is_dir():
            shutil.rmtree(child)


def main() -> None:
    if not IMAGE_DATABASE_CSV.exists():
        raise FileNotFoundError(f"Missing {IMAGE_DATABASE_CSV}")

    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    clear_directory(TARGET_DIR)
    TARGET_DIR.mkdir(parents=True, exist_ok=True)

    category_counts: dict[str, int] = defaultdict(int)
    manifest_rows: list[dict[str, str]] = []

    with IMAGE_DATABASE_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if clean_text(row.get("Default Vendor Name")) != "Tucker":
                continue
            if clean_text(row.get("Has Local Image")) != "Y":
                continue
            image_path = Path(clean_text(row.get("Local Image Absolute Path")))
            if not image_path.is_file():
                continue

            category = clean_text(row.get("Categories") or row.get("Reporting Category") or "Uncategorized")
            folder_name = safe_folder_name(category)
            folder_path = TARGET_DIR / folder_name
            folder_path.mkdir(parents=True, exist_ok=True)
            destination = folder_path / image_path.name
            if not destination.exists():
                shutil.copy2(image_path, destination)
            category_counts[folder_name] += 1
            manifest_rows.append(
                {
                    "SKU": clean_text(row.get("SKU")),
                    "Item Name": clean_text(row.get("Item Name")),
                    "Category": category,
                    "Category Folder": folder_name,
                    "Image Filename": image_path.name,
                    "Image Absolute Path": str(image_path),
                }
            )

    with MANIFEST_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["SKU", "Item Name", "Category", "Category Folder", "Image Filename", "Image Absolute Path"],
        )
        writer.writeheader()
        writer.writerows(manifest_rows)

    lines = ["Tucker images sorted by category."]
    for folder_name in sorted(category_counts):
        lines.append(f"{folder_name}: {category_counts[folder_name]}")
    README_PATH.write_text("\n".join(lines), encoding="utf-8")

    print(f"Tucker category image folder: {TARGET_DIR}")
    for folder_name in sorted(category_counts):
        print(f"{folder_name}: {category_counts[folder_name]}")


if __name__ == "__main__":
    main()
