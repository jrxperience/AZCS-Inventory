"""
build_stock_snapshot_json.py
Generates docs/stock_data.json from the latest Square catalog export.

Stock logic:
  In Stock          → qty > 0  (actively in stock)
  Recently Out       → qty < 0  (sold past zero — was actively moving)
  Skip               → qty = 0  (never stocked or no history)

Source priority:
  1. inputs/square_exports/   (any .xlsx or .csv matching MLT* or *export*)
  2. ~/Downloads/             (same pattern)

Run this, then push to GitHub to update the live site.
"""

import pandas as pd
import json
import glob
import os
from pathlib import Path


SEARCH_DIRS = [
    Path(__file__).parent / "inputs" / "square_exports",
    Path.home() / "Downloads",
]
PATTERNS = ["MLT*.xlsx", "MLT*.csv", "*export*.csv", "*export*.xlsx"]


def find_latest_export():
    candidates = []
    for d in SEARCH_DIRS:
        if not d.exists():
            continue
        for pat in PATTERNS:
            candidates.extend(glob.glob(str(d / pat)))
    if not candidates:
        return None
    return max(candidates, key=os.path.getmtime)


def to_float(val):
    try:
        return float(str(val).replace("$", "").replace(",", "").strip())
    except Exception:
        return 0.0


def read_export(path):
    p = Path(path)
    if p.suffix.lower() == ".csv":
        return pd.read_csv(path, dtype=str).fillna("")
    df = pd.read_excel(path, dtype=str, header=0).fillna("")
    if "Token" not in df.columns and "Item Name" not in df.columns:
        df = pd.read_excel(path, dtype=str, header=1).fillna("")
    return df


def build_snapshot():
    base = Path(__file__).parent
    docs_path = base / "docs"
    docs_path.mkdir(exist_ok=True)

    export_path = find_latest_export()
    if not export_path:
        print("ERROR: No Square catalog export found.")
        print("  Place a Square item-library export in inputs/square_exports/ or ~/Downloads/")
        return

    print(f"Using: {export_path}")
    df = read_export(export_path)
    df.columns = [c.strip() for c in df.columns]

    # Non-archived only
    if "Archived" in df.columns:
        df = df[~df["Archived"].str.strip().str.upper().isin(["Y", "TRUE", "YES", "1"])]

    # Keep: Stockable=Y, OR blank Stockable with Item Type = Physical good
    if "Stockable" in df.columns:
        stockable_yes   = df["Stockable"].str.strip().str.upper().isin(["Y", "TRUE", "YES", "1"])
        stockable_blank = (df["Stockable"].str.strip() == "")
        physical_good   = df.get("Item Type", pd.Series([""] * len(df))).str.strip().str.lower() == "physical good"
        df = df[stockable_yes | (stockable_blank & physical_good)]

    items_in  = []
    items_out = []

    for _, row in df.iterrows():
        qty_az   = to_float(row.get("Current Quantity AZ Cleaning Supplies", 0))
        qty_azcs = to_float(row.get("Current Quantity AZCS", 0))
        total_qty = qty_az + qty_azcs

        # Skip items with zero qty — no activity history
        if total_qty == 0:
            continue

        price = to_float(row.get("Price", 0))
        if price == 0:
            price = to_float(row.get("Price AZ Cleaning Supplies", 0))
        cost = to_float(row.get("Default Unit Cost", 0))

        name = (row.get("Customer-facing Name", "") or row.get("Item Name", "")).strip()
        if not name:
            continue

        record = {
            "name":        name,
            "sku":         row.get("SKU", "").strip(),
            "vendor":      row.get("Default Vendor Name", "").strip(),
            "vendor_code": row.get("Default Vendor Code", "").strip(),
            "category":    row.get("Categories", "").strip(),
            "qty_az":      int(qty_az),
            "qty_azcs":    int(qty_azcs),
            "total_qty":   int(total_qty),
            "price":       round(price, 2),
            "cost":        round(cost, 2),
        }

        if total_qty > 0:
            items_in.append(record)
        else:  # total_qty < 0
            items_out.append(record)

    items_in  = sorted(items_in,  key=lambda x: x["name"].lower())
    items_out = sorted(items_out, key=lambda x: x["name"].lower())

    snapshot = {
        "generated":    pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
        "source":       os.path.basename(export_path),
        "in_stock":     items_in,
        "out_of_stock": items_out,
    }

    output_path = docs_path / "stock_data.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    print(f"Generated: {output_path}")
    print(f"  In stock:           {len(items_in):>5} items")
    print(f"  Recently out:       {len(items_out):>5} items (negative qty)")
    print(f"\nPush docs/ to GitHub to update the live site.")


if __name__ == "__main__":
    build_snapshot()
