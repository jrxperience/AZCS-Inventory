# AZCS Inventory Workflow

This repo now supports seven repeatable jobs:

1. Rebuild the master Square catalog from vendor price lists.
2. Track deliveries and manual stock adjustments by SKU.
3. Track selling-price overrides by SKU.
4. Generate Square-ready quantity and price update files.
5. Generate strategic selling-price recommendations for the full catalog.
6. Match Square sales history back to the catalog for pricing and review.
7. Build an after-hours receiving import from a fresh Square export plus the current delivery batch.

## Folder layout

- `inputs/price_lists/`
  Put vendor price lists here. The inventory builder uses the newest matching file for each vendor pattern.
- `inputs/deliveries/`
  Put one or more delivery CSV or XLSX files here. Use [`templates/delivery_log_template.csv`](/C:/Codex/AZCS%20Inventory/templates/delivery_log_template.csv) as the starting format. The receiving workflow can match by `SKU`, `Vendor Code`, `GTIN`, or exact item name.
- `inputs/adjustments/`
  Put manual stock adjustments or opening balances here. Use [`templates/inventory_adjustments_template.csv`](/C:/Codex/AZCS%20Inventory/templates/inventory_adjustments_template.csv).
- `inputs/square_exports/`
  Put the fresh Square item-library export for that night's receiving session here. The receiving workflow uses the newest export file in this folder as the live quantity baseline.
- `inputs/price_updates/`
  Put selling-price changes here. Use [`templates/price_updates_template.csv`](/C:/Codex/AZCS%20Inventory/templates/price_updates_template.csv).
- `inputs/pricing_overrides/`
  Optional manual exceptions for the pricing engine. Use [`templates/pricing_overrides_template.csv`](/C:/Codex/AZCS%20Inventory/templates/pricing_overrides_template.csv) when you want to force a price or target margin for a specific SKU.
- `inputs/sales/`
  Put Square `Sales by item` exports here when you want to match sales history back to the catalog.
- `inputs/sales_match_overrides/`
  Optional manual match fixes for store-created sales SKUs. Use [`templates/sales_match_overrides_template.csv`](/C:/Codex/AZCS%20Inventory/templates/sales_match_overrides_template.csv).
- `inputs/verified_product_enrichment.csv`
  Optional manual overrides for high-confidence GTIN fixes and verified SEO metadata. Use [`templates/verified_product_enrichment_template.csv`](/C:/Codex/AZCS%20Inventory/templates/verified_product_enrichment_template.csv).
- `templates/`
  Holds the Square import template and transaction templates.
- `outputs/`
  Generated inventory and stock files land here.

## Rebuild the master inventory

Run:

```powershell
python build_master_inventory.py
```

This generates:

- `outputs/square_master_inventory.csv`
- `outputs/square_master_inventory_overlap_review.csv`
- `outputs/square_master_inventory_summary.txt`
- `outputs/product_enrichment_audit.csv`
- `outputs/inventory_database_with_images.csv`
- `outputs/inventory_database_with_images.xlsx`
- `outputs/product_image_match_audit.csv`

Current dedupe behavior:

- Exact cross-vendor item matches are merged into one inventory row.
- When multiple possible costs exist for the same merged item, the higher `Default Unit Cost` wins.
- Same-vendor duplicate names are either merged when they look truly duplicate, or renamed with case-pack / GTIN / SKU labels when they look like separate variants.
- GTIN values only populate when they pass checksum validation, come from a verified manual override, or are inferred from an exact manufacturer-style code already tied to a validated GTIN elsewhere in the catalog.
- SEO title, SEO description, social metadata, and permalinks are generated automatically for every row.
- Live website enrichment is currently enabled for MPWSR and Barens. It uses their Shopify product feeds as a primary source for exact SKU matches and unique exact-title matches, and can fill customer-facing names, descriptions, permalinks, and weights when those fields are published.
- JRacenstein enrichment now uses the live Storefront catalog for exact product or variant code matches. When a code match is exact, it can fill customer-facing names, descriptions, GTINs, weights, and stronger permalinks.
- Gold Assassin enrichment is enabled for exact live manufacturer SKUs found in the catalog. It can tighten names/descriptions/permalinks and fill missing weights when the manufacturer site publishes them clearly.
- Direct product-page enrichment is also enabled for Trident and EaCo Chem. It uses exact base product-name matches and preserves the existing pack-size suffix in your catalog rows.
- Local image matching is enabled for the `Images/` folder. Image paths are added to the internal inventory database exports and image-audit file, but not to the Square import file because Square's item import template does not include image columns.

## Track deliveries, stock, and selling prices

1. Add delivery CSVs to `inputs/deliveries/`.
2. Add opening balances or manual adjustments to `inputs/adjustments/`.
3. Add selling-price changes to `inputs/price_updates/`.
4. Run:

```powershell
python build_stock_snapshot.py
```

This generates:

- `outputs/current_stock_snapshot.csv`
- `outputs/current_pricing_snapshot.csv`
- `outputs/square_inventory_quantity_update.csv`
- `outputs/square_catalog_price_update.csv`
- `outputs/stock_transaction_issues.csv`
- `outputs/stock_snapshot_summary.txt`

## Build an after-hours receiving import

Use this when you want to:

- export the live item library from Square after hours
- add that night's packing slip or order details
- import only the changed quantity rows back into Square

Run:

```powershell
python build_receiving_import.py
```

This expects:

1. the newest Square export in `inputs/square_exports/`
2. the current delivery batch in `inputs/deliveries/`
3. any same-night manual quantity corrections in `inputs/adjustments/`

This generates:

- `outputs/square_receiving_update.csv`
- `outputs/square_receiving_update.xlsx`
- `outputs/receiving_update_audit.csv`
- `outputs/receiving_update_issues.csv`
- `outputs/receiving_update_summary.txt`

Receiving behavior:

- The workflow uses the newest Square export as the live quantity baseline.
- It updates only the `New Quantity ...` column for the target location and only for SKUs with stock activity.
- It leaves untouched rows out of the import file so the reimport only changes the delivery batch.
- It matches delivery rows by `SKU` first, then falls back to master-catalog `Vendor Code`, `GTIN`, and exact item name when possible.
- It is designed for same-session after-hours export/import windows, not for daytime use while sales are still coming in.

## Build strategic pricing recommendations

Run:

```powershell
python build_pricing_recommendations.py
```

This generates:

- `outputs/pricing_recommendations.csv`
- `outputs/pricing_recommendations.xlsx`
- `outputs/square_master_inventory_strategic_pricing.csv`
- `outputs/square_catalog_strategic_price_update.csv`
- `outputs/pricing_strategy_summary.txt`
- `outputs/pricing_strategy_issues.csv`

Pricing behavior:

- The engine assumes you want roughly a `30%` average gross margin across the catalog, not a flat `30%` markup.
- Missing prices are filled automatically from cost, product type, and rounding rules.
- Current prices that already sit inside an acceptable band are preserved.
- Very low-margin items can be raised automatically to the strategic floor.
- Trusted matched sales history can also support strategic price increases on proven sellers.
- Higher-margin items are preserved unless you manually override them.
- Chemicals, premium tools, and bundles target stronger margins than commodity parts and larger equipment.

## Build a sales match audit

Run:

```powershell
python build_sales_match_audit.py
```

This generates:

- `outputs/sales_item_match_audit.csv`
- `outputs/sales_item_match_audit.xlsx`
- `outputs/sales_item_match_review.csv`
- `outputs/sales_catalog_signals.csv`
- `outputs/sales_match_summary.txt`
- `outputs/sales_match_issues.csv`

Sales-match behavior:

- Exact catalog SKU matches are accepted automatically.
- Exact catalog vendor-code and exact normalized-name hits can also be accepted automatically when they are unique.
- Strong fuzzy matches are accepted only when the score is high enough and clearly better than the next candidate.
- Weaker fuzzy rows are pushed into the review file instead of being forced into the catalog.
- Manual overrides let you pin odd Square-only SKUs to the right catalog SKU one time and reuse that fix later.

## Recommended day-to-day process

1. Drop new vendor price lists into `inputs/price_lists/`.
2. Run `python build_master_inventory.py`.
3. Review `outputs/square_master_inventory_overlap_review.csv`.
4. If you want sales-aware pricing, drop a Square `Sales by item` CSV into `inputs/sales/` and run `python build_sales_match_audit.py`.
5. Run `python build_pricing_recommendations.py`.
6. Review `outputs/pricing_recommendations.xlsx` and `outputs/sales_item_match_review.csv`.
7. Add deliveries, adjustments, and price updates.
8. If you are doing after-hours receiving from a fresh Square export, drop that export into `inputs/square_exports/` and run `python build_receiving_import.py`.
9. Run `python build_stock_snapshot.py`.
10. Upload the quantity or price update CSVs into Square as needed.

## Notes

- Keep old source files if you want history; the builder uses the newest matching file in `inputs/price_lists/`.
- Delivery and adjustment files should use SKUs from the latest master inventory.
- Archive or remove completed delivery/adjustment files after each receiving session, or they will be counted again on the next run.
- Price update files should also use SKUs from the latest master inventory.
- Any stock rows with missing or unknown SKUs are written to `outputs/stock_transaction_issues.csv`.
