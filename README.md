# AZCS Inventory Workflow

This repo now supports four repeatable jobs:

1. Rebuild the master Square catalog from vendor price lists.
2. Track deliveries and manual stock adjustments by SKU.
3. Track selling-price overrides by SKU.
4. Generate Square-ready quantity and price update files.

## Folder layout

- `inputs/price_lists/`
  Put vendor price lists here. The inventory builder uses the newest matching file for each vendor pattern.
- `inputs/deliveries/`
  Put one or more delivery CSVs here. Use [`templates/delivery_log_template.csv`](/C:/Codex/AZCS%20Inventory/templates/delivery_log_template.csv) as the starting format.
- `inputs/adjustments/`
  Put manual stock adjustments or opening balances here. Use [`templates/inventory_adjustments_template.csv`](/C:/Codex/AZCS%20Inventory/templates/inventory_adjustments_template.csv).
- `inputs/price_updates/`
  Put selling-price changes here. Use [`templates/price_updates_template.csv`](/C:/Codex/AZCS%20Inventory/templates/price_updates_template.csv).
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

## Recommended day-to-day process

1. Drop new vendor price lists into `inputs/price_lists/`.
2. Run `python build_master_inventory.py`.
3. Review `outputs/square_master_inventory_overlap_review.csv`.
4. Add deliveries, adjustments, and price updates.
5. Run `python build_stock_snapshot.py`.
6. Upload the quantity or price update CSVs into Square as needed.

## Notes

- Keep old source files if you want history; the builder uses the newest matching file in `inputs/price_lists/`.
- Delivery and adjustment files should use SKUs from the latest master inventory.
- Price update files should also use SKUs from the latest master inventory.
- Any stock rows with missing or unknown SKUs are written to `outputs/stock_transaction_issues.csv`.
