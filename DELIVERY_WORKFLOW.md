# Delivery Workflow

This is the target delivery workflow for vendor invoices, packing slips, and other receiving documents.

## Goal

Turn any delivery document into a standardized receiving package that:

- matches invoice lines to inventory
- updates quantities safely
- reviews cost changes without lowering sell prices automatically
- flags missing or unclear matches
- publishes clean outputs into `UPLOAD READY\<date>\V n\DELIVERIES`

## Supported Inputs

- vendor invoice PDF
- packing slip PDF
- scanned PDF or image-based invoice
- CSV/XLSX delivery export
- manual receiving log

## Standard Internal Line Format

Every input should be normalized into this line-item shape before matching:

- `transaction_date`
- `vendor`
- `reference`
- `document_type`
- `line_number`
- `sku`
- `vendor_code`
- `gtin`
- `item_name`
- `quantity_received`
- `unit_cost`
- `extended_cost`
- `notes`

The existing starter file is:
[delivery_log_template.csv](/C:/Codex/AZCS%20Inventory/templates/delivery_log_template.csv)

## Matching Order

Match delivery rows to inventory in this order:

1. exact `SKU`
2. exact `Default Vendor Code`
3. exact `GTIN`
4. exact normalized `Item Name` within the same vendor
5. fuzzy normalized `Item Name` within the same vendor
6. fuzzy cross-vendor only if confidence is high and sent to review

## Business Rules

- quantities update only when the match is reliable
- price floors are protected by your approved baseline source
- costs can change from the invoice
- sell prices do not automatically drop
- store activity rules remain enforced
- online visibility rules remain enforced
- low-confidence rows go to review, not directly into the inventory import

## Output Package

Each delivery run should publish to:

```text
UPLOAD READY
└── <M.D.YY>
    └── V <n>
        └── DELIVERIES
            ├── DELIVERY_MATCHED.csv
            ├── DELIVERY_REVIEW.csv
            ├── DELIVERY_NEW_ITEMS.csv
            ├── DELIVERY_QUANTITY_UPDATE.csv
            ├── DELIVERY_COST_REVIEW.csv
            └── DELIVERY_SUMMARY.txt
```

## Output File Purpose

`DELIVERY_MATCHED.csv`
- all invoice lines with their matched inventory row

`DELIVERY_REVIEW.csv`
- rows that need human review because the match is uncertain

`DELIVERY_NEW_ITEMS.csv`
- rows that appear to be real new products missing from inventory

`DELIVERY_QUANTITY_UPDATE.csv`
- clean quantity updates ready to apply to inventory/location counts

`DELIVERY_COST_REVIEW.csv`
- invoice cost changes compared against current cost and price floor context

`DELIVERY_SUMMARY.txt`
- one-page run summary with counts and exceptions

## Delivery-Only Square Upload

If nothing else is changing, the Square import should contain only the items on the delivery.

That means the delivery upload should:

- include only matched delivered items
- update only quantity, cost, and any approved receiving fields
- leave unrelated inventory rows out of the file
- preserve protected price floors unless you explicitly approve a price change

Starter file:
[delivery_inventory_upload_template.csv](/C:/Codex/AZCS%20Inventory/templates/delivery_inventory_upload_template.csv)

## Example Run

For a file like:
[Invoice PI00057612 for AZ Cleaning Supplies (1).pdf](C:/Users/JRAZC/Downloads/Invoice%20PI00057612%20for%20AZ%20Cleaning%20Supplies%20(1).pdf)

The workflow should be:

1. extract the invoice into normalized lines
2. match those lines against the current active upload
3. split rows into matched, review, and new-item buckets
4. build quantity and cost review outputs
5. publish the package into `UPLOAD READY\<date>\V n\DELIVERIES`

## Quality Standard

A delivery run is not considered complete unless:

- every source line appears in one output bucket
- no line is silently dropped
- every matched line has a clear inventory target
- every unmatched line is visible in review/new-items output
- cost changes are visible before price changes
- output filenames and folder structure match the upload-ready standard
