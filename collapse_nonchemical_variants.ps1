param(
    [string]$InputPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_FINAL.csv",
    [string]$OutputPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_AGGRESSIVE.csv",
    [string]$SummaryPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_AGGRESSIVE.txt",
    [string]$ReportPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_AGGRESSIVE_REDUCTION_REPORT.csv",
    [string]$BaselinePricePath = "C:\Users\JRAZC\Downloads\MLT3E97CHP443_catalog-2026-03-20-2210.csv"
)

$ErrorActionPreference = "Stop"

function Get-DecimalValue {
    param([string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return [decimal]0
    }

    $clean = $Value.Replace('$', '').Trim()
    $parsed = [decimal]0
    if ([decimal]::TryParse($clean, [ref]$parsed)) {
        return $parsed
    }
    return [decimal]0
}

function Normalize-NameFloorKey {
    param([string]$Text)

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return ""
    }

    $value = $Text.ToUpperInvariant()
    $value = [regex]::Replace($value, '[^A-Z0-9]+', ' ')
    $value = [regex]::Replace($value, '\b(WITH|W|FOR|AND|THE|OF|REGULAR|COMPLETE)\b', ' ')
    $value = [regex]::Replace($value, '\s+', ' ').Trim()
    return $value
}

function Is-ChemicalLike {
    param($Row)

    $text = @(
        $Row.'Item Name'
        $Row.Categories
        $Row.'Default Vendor Name'
        $Row.Description
    ) -join ' '
    $upper = $text.ToUpperInvariant()

    if ($upper -match 'SEALER|STAIN|CLEANER|DEGREASER|DETERGENT|SOAP|STRIP|OX|RESTORE|PRESERVE|BLEACH|ACID|MORTAR|MASONRY|BIO BASED|ASSASSIN|BULLSEYE|NMD|EFLO|NEUTRA|GALLON|GAL\b|DRUM|PAIL') {
        return $true
    }

    if ($Row.'Default Vendor Name' -match 'EacoChem|Trident|EnviroBioCleaner|Front9|RCCT') {
        return $true
    }

    return $false
}

function Normalize-VariantBase {
    param([string]$Text)

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return ""
    }

    $value = $Text.ToUpperInvariant()
    $value = [regex]::Replace($value, '\b(\d+\.?\d*)\s*(INCH|IN|FT|FOOT|FEET|MM|CM|OZ|OUNCE|LB|LBS|PACK|PK|CASE|DEGREE|GPM|GAL|GALLON|GALLONS)\b', ' ')
    $value = [regex]::Replace($value, '\b\d+\/?\d*\b', ' ')
    $value = [regex]::Replace($value, '[^A-Z]+', ' ')
    $value = [regex]::Replace($value, '\b(WITH|W|FOR|AND|THE|OF|REGULAR|COMPLETE|REPLACEMENT|REFILL|KIT)\b', ' ')
    $value = [regex]::Replace($value, '\s+', ' ').Trim()
    return $value
}

function Get-TextLength {
    param([string]$Value)
    if ([string]::IsNullOrEmpty($Value)) {
        return 0
    }
    return $Value.Length
}

$rawLines = Get-Content $InputPath
if ($rawLines.Length -lt 6) {
    throw "Input file does not look like a Square import template: $InputPath"
}

$templatePrefix = $rawLines[0..3]
$csvPayload = $rawLines[4..($rawLines.Length - 1)] -join "`r`n"
$rows = $csvPayload | ConvertFrom-Csv

$baselineRows = Import-Csv $BaselinePricePath
$baselineBySku = @{}
$baselineByVendorCode = @{}
$baselineByName = @{}
$manualFloorBySku = @{
    "TRIDEN-HURRICANE-CAT-5-24-KITS-KIT" = [decimal]609.99
    "TDS-HURRICAT5K" = [decimal]679.00
    "TDS-HURRICAT5H" = [decimal]345.99
}
foreach ($row in $baselineRows) {
    $price = Get-DecimalValue $row.Price
    if ($price -le 0) {
        continue
    }

    if (-not [string]::IsNullOrWhiteSpace($row.SKU)) {
        $key = $row.SKU.Trim().ToUpperInvariant()
        if (-not $baselineBySku.ContainsKey($key) -or $price -gt $baselineBySku[$key]) {
            $baselineBySku[$key] = $price
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($row.'Default Vendor Code')) {
        $key = $row.'Default Vendor Code'.Trim().ToUpperInvariant()
        if (-not $baselineByVendorCode.ContainsKey($key) -or $price -gt $baselineByVendorCode[$key]) {
            $baselineByVendorCode[$key] = $price
        }
    }

    $nameKey = Normalize-NameFloorKey $row.'Item Name'
    if ($nameKey) {
        if (-not $baselineByName.ContainsKey($nameKey) -or $price -gt $baselineByName[$nameKey]) {
            $baselineByName[$nameKey] = $price
        }
    }
}

$resultRows = New-Object System.Collections.Generic.List[object]
$reportRows = New-Object System.Collections.Generic.List[object]
$collapsedGroups = 0
$droppedRows = 0
$baselineFloorAdjustments = 0

$chemicalRows = $rows | Where-Object { Is-ChemicalLike $_ }
$nonChemicalRows = $rows | Where-Object { -not (Is-ChemicalLike $_) }

# Keep all chemical rows as-is.
foreach ($row in $chemicalRows) {
    $row.Archived = "N"
    $row.Sellable = "Y"
    $row.'Enabled AZ Cleaning Supplies' = "Y"
    $row.'Enabled AZCS' = "Y"
    $resultRows.Add($row)
}

$groups = $nonChemicalRows | Group-Object { Normalize-VariantBase $_.'Item Name' }
foreach ($group in $groups) {
    if ([string]::IsNullOrWhiteSpace($group.Name) -or $group.Count -eq 1) {
        foreach ($row in $group.Group) {
            $resultRows.Add($row)
        }
        continue
    }

    $collapsedGroups += 1
    $ranked = $group.Group | Sort-Object `
        @{ Expression = { Get-DecimalValue $_.Price }; Descending = $true }, `
        @{ Expression = { Get-DecimalValue $_.'Default Unit Cost' }; Descending = $true }, `
        @{ Expression = { Get-TextLength $_.Description }; Descending = $true }, `
        @{ Expression = { if ([string]::IsNullOrWhiteSpace($_.SKU)) { 0 } else { 1 } }; Descending = $true }, `
        @{ Expression = { $_.SKU }; Descending = $false }

    $keeper = $ranked[0]
    $keeper.Archived = "N"
    $keeper.Sellable = "Y"
    $keeper.'Enabled AZ Cleaning Supplies' = "Y"
    $keeper.'Enabled AZCS' = "Y"
    $resultRows.Add($keeper)

    foreach ($row in $group.Group) {
        $action = "KEEP"
        if ($row -ne $keeper) {
            $action = "DROP"
            $droppedRows += 1
        }

        $reportRows.Add([pscustomobject]@{
            VariantBase = $group.Name
            ItemName = $row.'Item Name'
            Action = $action
            KeptSku = $keeper.SKU
            SKU = $row.SKU
            Vendor = $row.'Default Vendor Name'
            Price = $row.Price
            DefaultUnitCost = $row.'Default Unit Cost'
        })
    }
}

foreach ($row in $resultRows) {
    $priceFloor = Get-DecimalValue $row.Price

    if (-not [string]::IsNullOrWhiteSpace($row.SKU)) {
        $skuKey = $row.SKU.Trim().ToUpperInvariant()
        if ($manualFloorBySku.ContainsKey($skuKey) -and $manualFloorBySku[$skuKey] -gt $priceFloor) {
            $priceFloor = $manualFloorBySku[$skuKey]
        }
        if ($baselineBySku.ContainsKey($skuKey) -and $baselineBySku[$skuKey] -gt $priceFloor) {
            $priceFloor = $baselineBySku[$skuKey]
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($row.'Default Vendor Code')) {
        $vendorCodeKey = $row.'Default Vendor Code'.Trim().ToUpperInvariant()
        if ($baselineByVendorCode.ContainsKey($vendorCodeKey) -and $baselineByVendorCode[$vendorCodeKey] -gt $priceFloor) {
            $priceFloor = $baselineByVendorCode[$vendorCodeKey]
        }
    }

    $nameKey = Normalize-NameFloorKey $row.'Item Name'
    if ($nameKey -and $baselineByName.ContainsKey($nameKey) -and $baselineByName[$nameKey] -gt $priceFloor) {
        $priceFloor = $baselineByName[$nameKey]
    }

    if ($priceFloor -gt (Get-DecimalValue $row.Price)) {
        $baselineFloorAdjustments += 1
    }

    $row.Price = ('{0:0.00}' -f $priceFloor)
    $row.'Price AZ Cleaning Supplies' = $row.Price
    $row.'Price AZCS' = $row.Price
}

$outputCsvLines = $resultRows | ConvertTo-Csv -NoTypeInformation
$finalLines = @($templatePrefix + $outputCsvLines)
Set-Content -Path $OutputPath -Value $finalLines

$reportRows | Sort-Object VariantBase, @{ Expression = { if ($_.Action -eq 'KEEP') { 0 } else { 1 } } }, SKU |
    Export-Csv -Path $ReportPath -NoTypeInformation

$summary = @(
    "Source inventory: $InputPath"
    "Output inventory: $OutputPath"
    "Starting rows: $($rows.Count)"
    "Ending rows: $($resultRows.Count)"
    "Non-chemical variant groups collapsed: $collapsedGroups"
    "Rows dropped in aggressive pass: $droppedRows"
    "Rows raised to baseline price floor: $baselineFloorAdjustments"
    "Rows disabled at AZ Cleaning Supplies: $(($resultRows | Where-Object { $_.'Enabled AZ Cleaning Supplies'.Trim().ToUpperInvariant() -ne 'Y' }).Count)"
    "Rows disabled at AZCS: $(($resultRows | Where-Object { $_.'Enabled AZCS'.Trim().ToUpperInvariant() -ne 'Y' }).Count)"
    "Rows not sellable: $(($resultRows | Where-Object { $_.Sellable.Trim().ToUpperInvariant() -ne 'Y' }).Count)"
    "Reduction report: $ReportPath"
)
Set-Content -Path $SummaryPath -Value $summary
