param(
    [string]$InputPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_POLISHED.csv",
    [string]$OutputPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_FINAL.csv",
    [string]$SummaryPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_FINAL.txt",
    [string]$ReductionReportPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_FINAL_REDUCTION_REPORT.csv",
    [string]$ChemicalSizeAuditPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\CHEMICAL_SIZE_AUDIT_2026-03-24.csv",
    [int]$DescriptionMax = 400
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

function Normalize-Description {
    param([string]$Text, [int]$MaxLength)

    if ([string]::IsNullOrEmpty($Text)) {
        return $Text
    }
    if ($Text.Length -le $MaxLength) {
        return $Text
    }
    if ($MaxLength -le 3) {
        return $Text.Substring(0, $MaxLength)
    }
    return $Text.Substring(0, $MaxLength - 3) + "..."
}

function Normalize-NameKey {
    param([string]$Text)

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return ""
    }

    $value = $Text.ToUpperInvariant()
    $value = [regex]::Replace($value, '\bGALLONS?\b', ' GAL ')
    $value = [regex]::Replace($value, '\bGAL\.\b', ' GAL ')
    $value = [regex]::Replace($value, '[^A-Z0-9]+', ' ')
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

function Is-ChemicalLike {
    param($Row)

    $text = @(
        $Row.'Item Name'
        $Row.Categories
        $Row.'Default Vendor Name'
        $Row.Description
    ) -join ' '
    $upper = $text.ToUpperInvariant()

    if ($upper -match 'SEALER|STAIN|CLEANER|DEGREASER|DETERGENT|SOAP|STRIP|OX|RESTORE|PRESERVE|BLEACH|ACID|MORTAR|MASONRY|BIO BASED|ASSASSIN|BULLSEYE|NMD|EFLO|EF-FORTLESS|NEUTRA') {
        return $true
    }

    if ($Row.'Default Vendor Name' -match 'EacoChem|Trident|EnviroBioCleaner|Front9|RCCT') {
        return $true
    }

    return $false
}

function Get-ChemicalBaseName {
    param([string]$Name)

    if ([string]::IsNullOrWhiteSpace($Name)) {
        return ""
    }

    $value = $Name.ToUpperInvariant()
    $value = [regex]::Replace($value, '\b(1|5|55)\s*(GAL|GALLON|GALLONS)\b', ' ')
    $value = [regex]::Replace($value, '\b(DRUM|PAIL|BUCKET)\b', ' ')
    $value = [regex]::Replace($value, '[^A-Z0-9]+', ' ')
    $value = [regex]::Replace($value, '\s+', ' ').Trim()
    return $value
}

$rawLines = Get-Content $InputPath
if ($rawLines.Length -lt 6) {
    throw "Input file does not look like a Square import template: $InputPath"
}

$templatePrefix = $rawLines[0..3]
$csvPayload = $rawLines[4..($rawLines.Length - 1)] -join "`r`n"
$rows = $csvPayload | ConvertFrom-Csv

$reportRows = New-Object System.Collections.Generic.List[object]
$resultRows = New-Object System.Collections.Generic.List[object]
$trimmedDescriptions = 0
$collapsedGroups = 0
$droppedRows = 0

$groups = $rows | Group-Object { Normalize-NameKey $_.'Item Name' }

foreach ($group in $groups) {
    $activeGroup = $group.Group | Where-Object { $_.Archived.Trim().ToUpperInvariant() -ne 'Y' }
    if ($activeGroup.Count -eq 0) {
        continue
    }

    $ranked = $activeGroup | Sort-Object `
        @{ Expression = { Get-DecimalValue $_.Price }; Descending = $true }, `
        @{ Expression = { Get-DecimalValue $_.'Default Unit Cost' }; Descending = $true }, `
        @{ Expression = { Get-TextLength $_.Description }; Descending = $true }, `
        @{ Expression = { if ([string]::IsNullOrWhiteSpace($_.SKU)) { 0 } else { 1 } }; Descending = $true }, `
        @{ Expression = { $_.SKU }; Descending = $false }

    $keeper = $ranked[0]
    if ($activeGroup.Count -gt 1 -and -not [string]::IsNullOrWhiteSpace($group.Name)) {
        $collapsedGroups += 1
    }

    $keeper.Archived = "N"
    $keeper.Sellable = "Y"
    $keeper.'Enabled AZ Cleaning Supplies' = "Y"
    $keeper.'Enabled AZCS' = "Y"
    $keeper.'Price AZ Cleaning Supplies' = $keeper.Price
    $keeper.'Price AZCS' = $keeper.Price
    $keeper.Description = Normalize-Description -Text $keeper.Description -MaxLength $DescriptionMax
    if (Get-TextLength $keeper.Description -eq $DescriptionMax) {
        $trimmedDescriptions += 1
    }

    $resultRows.Add($keeper)

    foreach ($row in $activeGroup) {
        $action = "KEEP"
        if ($row -ne $keeper) {
            $action = "DROP"
            $droppedRows += 1
        }

        $reportRows.Add([pscustomobject]@{
            NameKey = $group.Name
            ItemName = $row.'Item Name'
            Action = $action
            KeptSku = $keeper.SKU
            SKU = $row.SKU
            Price = $row.Price
            DefaultUnitCost = $row.'Default Unit Cost'
            DescriptionLength = Get-TextLength $row.Description
            Vendor = $row.'Default Vendor Name'
        })
    }
}

$outputCsvLines = $resultRows | ConvertTo-Csv -NoTypeInformation
$finalLines = @($templatePrefix + $outputCsvLines)
Set-Content -Path $OutputPath -Value $finalLines

$reportRows | Sort-Object NameKey, @{ Expression = { if ($_.Action -eq 'KEEP') { 0 } else { 1 } } }, SKU |
    Export-Csv -Path $ReductionReportPath -NoTypeInformation

$chemicalRows = $resultRows | Where-Object { Is-ChemicalLike $_ }
$chemicalFamilies = $chemicalRows | Group-Object { Get-ChemicalBaseName $_.'Item Name' } | Where-Object { $_.Name }
$sizeAudit = foreach ($family in $chemicalFamilies) {
    $names = $family.Group.'Item Name'
    $has1 = ($names | Where-Object { $_ -match '(^|[^0-9])1\s*(GAL|GALLON)' }).Count -gt 0
    $has5 = ($names | Where-Object { $_ -match '(^|[^0-9])5\s*(GAL|GALLON)' }).Count -gt 0
    $has55 = ($names | Where-Object { $_ -match '55\s*(GAL|GALLON)|DRUM' }).Count -gt 0
    [pscustomobject]@{
        Family = $family.Name
        RowCount = $family.Count
        Has1Gal = $has1
        Has5Gal = $has5
        Has55Gal = $has55
    }
}
$sizeAudit | Export-Csv -Path $ChemicalSizeAuditPath -NoTypeInformation

$summary = @(
    "Source inventory: $InputPath"
    "Output inventory: $OutputPath"
    "Starting rows: $($rows.Count)"
    "Ending rows: $($resultRows.Count)"
    "Collapsed duplicate-name groups: $collapsedGroups"
    "Dropped rows in final pass: $droppedRows"
    "Rows not sellable: $(($resultRows | Where-Object { $_.Sellable.Trim().ToUpperInvariant() -ne 'Y' }).Count)"
    "Rows disabled at AZ Cleaning Supplies: $(($resultRows | Where-Object { $_.'Enabled AZ Cleaning Supplies'.Trim().ToUpperInvariant() -ne 'Y' }).Count)"
    "Rows disabled at AZCS: $(($resultRows | Where-Object { $_.'Enabled AZCS'.Trim().ToUpperInvariant() -ne 'Y' }).Count)"
    "Chemical size audit: $ChemicalSizeAuditPath"
    "Reduction report: $ReductionReportPath"
)
Set-Content -Path $SummaryPath -Value $summary
