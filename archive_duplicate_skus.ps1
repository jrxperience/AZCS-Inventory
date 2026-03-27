param(
    [string]$InputPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\UPLOAD_INVENTORY.csv",
    [string]$OutputPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\UPLOAD_INVENTORY_DEDUPED.csv",
    [string]$ReportPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\DUPLICATE_SKU_ARCHIVE_REPORT.csv",
    [string]$SummaryPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\DUPLICATE_SKU_ARCHIVE_REPORT.txt"
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

function Get-YesNoRank {
    param([string]$Value)

    if (($Value | ForEach-Object { $_.Trim().ToUpperInvariant() }) -eq "Y") {
        return 1
    }
    return 0
}

$rawLines = Get-Content $InputPath
if ($rawLines.Length -lt 6) {
    throw "Input file does not look like a Square import template: $InputPath"
}

$templatePrefix = $rawLines[0..3]
$csvPayload = $rawLines[4..($rawLines.Length - 1)] -join "`r`n"
$rows = $csvPayload | ConvertFrom-Csv

$duplicateGroups = $rows |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_.SKU) } |
    Group-Object SKU |
    Where-Object { $_.Count -gt 1 }

$reportRows = New-Object System.Collections.Generic.List[object]
$archivedCount = 0

foreach ($group in $duplicateGroups) {
    $ranked = $group.Group | Sort-Object `
        @{ Expression = { Get-DecimalValue $_.'Default Unit Cost' }; Descending = $true }, `
        @{ Expression = { Get-YesNoRank $_.Archived }; Descending = $false }, `
        @{ Expression = { Get-YesNoRank $_.Sellable }; Descending = $true }, `
        @{ Expression = { $_.Token } ; Descending = $false }

    $winner = $ranked[0]
    $winner.Archived = "N"

    foreach ($row in $group.Group) {
        $action = "KEEP"
        if ($row.Token -ne $winner.Token) {
            $row.Archived = "Y"
            $action = "ARCHIVE"
            $archivedCount += 1
        }

        $reportRows.Add([pscustomobject]@{
            SKU = $group.Name
            DuplicateCount = $group.Count
            Action = $action
            KeptToken = $winner.Token
            Token = $row.Token
            ItemName = $row.'Item Name'
            VariationName = $row.'Variation Name'
            Archived = $row.Archived
            Sellable = $row.Sellable
            DefaultUnitCost = $row.'Default Unit Cost'
            Price = $row.Price
            DefaultVendorName = $row.'Default Vendor Name'
        })
    }
}

$outputCsvLines = $rows | ConvertTo-Csv -NoTypeInformation
$finalLines = @($templatePrefix + $outputCsvLines)
Set-Content -Path $OutputPath -Value $finalLines

$reportRows |
    Sort-Object SKU, @{ Expression = { if ($_.Action -eq "KEEP") { 0 } else { 1 } } }, Token |
    Export-Csv -Path $ReportPath -NoTypeInformation

$summary = @(
    "Source inventory: $InputPath"
    "Output inventory: $OutputPath"
    "Duplicate SKU groups processed: $($duplicateGroups.Count)"
    "Rows archived in duplicate groups: $archivedCount"
    "Duplicate archive report: $ReportPath"
)
Set-Content -Path $SummaryPath -Value $summary
