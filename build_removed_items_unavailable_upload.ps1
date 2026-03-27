param(
    [string]$SourceInventoryPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_FINAL.csv",
    [string]$ReductionReportPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_AGGRESSIVE_REDUCTION_REPORT.csv",
    [string]$OutputPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\REMOVED_ITEMS_UNAVAILABLE_2026-03-24.csv",
    [string]$SummaryPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\REMOVED_ITEMS_UNAVAILABLE_2026-03-24.txt"
)

$ErrorActionPreference = "Stop"

$rawLines = Get-Content $SourceInventoryPath
if ($rawLines.Length -lt 6) {
    throw "Input file does not look like a Square import template: $SourceInventoryPath"
}

$templatePrefix = $rawLines[0..3]
$csvPayload = $rawLines[4..($rawLines.Length - 1)] -join "`r`n"
$sourceRows = $csvPayload | ConvertFrom-Csv
$reportRows = Import-Csv $ReductionReportPath | Where-Object { $_.Action -eq 'DROP' }

$sourceBySku = @{}
$sourceByName = @{}
foreach ($row in $sourceRows) {
    if (-not [string]::IsNullOrWhiteSpace($row.SKU) -and -not $sourceBySku.ContainsKey($row.SKU)) {
        $sourceBySku[$row.SKU] = $row
    }

    if (-not [string]::IsNullOrWhiteSpace($row.'Item Name') -and -not $sourceByName.ContainsKey($row.'Item Name')) {
        $sourceByName[$row.'Item Name'] = $row
    }
}

$outputRows = New-Object System.Collections.Generic.List[object]
$missingMatches = New-Object System.Collections.Generic.List[object]

foreach ($reportRow in $reportRows) {
    $matched = $null

    if (-not [string]::IsNullOrWhiteSpace($reportRow.SKU) -and $sourceBySku.ContainsKey($reportRow.SKU)) {
        $matched = $sourceBySku[$reportRow.SKU]
    } elseif (-not [string]::IsNullOrWhiteSpace($reportRow.ItemName) -and $sourceByName.ContainsKey($reportRow.ItemName)) {
        $matched = $sourceByName[$reportRow.ItemName]
    }

    if ($null -eq $matched) {
        $missingMatches.Add($reportRow) | Out-Null
        continue
    }

    $matched.'Square Online Item Visibility' = 'unavailable'
    $matched.Archived = 'N'
    $matched.Sellable = 'Y'
    $matched.'Enabled AZ Cleaning Supplies' = 'Y'
    $matched.'Enabled AZCS' = 'Y'
    $matched.'Price AZ Cleaning Supplies' = $matched.Price
    $matched.'Price AZCS' = $matched.Price
    $outputRows.Add($matched)
}

$outputCsvLines = $outputRows | ConvertTo-Csv -NoTypeInformation
$finalLines = @($templatePrefix + $outputCsvLines)
Set-Content -Path $OutputPath -Value $finalLines

$summary = @(
    "Source inventory: $SourceInventoryPath"
    "Reduction report: $ReductionReportPath"
    "Output inventory: $OutputPath"
    "Removed rows exported: $($outputRows.Count)"
    "Rows not matched back to source: $($missingMatches.Count)"
    "Online visibility setting used: unavailable"
)
Set-Content -Path $SummaryPath -Value $summary
