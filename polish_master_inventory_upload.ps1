param(
    [string]$InputPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_FLAT.csv",
    [string]$OutputPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_POLISHED.csv",
    [string]$SummaryPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_POLISHED.txt",
    [string]$ReportPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_POLISHED_REDUCTION_REPORT.csv"
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

$reportRows = New-Object System.Collections.Generic.List[object]
$resultRows = New-Object System.Collections.Generic.List[object]
$collapsedGroups = 0
$droppedRows = 0

$groups = $rows | Group-Object { if ([string]::IsNullOrWhiteSpace($_.'Item Name')) { "__BLANK_ITEM_NAME__::$($_.SKU)" } else { $_.'Item Name'.Trim().ToUpperInvariant() } }

foreach ($group in $groups) {
    if ($group.Count -eq 1) {
        $resultRows.Add($group.Group[0])
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
    $resultRows.Add($keeper)

    foreach ($row in $group.Group) {
        $action = "KEEP"
        if ($row -ne $keeper) {
            $action = "DROP"
            $droppedRows += 1
        }

        $reportRows.Add([pscustomobject]@{
            ItemName = $row.'Item Name'
            Action = $action
            KeptSku = $keeper.SKU
            SKU = $row.SKU
            Price = $row.Price
            DefaultUnitCost = $row.'Default Unit Cost'
            DescriptionLength = Get-TextLength $row.Description
            Categories = $row.Categories
        })
    }
}

$outputCsvLines = $resultRows | ConvertTo-Csv -NoTypeInformation
$finalLines = @($templatePrefix + $outputCsvLines)
Set-Content -Path $OutputPath -Value $finalLines

$reportRows | Sort-Object ItemName, @{ Expression = { if ($_.Action -eq 'KEEP') { 0 } else { 1 } } }, SKU |
    Export-Csv -Path $ReportPath -NoTypeInformation

$summary = @(
    "Source inventory: $InputPath"
    "Output inventory: $OutputPath"
    "Starting rows: $($rows.Count)"
    "Ending rows: $($resultRows.Count)"
    "Exact item-name groups collapsed: $collapsedGroups"
    "Rows dropped in polish pass: $droppedRows"
    "Reduction report: $ReportPath"
)
Set-Content -Path $SummaryPath -Value $summary
