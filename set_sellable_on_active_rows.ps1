param(
    [string]$InputPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\UPLOAD_INVENTORY_DEDUPED.csv",
    [string]$OutputPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\UPLOAD_INVENTORY_READY.csv",
    [string]$SummaryPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\UPLOAD_INVENTORY_READY.txt"
)

$ErrorActionPreference = "Stop"

$rawLines = Get-Content $InputPath
if ($rawLines.Length -lt 6) {
    throw "Input file does not look like a Square import template: $InputPath"
}

$templatePrefix = $rawLines[0..3]
$csvPayload = $rawLines[4..($rawLines.Length - 1)] -join "`r`n"
$rows = $csvPayload | ConvertFrom-Csv

$updatedCount = 0
foreach ($row in $rows) {
    if (($row.Archived | ForEach-Object { $_.Trim().ToUpperInvariant() }) -ne "Y") {
        if ($row.Sellable -ne "Y") {
            $row.Sellable = "Y"
            $updatedCount += 1
        }
    }
}

$outputCsvLines = $rows | ConvertTo-Csv -NoTypeInformation
$finalLines = @($templatePrefix + $outputCsvLines)
Set-Content -Path $OutputPath -Value $finalLines

$activeRows = ($rows | Where-Object { ($_.Archived | ForEach-Object { $_.Trim().ToUpperInvariant() }) -ne "Y" }).Count
$activeNotSellable = ($rows | Where-Object {
    ($_.Archived | ForEach-Object { $_.Trim().ToUpperInvariant() }) -ne "Y" -and
    ($_.Sellable | ForEach-Object { $_.Trim().ToUpperInvariant() }) -ne "Y"
}).Count

$summary = @(
    "Source inventory: $InputPath"
    "Output inventory: $OutputPath"
    "Active rows set to sellable: $updatedCount"
    "Active rows total: $activeRows"
    "Active rows still not sellable: $activeNotSellable"
)
Set-Content -Path $SummaryPath -Value $summary
