param(
    [string]$InputPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\UPLOAD_INVENTORY.csv",
    [string]$OutputPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_FLAT.csv",
    [string]$SummaryPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_FLAT.txt",
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

$rawLines = Get-Content $InputPath
if ($rawLines.Length -lt 6) {
    throw "Input file does not look like a Square import template: $InputPath"
}

$templatePrefix = $rawLines[0..3]
$csvPayload = $rawLines[4..($rawLines.Length - 1)] -join "`r`n"
$rows = $csvPayload | ConvertFrom-Csv

# Work from active rows only, then keep one row per SKU.
$activeRows = $rows | Where-Object { ($_.Archived | ForEach-Object { $_.Trim().ToUpperInvariant() }) -ne "Y" }
$groupedBySku = $activeRows | Group-Object SKU

$flatRows = New-Object System.Collections.Generic.List[object]
$trimmedDescriptions = 0
$multiRowSkuGroups = 0

foreach ($group in $groupedBySku) {
    $ranked = $group.Group | Sort-Object `
        @{ Expression = { Get-DecimalValue $_.'Default Unit Cost' }; Descending = $true }, `
        @{ Expression = { Get-DecimalValue $_.Price }; Descending = $true }, `
        @{ Expression = { $_.Token }; Descending = $false }

    if ($group.Count -gt 1 -and -not [string]::IsNullOrWhiteSpace($group.Name)) {
        $multiRowSkuGroups += 1
    }

    $row = $ranked[0]
    $row.Archived = "N"
    $row.Sellable = "Y"
    $row.'Variation Name' = "Regular"
    $row.'Option Name 1' = ""
    $row.'Option Value 1' = ""
    $row.'Customer-facing Name' = $row.'Item Name'

    $normalizedDescription = Normalize-Description -Text $row.Description -MaxLength $DescriptionMax
    if ($normalizedDescription -ne $row.Description) {
        $trimmedDescriptions += 1
        $row.Description = $normalizedDescription
    }

    $flatRows.Add($row)
}

$outputCsvLines = $flatRows | ConvertTo-Csv -NoTypeInformation
$finalLines = @($templatePrefix + $outputCsvLines)
Set-Content -Path $OutputPath -Value $finalLines

$summary = @(
    "Source inventory: $InputPath"
    "Output inventory: $OutputPath"
    "Rows kept in flat upload: $($flatRows.Count)"
    "Duplicate active SKU groups collapsed: $multiRowSkuGroups"
    "Descriptions trimmed to $DescriptionMax chars: $trimmedDescriptions"
    "Rows still archived in final file: $(($flatRows | Where-Object { $_.Archived.Trim().ToUpperInvariant() -eq 'Y' }).Count)"
    "Rows still not sellable in final file: $(($flatRows | Where-Object { $_.Sellable.Trim().ToUpperInvariant() -ne 'Y' }).Count)"
)
Set-Content -Path $SummaryPath -Value $summary
