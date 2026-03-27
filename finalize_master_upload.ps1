param(
    [string]$InputPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\UPLOAD_INVENTORY_READY.csv",
    [string]$OutputPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-23_FINAL.csv",
    [string]$SummaryPath = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-23_FINAL.txt",
    [int]$DescriptionMax = 400
)

$ErrorActionPreference = "Stop"

function Normalize-Description {
    param(
        [string]$Text,
        [int]$MaxLength
    )

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

$keptRows = New-Object System.Collections.Generic.List[object]
$descriptionTrimmed = 0
$activeRows = 0

foreach ($row in $rows) {
    $archived = ""
    if ($null -ne $row.Archived) {
        $archived = $row.Archived.Trim().ToUpperInvariant()
    }

    if ($archived -eq "Y") {
        continue
    }

    $activeRows += 1
    $row.Archived = "N"
    $row.Sellable = "Y"

    $normalizedDescription = Normalize-Description -Text $row.Description -MaxLength $DescriptionMax
    if ($normalizedDescription -ne $row.Description) {
        $descriptionTrimmed += 1
        $row.Description = $normalizedDescription
    }

    $keptRows.Add($row)
}

$outputCsvLines = $keptRows | ConvertTo-Csv -NoTypeInformation
$finalLines = @($templatePrefix + $outputCsvLines)
Set-Content -Path $OutputPath -Value $finalLines

$activeDuplicateSkuGroups = ($keptRows |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_.SKU) } |
    Group-Object SKU |
    Where-Object { $_.Count -gt 1 }).Count

$activeNotSellable = ($keptRows |
    Where-Object { $_.Sellable.Trim().ToUpperInvariant() -ne "Y" }).Count

$longDescriptionsRemaining = ($keptRows |
    Where-Object { -not [string]::IsNullOrEmpty($_.Description) -and $_.Description.Length -gt $DescriptionMax }).Count

$summary = @(
    "Source inventory: $InputPath"
    "Output inventory: $OutputPath"
    "Rows kept in final upload: $activeRows"
    "Descriptions trimmed to $DescriptionMax chars: $descriptionTrimmed"
    "Active duplicate SKU groups remaining: $activeDuplicateSkuGroups"
    "Active rows still not sellable: $activeNotSellable"
    "Descriptions still over $DescriptionMax chars: $longDescriptionsRemaining"
)
Set-Content -Path $SummaryPath -Value $summary
