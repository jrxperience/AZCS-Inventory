param(
    [string]$SourceCsv = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_AGGRESSIVE.csv",
    [string]$SourceSummary = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_AGGRESSIVE.txt",
    [string]$SourceReductionReport = "C:\Codex\AZCS Inventory\square_ready\CURRENT\MASTER_INVENTORY_UPLOAD_2026-03-24_AGGRESSIVE_REDUCTION_REPORT.csv",
    [string]$UploadReadyRoot = "C:\Codex\AZCS Inventory\UPLOAD READY"
)

$ErrorActionPreference = "Stop"

$dateFolderName = Get-Date -Format "M.d.yy"
$dateFolderPath = Join-Path $UploadReadyRoot $dateFolderName
if (-not (Test-Path $dateFolderPath)) {
    New-Item -ItemType Directory -Path $dateFolderPath | Out-Null
}

$versionFolders = Get-ChildItem $dateFolderPath -Directory -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match '^V\s+\d+$' } |
    Sort-Object {
        [int]($_.Name -replace '[^\d]', '')
    }

$targetVersionFolder = $null
foreach ($folder in $versionFolders) {
    $hasFiles = (Get-ChildItem $folder.FullName -File -ErrorAction SilentlyContinue | Measure-Object).Count -gt 0
    if (-not $hasFiles) {
        $targetVersionFolder = $folder.FullName
        break
    }
}

if (-not $targetVersionFolder) {
    $nextNumber = 1
    if ($versionFolders.Count -gt 0) {
        $nextNumber = (($versionFolders | ForEach-Object { [int]($_.Name -replace '[^\d]', '') } | Measure-Object -Maximum).Maximum) + 1
    }
    $targetVersionFolder = Join-Path $dateFolderPath ("V " + $nextNumber)
    New-Item -ItemType Directory -Path $targetVersionFolder | Out-Null
}

Copy-Item $SourceCsv (Join-Path $targetVersionFolder 'UPLOAD_INVENTORY.csv') -Force

if (Test-Path $SourceSummary) {
    Copy-Item $SourceSummary (Join-Path $targetVersionFolder 'UPLOAD_SUMMARY.txt') -Force
}

if (Test-Path $SourceReductionReport) {
    Copy-Item $SourceReductionReport (Join-Path $targetVersionFolder 'REDUCTION_REPORT.csv') -Force
}

[pscustomobject]@{
    DateFolder = $dateFolderPath
    VersionFolder = $targetVersionFolder
    UploadCsv = (Join-Path $targetVersionFolder 'UPLOAD_INVENTORY.csv')
} | ConvertTo-Json -Compress
