param(
    [string]$ExportPath = "C:\Users\JRAZC\Downloads\MLT3E97CHP443_catalog-2026-03-23-1752.csv",
    [string]$SourceUploadPath = "C:\Codex\AZCS Inventory\square_ready\BASELINE_CURRENT\UPLOAD_THIS_TO_SQUARE.csv"
)

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName Microsoft.VisualBasic

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$currentDir = Join-Path $repoRoot "square_ready\CURRENT"
$baselineDir = Join-Path $repoRoot "square_ready\BASELINE_CURRENT"
$currentCsv = Join-Path $currentDir "UPLOAD_INVENTORY.csv"
$baselineCsv = Join-Path $baselineDir "UPLOAD_THIS_TO_SQUARE.csv"
$currentXlsx = Join-Path $currentDir "UPLOAD_INVENTORY.xlsx"
$baselineXlsx = Join-Path $baselineDir "UPLOAD_THIS_TO_SQUARE.xlsx"
$validationPath = Join-Path $currentDir "UPLOAD_VALIDATION.txt"

function Read-TemplatedCsv {
    param([string]$Path)

    $prelude = Get-Content -LiteralPath $Path -TotalCount 4
    $parser = New-Object Microsoft.VisualBasic.FileIO.TextFieldParser($Path)
    $parser.TextFieldType = [Microsoft.VisualBasic.FileIO.FieldType]::Delimited
    $parser.SetDelimiters(",")
    $parser.HasFieldsEnclosedInQuotes = $true
    $parser.TrimWhiteSpace = $false

    $headers = $null
    $records = New-Object System.Collections.Generic.List[object]
    try {
        while (-not $parser.EndOfData) {
            $fields = $parser.ReadFields()
            if (-not $headers) {
                if ($fields.Count -gt 0 -and $fields[0] -eq "Token") {
                    $headers = $fields
                }
                continue
            }

            if ($fields.Count -eq 0) {
                continue
            }

            $row = [ordered]@{}
            for ($index = 0; $index -lt $headers.Count; $index++) {
                $value = ""
                if ($index -lt $fields.Count) {
                    $value = $fields[$index]
                }
                $row[$headers[$index]] = $value
            }
            $records.Add([PSCustomObject]$row) | Out-Null
        }
    }
    finally {
        $parser.Close()
    }

    if (-not $headers) {
        throw "Could not find CSV header row in $Path"
    }
    return @{
        Prelude = $prelude
        Records = [object[]]$records.ToArray()
    }
}

function Write-TemplatedCsv {
    param(
        [string]$Path,
        [string[]]$Prelude,
        [object[]]$Records
    )

    $tempDir = Join-Path $repoRoot ".tmp"
    if (-not (Test-Path -LiteralPath $tempDir)) {
        New-Item -ItemType Directory -Path $tempDir | Out-Null
    }
    $tempPath = Join-Path $tempDir ("csv-" + [guid]::NewGuid().ToString("N") + ".tmp")
    try {
        $Records | Export-Csv -LiteralPath $tempPath -NoTypeInformation -Encoding utf8
        $body = Get-Content -LiteralPath $tempPath
        $allLines = @()
        if ($Prelude.Count -gt 0) {
            $allLines += $Prelude
        }
        $allLines += $body
        Set-Content -LiteralPath $Path -Value $allLines -Encoding utf8
    }
    finally {
        Remove-Item -LiteralPath $tempPath -ErrorAction SilentlyContinue
    }
}

function First-NonEmpty {
    param([string[]]$Values)
    foreach ($value in $Values) {
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            return $value.Trim()
        }
    }
    return ""
}

function Is-Yes {
    param([string]$Value)
    return $Value -and $Value.Trim().ToUpperInvariant() -eq "Y"
}

function Record-Blob {
    param($Record)
    return @(
        $Record."Item Name",
        $Record.Description,
        $Record.Categories,
        $Record."Reporting Category",
        $Record."Default Vendor Name",
        $Record."Default Vendor Code"
    ) -join " | "
}

function Is-ChemicalLike {
    param($Record)

    $categoryBlob = @(
        $Record.Categories,
        $Record."Reporting Category"
    ) -join " | "
    $blob = (Record-Blob $Record).ToUpperInvariant()
    $categoryBlob = $categoryBlob.ToUpperInvariant()

    $categoryHints = @(
        "CONCRETE CLEANERS",
        "MULTIPURPOSE CLEANER",
        "RESTORATION",
        "SEALERS",
        "STAIN",
        "CHEMICAL",
        "> SOAP"
    )
    $productHints = @(
        "RUST AND OXIDATION REMOVER",
        "RUST ERASER",
        "MULTIPURPOSE CLEANER",
        "EFFLORESCENCE AND CALCIUM REMOVER",
        "RESTORER",
        "BRIGHTENER",
        "BARC",
        "GROUNDSKEEPER",
        "EFFLORESCENCE",
        "C-TAR",
        "CLEANSOL",
        "GLIDE",
        "ACRYLISTRIP",
        "SODIUM HYDROXIDE",
        "HOUSE WASH",
        "SURFACTANT",
        "ASSASSIN",
        "HURRICANE CAT 5 SEALER",
        "SQUEEGEE OFF",
        "WINDOW MAULER",
        "CLEAR SEAL",
        "COLOR SEAL",
        "ACCENT BASE",
        "URETHANE",
        "QUICK STRIP",
        "COBBLE GRIP",
        "COBBLE STRIP"
    )
    $genericHints = @(
        "CLEANER",
        "DEGREASER",
        "RESTORER",
        "BRIGHTENER",
        "SURFACTANT",
        "SEALER",
        "REMOVER",
        "HOUSE WASH",
        "OXIDATION"
    )
    $packagingHints = @(
        "GALLON",
        "PAIL",
        "DRUM",
        "KIT",
        "LB BAG",
        "50 LB",
        "55 GALLON",
        "5 GALLON",
        "1 GALLON",
        "QUART",
        "OUNCE"
    )
    $exclusions = @(
        "NOZZLE",
        "FILTER",
        "HOSE",
        "LANCE",
        "BOTTLE",
        "SURFACE CLEANER",
        "PUMP",
        "SOCKET",
        "STRAINER",
        "INJECTOR",
        "SYSTEM",
        "SKID",
        "TRAILER",
        "WRENCH",
        "GUN",
        "WAND",
        "CLAMP"
    )

    foreach ($keyword in $categoryHints) {
        if ($categoryBlob.Contains($keyword)) {
            return $true
        }
    }
    foreach ($keyword in $exclusions) {
        if ($blob.Contains($keyword)) {
            return $false
        }
    }
    foreach ($keyword in $productHints) {
        if ($blob.Contains($keyword)) {
            return $true
        }
    }

    $hasGeneric = $false
    foreach ($keyword in $genericHints) {
        if ($blob.Contains($keyword)) {
            $hasGeneric = $true
            break
        }
    }
    if (-not $hasGeneric) {
        return $false
    }
    foreach ($keyword in $packagingHints) {
        if ($blob.Contains($keyword)) {
            return $true
        }
    }
    return $false
}

function Coalesce-ExportGroup {
    param([object[]]$Group)

    $canonical = $Group | Select-Object -First 1
    $sharedPrice = First-NonEmpty @(
        $canonical.Price,
        $canonical."Price AZCS",
        $canonical."Price AZ Cleaning Supplies",
        (($Group | ForEach-Object { $_."Price AZCS" }) -join "|"),
        (($Group | ForEach-Object { $_."Price AZ Cleaning Supplies" }) -join "|")
    )
    if ($sharedPrice.Contains("|")) {
        $sharedPrice = First-NonEmpty ($sharedPrice -split "\|")
    }
    $oldEnabled = ($Group | Where-Object { Is-Yes $_."Enabled AZ Cleaning Supplies" } | Measure-Object).Count -gt 0
    $newEnabled = ($Group | Where-Object { Is-Yes $_."Enabled AZCS" } | Measure-Object).Count -gt 0
    return [PSCustomObject]@{
        Token                          = First-NonEmpty @($canonical.Token, (($Group | ForEach-Object { $_.Token }) -join "|"))
        "Enabled AZ Cleaning Supplies" = if ($oldEnabled) { "Y" } else { First-NonEmpty @($canonical."Enabled AZ Cleaning Supplies") }
        "Enabled AZCS"                 = if ($newEnabled) { "Y" } else { First-NonEmpty @($canonical."Enabled AZCS") }
        "Price AZ Cleaning Supplies"   = $sharedPrice
        "Price AZCS"                   = $sharedPrice
        Price                          = $sharedPrice
    }
}

function Write-XlsxFromCsv {
    param(
        [string]$CsvPath,
        [string]$XlsxPath
    )

    try {
        $excel = New-Object -ComObject Excel.Application
    }
    catch {
        return $false
    }

    $excel.Visible = $false
    $excel.DisplayAlerts = $false
    try {
        $workbook = $excel.Workbooks.Open($CsvPath)
        $workbook.SaveAs($XlsxPath, 51)
        $workbook.Close($false)
        return $true
    }
    finally {
        $excel.Quit()
        [System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel) | Out-Null
    }
}

$source = Read-TemplatedCsv -Path $SourceUploadPath
$uploadRecords = @($source.Records)
$exportRecords = Import-Csv -LiteralPath $ExportPath

$exportBySku = @{}
foreach ($group in ($exportRecords | Where-Object { -not [string]::IsNullOrWhiteSpace($_.SKU) } | Group-Object SKU)) {
    $exportBySku[$group.Name.Trim().ToUpperInvariant()] = Coalesce-ExportGroup -Group @($group.Group)
}

$changedChemicals = New-Object System.Collections.Generic.List[string]
foreach ($record in $uploadRecords) {
    $sku = ($record.SKU ?? "").Trim().ToUpperInvariant()
    $exportRow = $null
    if ($sku -and $exportBySku.ContainsKey($sku)) {
        $exportRow = $exportBySku[$sku]
        if ([string]::IsNullOrWhiteSpace($record.Token) -and -not [string]::IsNullOrWhiteSpace($exportRow.Token)) {
            $record.Token = $exportRow.Token
        }
    }

    $sharedPrice = First-NonEmpty @(
        $record.Price,
        $record."Price AZCS",
        $record."Price AZ Cleaning Supplies",
        $(if ($exportRow) { $exportRow.Price } else { "" }),
        $(if ($exportRow) { $exportRow."Price AZCS" } else { "" }),
        $(if ($exportRow) { $exportRow."Price AZ Cleaning Supplies" } else { "" })
    )
    if ($sharedPrice) {
        $record.Price = $sharedPrice
        $record."Price AZCS" = $sharedPrice
        $record."Price AZ Cleaning Supplies" = $sharedPrice
    }

    $forceDual = $false
    if (Is-Yes $record."Enabled AZ Cleaning Supplies") {
        $forceDual = $true
    }
    elseif ($exportRow -and (Is-Yes $exportRow."Enabled AZ Cleaning Supplies")) {
        $forceDual = $true
    }
    elseif ($sharedPrice -and (Is-ChemicalLike $record)) {
        $forceDual = $true
        if ($sku) {
            $changedChemicals.Add($sku)
        }
    }

    if ($forceDual) {
        $record."Enabled AZ Cleaning Supplies" = "Y"
        $record."Enabled AZCS" = "Y"
    }
    elseif ((Is-Yes $record."Enabled AZCS") -or ($exportRow -and (Is-Yes $exportRow."Enabled AZCS"))) {
        $record."Enabled AZCS" = "Y"
    }
}

Write-TemplatedCsv -Path $baselineCsv -Prelude $source.Prelude -Records $uploadRecords
Write-TemplatedCsv -Path $currentCsv -Prelude $source.Prelude -Records $uploadRecords

$xlsxCurrentOk = Write-XlsxFromCsv -CsvPath $currentCsv -XlsxPath $currentXlsx
$xlsxBaselineOk = Write-XlsxFromCsv -CsvPath $baselineCsv -XlsxPath $baselineXlsx

$activeRows = @($uploadRecords | Where-Object { $_.Archived -ne "Y" }).Count
$bothEnabled = @($uploadRecords | Where-Object { $_.Archived -ne "Y" -and (Is-Yes $_."Enabled AZ Cleaning Supplies") -and (Is-Yes $_."Enabled AZCS") }).Count
$oldOnly = @($uploadRecords | Where-Object { $_.Archived -ne "Y" -and (Is-Yes $_."Enabled AZ Cleaning Supplies") -and -not (Is-Yes $_."Enabled AZCS") }).Count
$priceMismatch = @($uploadRecords | Where-Object {
    $_.Archived -ne "Y" -and
    -not [string]::IsNullOrWhiteSpace($_."Price AZ Cleaning Supplies") -and
    -not [string]::IsNullOrWhiteSpace($_."Price AZCS") -and
    $_."Price AZ Cleaning Supplies".Trim() -ne $_."Price AZCS".Trim()
}).Count
$chemicalsDual = @($uploadRecords | Where-Object { $_.Archived -ne "Y" -and (Is-ChemicalLike $_) -and (Is-Yes $_."Enabled AZ Cleaning Supplies") -and (Is-Yes $_."Enabled AZCS") }).Count
$validation = @(
    "Export source: $ExportPath",
    "Source upload: $SourceUploadPath",
    "Active rows: $activeRows",
    "Both enabled: $bothEnabled",
    "Old-only enabled: $oldOnly",
    "Location price mismatches: $priceMismatch",
    "Dual-enabled chemical-like rows: $chemicalsDual",
    "Chemicals forced dual this pass: $($changedChemicals.Count)",
    "Excel output current created: $xlsxCurrentOk",
    "Excel output baseline created: $xlsxBaselineOk"
)
Set-Content -LiteralPath $validationPath -Value $validation -Encoding utf8

$validation | ForEach-Object { Write-Output $_ }
