param(
    [string]$SourceCsv = "C:\Codex\AZCS Inventory\square_ready\CURRENT\UPLOAD_INVENTORY_CLEANED.csv",
    [string]$CorrectionCsv = "C:\Codex\AZCS Inventory\square_ready\CURRENT\IMPORT_ERROR_CORRECTION_LIST.csv",
    [string]$OutputCsv = "C:\Codex\AZCS Inventory\square_ready\CURRENT\UPLOAD_INVENTORY_ERRORS_ONLY.csv"
)

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName Microsoft.VisualBasic
Add-Type -AssemblyName System.IO.Compression.FileSystem

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
            $row = [ordered]@{}
            for ($index = 0; $index -lt $headers.Count; $index++) {
                $row[$headers[$index]] = if ($index -lt $fields.Count) { $fields[$index] } else { "" }
            }
            $records.Add([pscustomobject]$row) | Out-Null
        }
    }
    finally {
        $parser.Close()
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

    $tmpDir = Join-Path "C:\Codex\AZCS Inventory" ".tmp"
    if (-not (Test-Path -LiteralPath $tmpDir)) {
        New-Item -ItemType Directory -Path $tmpDir | Out-Null
    }
    $tempPath = Join-Path $tmpDir ("csv-" + [guid]::NewGuid().ToString("N") + ".tmp")
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

function Convert-TemplatedCsvToXlsx {
    param(
        [string]$CsvPath,
        [string]$XlsxPath
    )

    if (Test-Path -LiteralPath $XlsxPath) {
        Remove-Item -LiteralPath $XlsxPath -Force
    }

    $dir = Join-Path "C:\Codex\AZCS Inventory\.tmp" ("xlsx-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $dir | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $dir "_rels") | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $dir "docProps") | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $dir "xl") | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $dir "xl\_rels") | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $dir "xl\worksheets") | Out-Null

    function Escape-Xml([string]$s) {
        if ($null -eq $s) { return "" }
        return [System.Security.SecurityElement]::Escape($s)
    }

    function ColName([int]$n) {
        $name = ""
        while ($n -gt 0) {
            $r = ($n - 1) % 26
            $name = [char](65 + $r) + $name
            $n = [math]::Floor(($n - 1) / 26)
        }
        return $name
    }

    $parser = New-Object Microsoft.VisualBasic.FileIO.TextFieldParser($CsvPath)
    $parser.TextFieldType = [Microsoft.VisualBasic.FileIO.FieldType]::Delimited
    $parser.SetDelimiters(",")
    $parser.HasFieldsEnclosedInQuotes = $true
    $parser.TrimWhiteSpace = $false
    $allRows = New-Object System.Collections.Generic.List[object]
    $maxCols = 1
    try {
        while (-not $parser.EndOfData) {
            $fields = $parser.ReadFields()
            if ($fields.Count -gt $maxCols) {
                $maxCols = $fields.Count
            }
            $allRows.Add($fields) | Out-Null
        }
    }
    finally {
        $parser.Close()
    }

    $sheetRows = New-Object System.Collections.Generic.List[string]
    $rowIndex = 0
    foreach ($fields in $allRows) {
        $rowIndex++
        $cells = New-Object System.Collections.Generic.List[string]
        for ($i = 0; $i -lt $fields.Count; $i++) {
            $addr = (ColName ($i + 1)) + $rowIndex
            $raw = [string]$fields[$i]
            if ($raw -match '^-?\d+(\.\d+)?$' -and $raw -notmatch '^0\d+') {
                $cells.Add('<c r="' + $addr + '"><v>' + $raw + '</v></c>') | Out-Null
            }
            else {
                $v = Escape-Xml $raw
                $cells.Add('<c r="' + $addr + '" t="inlineStr"><is><t xml:space="preserve">' + $v + '</t></is></c>') | Out-Null
            }
        }
        $sheetRows.Add('<row r="' + $rowIndex + '">' + ($cells -join '') + '</row>') | Out-Null
    }

    $contentTypes = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>
'@
    $rels = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>
'@
    $workbook = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Error Items Only" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>
'@
    $wbRels = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>
'@
    $styles = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>
  <fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>
  <borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
  <cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
  <cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>
  <cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
</styleSheet>
'@
    $now = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $core = @"
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>Codex</dc:creator>
  <cp:lastModifiedBy>Codex</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">$now</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">$now</dcterms:modified>
</cp:coreProperties>
"@
    $app = @'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>Codex</Application>
</Properties>
'@
    $dimension = 'A1:' + (ColName $maxCols) + $rowIndex
    $sheet = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><dimension ref="' + $dimension + '"/><sheetViews><sheetView workbookViewId="0"/></sheetViews><sheetFormatPr defaultRowHeight="15"/><sheetData>' + ($sheetRows -join '') + '</sheetData></worksheet>'

    Set-Content -LiteralPath (Join-Path $dir '[Content_Types].xml') -Value $contentTypes -Encoding utf8
    Set-Content -LiteralPath (Join-Path $dir '_rels\.rels') -Value $rels -Encoding utf8
    Set-Content -LiteralPath (Join-Path $dir 'docProps\core.xml') -Value $core -Encoding utf8
    Set-Content -LiteralPath (Join-Path $dir 'docProps\app.xml') -Value $app -Encoding utf8
    Set-Content -LiteralPath (Join-Path $dir 'xl\workbook.xml') -Value $workbook -Encoding utf8
    Set-Content -LiteralPath (Join-Path $dir 'xl\_rels\workbook.xml.rels') -Value $wbRels -Encoding utf8
    Set-Content -LiteralPath (Join-Path $dir 'xl\styles.xml') -Value $styles -Encoding utf8
    Set-Content -LiteralPath (Join-Path $dir 'xl\worksheets\sheet1.xml') -Value $sheet -Encoding utf8
    [System.IO.Compression.ZipFile]::CreateFromDirectory($dir, $XlsxPath)
    Remove-Item -LiteralPath $dir -Recurse -Force
}

$source = Read-TemplatedCsv -Path $SourceCsv
$records = @($source.Records)
$corrections = Import-Csv -LiteralPath $CorrectionCsv

$targetSkus = $corrections | ForEach-Object { $_.SKU } | Where-Object { $_ } | Sort-Object -Unique
$skuSet = New-Object System.Collections.Generic.HashSet[string]([System.StringComparer]::OrdinalIgnoreCase)
foreach ($sku in $targetSkus) {
    [void]$skuSet.Add($sku)
}

$filtered = New-Object System.Collections.Generic.List[object]
foreach ($record in $records) {
    if ($skuSet.Contains([string]$record.SKU)) {
        $filtered.Add($record) | Out-Null
    }
}

Write-TemplatedCsv -Path $OutputCsv -Prelude $source.Prelude -Records ([object[]]$filtered.ToArray())
$outputXlsx = [System.IO.Path]::ChangeExtension($OutputCsv, ".xlsx")
Convert-TemplatedCsvToXlsx -CsvPath $OutputCsv -XlsxPath $outputXlsx

$summaryPath = [System.IO.Path]::ChangeExtension($OutputCsv, ".txt")
$summary = @(
    "Source CSV: $SourceCsv",
    "Correction list: $CorrectionCsv",
    "Unique SKUs from error report: $($targetSkus.Count)",
    "Rows in error-only file: $($filtered.Count)",
    "Output CSV: $OutputCsv",
    "Output XLSX: $outputXlsx"
)
Set-Content -LiteralPath $summaryPath -Value $summary -Encoding utf8

Get-Item -LiteralPath $OutputCsv, $outputXlsx, $summaryPath | Select-Object FullName, Length, LastWriteTime
