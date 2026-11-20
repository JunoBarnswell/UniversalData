param(
    [Parameter(Mandatory)]
    [string]$Output,
    [ValidateRange(1, 10000000)]
    [int]$Rows = 50000
)

$ErrorActionPreference = 'Stop'
$root = (New-Item -ItemType Directory -Path $Output -Force).FullName
$sql = Join-Path $env:ProgramFiles 'Microsoft SQL Server\170'
$runtime = Join-Path $sql 'DTS\Binn\DTExec.exe'
if (!(Test-Path $runtime)) { throw 'Microsoft SSIS 2025 DTExec is required.' }
$env:PATH = "$(Split-Path $runtime);$env:PATH"
$requested = @('Microsoft.SqlServer.ManagedDTS.dll', 'Microsoft.SqlServer.DTSPipelineWrap.dll',
    'Microsoft.SqlServer.DTSRuntimeWrap.dll', 'Microsoft.SqlServer.PipelineHost.dll')
$locations = @($sql, "$env:windir\Microsoft.NET\assembly\GAC_MSIL")
$assemblies = foreach ($name in $requested) {
    $match = Get-ChildItem $locations -Recurse -Filter $name -File -ErrorAction SilentlyContinue |
        Where-Object { $_.FullName.StartsWith($sql) -or $_.Directory.Name -match '^v4\.0_17\.' } |
        Select-Object -First 1
    if (!$match) { throw "SSIS 2025 assembly $name is missing." }
    [void][Reflection.Assembly]::LoadFrom($match.FullName)
    $match.FullName
}
Add-Type -Path (Join-Path $PSScriptRoot 'GoldenRows.cs') -ReferencedAssemblies $assemblies
[GoldenRows]::Create($root, $Rows)

$package = Join-Path $root 'baseline.dtsx'
$log = Join-Path $root 'dtexec.log'
& $runtime /FILE $package /REPORTING V 2>&1 | Tee-Object -FilePath $log
if ($LASTEXITCODE -ne 0) { throw "DTExec returned $LASTEXITCODE." }

$windows = Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion'
$ssdt = & "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe" -latest -requires Microsoft.VisualStudio.Component.SQL.SSDT -property installationVersion
if (!$ssdt) { throw 'The installed SSDT version is required for capture.' }
$env:UD_SSIS_CAPTURE_RUNTIME = (Get-Item $runtime).VersionInfo.ProductVersion
$env:UD_SSIS_CAPTURE_WINDOWS = "$($windows.ProductName) $($windows.CurrentBuild).$($windows.UBR)"
$env:UD_SSIS_CAPTURE_SSDT = $ssdt
$env:UD_SSIS_CAPTURE_TZ = (Get-TimeZone).Id
$env:UD_SSIS_CAPTURE_COMMAND = '"' + $runtime + '" /FILE "' + $package + '" /REPORTING V'
& python (Join-Path $PSScriptRoot 'capture_golden_rows.py') $root $Rows
if ($LASTEXITCODE -ne 0) { throw 'Could not serialize Microsoft SSIS observations.' }
