[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $Policy,
    [string] $Urls = "http://127.0.0.1:5080",
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $ExtraArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Require-Directory {
    param([string] $Path, [string] $Description)

    if (-not (Test-Path -LiteralPath $Path -PathType Container)) {
        throw "Missing ${Description} directory: $Path"
    }
}

function Require-File {
    param([string] $Path, [string] $Description)

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Missing ${Description}: $Path"
    }
}

function Assert-NoReparsePoints {
    param([string] $Path, [string] $Description)

    $rootItem = Get-Item -LiteralPath $Path -Force
    if (($rootItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "${Description} is a reparse point: $Path"
    }
    Get-ChildItem -LiteralPath $Path -Recurse -Force | ForEach-Object {
        if (($_.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "${Description} contains a reparse point: $($_.FullName)"
        }
    }
}

function Assert-ManifestRuntimeDlls {
    param([string] $PackageRoot)

    $manifest = Join-Path $PackageRoot "checksums/SHA256SUMS.txt"
    Require-File $manifest "delivery checksum manifest"
    $missing = [System.Collections.Generic.List[string]]::new()
    foreach ($line in Get-Content -LiteralPath $manifest) {
        if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#")) {
            continue
        }
        $parts = $line -split "  ", 2
        if ($parts.Count -ne 2 -or $parts[0].Length -ne 64 -or [string]::IsNullOrWhiteSpace($parts[1])) {
            throw "Malformed delivery checksum manifest line"
        }
        $relative = $parts[1].Replace('/', '\')
        if ($relative -notmatch '^(management|worker|plugins)\\[^\\]+(?:\\[^\\]+)*\.dll$') {
            continue
        }
        $candidate = [System.IO.Path]::GetFullPath((Join-Path $PackageRoot $relative))
        $root = [System.IO.Path]::GetFullPath($PackageRoot).TrimEnd('\') + '\'
        if (-not $candidate.StartsWith($root, [StringComparison]::OrdinalIgnoreCase) -or
            -not (Test-Path -LiteralPath $candidate -PathType Leaf)) {
            $missing.Add($parts[1])
        }
    }
    if ($missing.Count -gt 0) {
        throw "Delivery manifest declares missing runtime DLL(s): $($missing -join ', ')"
    }
}

$packageRoot = [System.IO.Path]::GetFullPath((Split-Path -Parent $MyInvocation.MyCommand.Path))
Assert-NoReparsePoints $packageRoot "BI Windows delivery"
Assert-ManifestRuntimeDlls $packageRoot
$management = Join-Path $packageRoot "management"
$worker = Join-Path $packageRoot "worker"
$plugins = Join-Path $packageRoot "plugins"
Require-Directory $management "packaged Management"
Require-Directory $worker "packaged Worker"
Require-Directory $plugins "packaged SQL plugins"
Require-File (Join-Path $management "UniversalData.Management.dll") "packaged Management assembly"
Require-File (Join-Path $worker "ud_worker.dll") "packaged Worker assembly"
$policyPath = [System.IO.Path]::GetFullPath((Resolve-Path -LiteralPath $Policy -ErrorAction Stop).Path)
Require-File $policyPath "host policy"

# Capture the installed .NET host before narrowing PATH. Invocation uses its
# absolute path; repository build directories are never added to the child.
$dotnet = Get-Command dotnet -ErrorAction Stop
$systemRoot = $env:SystemRoot
if ([string]::IsNullOrWhiteSpace($systemRoot)) {
    throw "SystemRoot is required to launch the Windows delivery"
}
$system32 = Join-Path $systemRoot "System32"
$oldPath = $env:PATH
$env:PATH = [string]::Join(";", @($worker, $plugins, $management, $system32, $systemRoot))

try {
    $arguments = @(
        (Join-Path $management "UniversalData.Management.dll"),
        "--worker", (Join-Path $worker "ud_worker.dll"),
        "--policy", $policyPath,
        "--urls", $Urls
    )
    if ($ExtraArgs) {
        $arguments += $ExtraArgs
    }

    & $dotnet.Path @arguments
    $exitCode = $LASTEXITCODE
} finally {
    $env:PATH = $oldPath
}

exit $exitCode
