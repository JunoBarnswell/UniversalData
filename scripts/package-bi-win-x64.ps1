[CmdletBinding()]
param(
    [string] $OutputRoot = "",
    [string] $NativeRoot = "",
    [string] $ManagementOutput = "",
    [string] $WorkerOutput = "",
    [string] $GuiRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))

function Resolve-FullPath {
    param([string] $Value, [string] $DefaultRelative)

    $candidate = if ([string]::IsNullOrWhiteSpace($Value)) {
        Join-Path $repoRoot $DefaultRelative
    } else {
        $Value
    }
    return [System.IO.Path]::GetFullPath($candidate)
}

function Resolve-ExistingDirectory {
    param([string] $Path, [string] $Description)

    if (-not (Test-Path -LiteralPath $Path -PathType Container)) {
        throw "Missing ${Description} directory: $Path"
    }
    return [System.IO.Path]::GetFullPath((Resolve-Path -LiteralPath $Path).Path)
}

function Require-File {
    param([string] $Path, [string] $Description)

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Missing ${Description}: $Path"
    }
}

function Assert-NoReparsePoints {
    param([string] $Path, [string] $Description)

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }

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

function Assert-NoReparseAncestors {
    param([string] $Path, [string] $Description)

    $current = [System.IO.Path]::GetFullPath($Path)
    while (-not [string]::IsNullOrWhiteSpace($current)) {
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force
            if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "${Description} contains a reparse-point ancestor: $current"
            }
        }
        $parent = Split-Path -Parent $current
        if ([string]::IsNullOrWhiteSpace($parent) -or $parent.Equals($current, [StringComparison]::OrdinalIgnoreCase)) {
            break
        }
        $current = $parent
    }
}

function New-Directory {
    param([string] $Path)

    New-Item -ItemType Directory -Path $Path -Force | Out-Null
}

function Copy-Tree {
    param([string] $Source, [string] $Destination, [string] $Description)

    Assert-NoReparsePoints $Source $Description
    New-Directory $Destination
    $sourceRoot = [System.IO.Path]::GetFullPath($Source)
    Get-ChildItem -LiteralPath $sourceRoot -Recurse -Force | ForEach-Object {
        $relative = [System.IO.Path]::GetRelativePath($sourceRoot, $_.FullName)
        $target = Join-Path $Destination $relative
        if ($_.PSIsContainer) {
            New-Directory $target
        } else {
            New-Directory (Split-Path -Parent $target)
            Copy-Item -LiteralPath $_.FullName -Destination $target -Force
        }
    }
}

function Copy-RuntimeTree {
    param([string] $Source, [string] $Destination, [string] $Description)

    Assert-NoReparsePoints $Source $Description
    New-Directory $Destination
    $sourceRoot = [System.IO.Path]::GetFullPath($Source)
    $buildOnlyExtensions = @(".lib", ".exp", ".pdb", ".ilk", ".obj", ".tlog", ".lastbuildstate")
    Get-ChildItem -LiteralPath $sourceRoot -Recurse -File -Force | ForEach-Object {
        if ($buildOnlyExtensions -contains $_.Extension.ToLowerInvariant()) {
            return
        }

        $relative = [System.IO.Path]::GetRelativePath($sourceRoot, $_.FullName)
        $target = Join-Path $Destination $relative
        New-Directory (Split-Path -Parent $target)
        Copy-Item -LiteralPath $_.FullName -Destination $target -Force
    }
}

function Copy-NativeRuntime {
    param(
        [string] $Source,
        [string[]] $Destinations,
        [string] $CoreHash,
        [string] $Description
    )

    Assert-NoReparsePoints $Source $Description
    $sourceRoot = [System.IO.Path]::GetFullPath($Source)
    $buildOnlyExtensions = @(".lib", ".exp", ".pdb", ".ilk", ".obj", ".tlog", ".lastbuildstate")
    $runtimeExtensions = @(".dll", ".json", ".dat", ".xml", ".ini", ".cfg", ".config", ".cnf", ".lic", ".txt")
    Get-ChildItem -LiteralPath $sourceRoot -Recurse -File -Force | ForEach-Object {
        if ($buildOnlyExtensions -contains $_.Extension.ToLowerInvariant()) {
            return
        }

        $relative = [System.IO.Path]::GetRelativePath($sourceRoot, $_.FullName)
        if ($runtimeExtensions -notcontains $_.Extension.ToLowerInvariant()) {
            return
        }

        foreach ($destination in $Destinations) {
            $target = Join-Path $destination $relative
            New-Directory (Split-Path -Parent $target)
            Copy-Item -LiteralPath $_.FullName -Destination $target -Force
        }
    }
}

function Copy-Required {
    param([string] $Source, [string] $Destination, [string] $Description, [string] $Name)

    Require-File $Source $Description
    New-Directory $Destination
    Copy-Item -LiteralPath $Source -Destination (Join-Path $Destination $Name) -Force
}

function Find-NoticeFile {
    param(
        [string] $Root,
        [string] $NamePattern,
        [string] $ContentPattern,
        [string] $Description
    )

    if (-not (Test-Path -LiteralPath $Root -PathType Container)) {
        throw "Missing ${Description} root: $Root"
    }

    $candidates = Get-ChildItem -LiteralPath $Root -Recurse -File -Force |
        Where-Object { $_.Name -match $NamePattern } |
        Sort-Object FullName
    foreach ($candidate in $candidates) {
        $prefix = (Get-Content -LiteralPath $candidate.FullName -TotalCount 20 | Out-String)
        if ($prefix -match $ContentPattern) {
            return [System.IO.Path]::GetFullPath($candidate.FullName)
        }
    }
    throw "Unable to find ${Description} under $Root"
}

function Write-TextFile {
    param([string] $Path, [string] $Content)

    $utf8 = [System.Text.UTF8Encoding]::new($false)
    [System.IO.File]::WriteAllText($Path, $Content, $utf8)
}

function Get-SdkVersion {
    param([string] $VersionHeader)

    Require-File $VersionHeader "SDK version header"
    $match = Select-String -LiteralPath $VersionHeader -Pattern '^\s*#define\s+UD_SDK_VERSION\s+"([^"]+)"' | Select-Object -First 1
    if ($null -eq $match) {
        throw "Unable to read UD_SDK_VERSION from $VersionHeader"
    }
    return $match.Matches[0].Groups[1].Value
}

function Get-RelativeFileList {
    param([string] $Root)

    $rootPath = [System.IO.Path]::GetFullPath($Root)
    return @(Get-ChildItem -LiteralPath $rootPath -Recurse -File -Force | Sort-Object FullName | ForEach-Object {
        [System.IO.Path]::GetRelativePath($rootPath, $_.FullName).Replace('\', '/')
    })
}

function Invoke-LoaderValidation {
    param(
        [string] $PackageRoot,
        [string] $SdkVersion,
        [string[]] $PluginNames,
        [string] $RepositoryRoot
    )

    $python = Get-Command python -ErrorAction Stop
    $test = Join-Path $RepositoryRoot "tests/windows_sql_runtime.py"
    Require-File $test "Windows runtime loader test"
    $managementCore = Join-Path $PackageRoot "management/ud_data.dll"
    $workerCore = Join-Path $PackageRoot "worker/ud_data.dll"
    Require-File $managementCore "packaged Management native core"
    Require-File $workerCore "packaged Worker native core"
    $plugins = foreach ($name in $PluginNames) {
        $path = Join-Path $PackageRoot (Join-Path "plugins" $name)
        Require-File $path "packaged SQL connector $name"
        $path
    }

    $systemRoot = $env:SystemRoot
    if ([string]::IsNullOrWhiteSpace($systemRoot)) {
        throw "SystemRoot is required for the system-only PATH loader test"
    }
    $oldPath = $env:PATH
    try {
        $env:PATH = [string]::Join(";", @((Join-Path $systemRoot "System32"), $systemRoot))
        $loaderOutput = @(& $python.Path $test --library $managementCore $workerCore --version $SdkVersion --plugins $plugins 2>&1)
        $exitCode = $LASTEXITCODE
        if ($exitCode -ne 0) {
            throw "Windows runtime loader test failed with exit code ${exitCode}: $($loaderOutput -join [Environment]::NewLine)"
        }
        $loaderOutput | ForEach-Object { Write-Host $_ }
    } finally {
        $env:PATH = $oldPath
    }

    return "tests/windows_sql_runtime.py passed for Management and Worker with six SQL plugins and System32-only PATH."
}

function Write-ShaManifest {
    param([string] $PackageRoot)

    $checksumRoot = Join-Path $PackageRoot "checksums"
    New-Directory $checksumRoot
    $manifest = Join-Path $checksumRoot "SHA256SUMS.txt"
    $lines = @(
        "# SHA-256 for every delivered file except this manifest itself",
        "# Paths are relative to the delivery root and use '/' separators."
    )
    $packagePath = [System.IO.Path]::GetFullPath($PackageRoot)
    Get-ChildItem -LiteralPath $packagePath -Recurse -File -Force |
        Where-Object { [System.IO.Path]::GetFullPath($_.FullName) -ne [System.IO.Path]::GetFullPath($manifest) } |
        Sort-Object FullName | ForEach-Object {
            $relative = [System.IO.Path]::GetRelativePath($packagePath, $_.FullName).Replace('\', '/')
            $hash = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
            $lines += "$hash  $relative"
        }
    Write-TextFile $manifest (($lines -join "`n") + "`n")
}

function Assert-OutputLocation {
    param([string] $Path, [string] $ArtifactsRoot)

    $full = [System.IO.Path]::GetFullPath($Path).TrimEnd([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar)
    $root = [System.IO.Path]::GetFullPath($ArtifactsRoot).TrimEnd([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar)
    $prefix = $root + [System.IO.Path]::DirectorySeparatorChar
    if ($full.Equals($root, [StringComparison]::OrdinalIgnoreCase) -or
        -not $full.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw "OutputRoot must be a child of the repository artifacts directory: $Path"
    }
}

function Assert-ExistingDeliveryTarget {
    param([string] $Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }
    if (-not (Test-Path -LiteralPath $Path -PathType Container)) {
        throw "Existing delivery target is not a directory: $Path"
    }
    Assert-NoReparsePoints $Path "existing delivery target"
    $required = @(
        @{ Path = "BUILD-INFO.md"; Description = "delivery identity" },
        @{ Path = "checksums/SHA256SUMS.txt"; Description = "delivery checksum manifest" },
        @{ Path = "management/UniversalData.Management.dll"; Description = "Management assembly" },
        @{ Path = "worker/ud_worker.dll"; Description = "Worker assembly" }
    )
    foreach ($entry in $required) {
        Require-File (Join-Path $Path $entry.Path) "existing $($entry.Description)"
    }
    $title = (Get-Content -LiteralPath (Join-Path $Path "BUILD-INFO.md") -TotalCount 1) -join ""
    if ($title -ne "# UniversalData BI Windows x64 delivery") {
        throw "Existing delivery target is not a UniversalData BI Windows x64 package: $Path"
    }
}

$output = Resolve-FullPath $OutputRoot "artifacts/bi-win-x64"
$native = Resolve-FullPath $NativeRoot "build/vs2026"
$management = Resolve-FullPath $ManagementOutput "build/vs2026/hosts/product/management"
$worker = Resolve-FullPath $WorkerOutput "build/vs2026/hosts/product/worker"
$gui = Resolve-FullPath $GuiRoot "products/UniversalData.Management/wwwroot"
$artifactsRoot = Resolve-ExistingDirectory (Join-Path $repoRoot "artifacts") "delivery artifacts root"
Assert-OutputLocation $output $artifactsRoot
$artifactParent = [System.IO.Path]::GetFullPath((Split-Path -Parent $output))
Assert-NoReparseAncestors $artifactParent "delivery artifacts path"

$native = Resolve-ExistingDirectory $native "Native staging"
$management = Resolve-ExistingDirectory $management "Management publish"
$worker = Resolve-ExistingDirectory $worker "Worker publish"
$gui = Resolve-ExistingDirectory $gui "Management GUI"
$nativeCoreCandidates = @(
    (Join-Path $native "Release/ud_data.dll"),
    (Join-Path $native "windows-runtime/Release/ud_core_windows_runtime-pe/ud_data.dll"),
    (Join-Path $native "windows-runtime/Release/ud_core_windows_runtime/ud_data.dll")
)
$nativeCorePath = $nativeCoreCandidates | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1
if ([string]::IsNullOrWhiteSpace($nativeCorePath)) {
    throw "Native staging has no CMake output ud_data.dll. Checked: $($nativeCoreCandidates -join ', ')"
}
$nativeCoreSource = [System.IO.Path]::GetFullPath((Split-Path -Parent $nativeCorePath))
$nativeRuntimeSources = @(
    (Join-Path $native "Release"),
    (Join-Path $native "windows-runtime/Release/ud_core_windows_runtime-pe"),
    (Join-Path $native "windows-runtime/Release/ud_core_windows_runtime")
) | Where-Object { Test-Path -LiteralPath $_ -PathType Container } | ForEach-Object {
    [System.IO.Path]::GetFullPath($_)
} | Select-Object -Unique
$pluginCandidates = @(
    (Join-Path $native "plugins/Release"),
    (Join-Path $native "Release/plugins/Release"),
    (Join-Path $native "windows-runtime/Release/ud_sql_windows_runtime"),
    (Join-Path $native "lib/UniversalData/plugins"),
    (Join-Path $native "hosts/product/plugins")
)
$pluginRelease = $pluginCandidates | Where-Object {
    $candidate = $_
    (Test-Path -LiteralPath $candidate -PathType Container) -and
    (@("ud_connector_sqlite.dll", "ud_connector_postgresql.dll", "ud_connector_odbc.dll", "ud_connector_mysql.dll", "ud_connector_mariadb.dll", "ud_connector_oracle_oci.dll" | ForEach-Object {
        Test-Path -LiteralPath (Join-Path $candidate $_) -PathType Leaf
    }) -notcontains $false)
} | Select-Object -First 1
if ([string]::IsNullOrWhiteSpace($pluginRelease)) {
    throw "Native staging has no complete six-connector plugin closure. Checked: $($pluginCandidates -join ', '). Configure the formal PRODUCT build or pass -NativeRoot to the approved six-driver CMake staging tree."
}
$pluginRelease = [System.IO.Path]::GetFullPath($pluginRelease)
Assert-NoReparsePoints $management "Management publish"
Assert-NoReparsePoints $worker "Worker publish"
Assert-NoReparsePoints $gui "Management GUI"
foreach ($path in $nativeRuntimeSources) {
    Assert-NoReparsePoints $path "Native runtime staging"
}
Assert-NoReparsePoints $pluginRelease "Native plugin staging"

$pluginNames = @(
    "ud_connector_sqlite.dll",
    "ud_connector_postgresql.dll",
    "ud_connector_odbc.dll",
    "ud_connector_mysql.dll",
    "ud_connector_mariadb.dll",
    "ud_connector_oracle_oci.dll"
)
Require-File $nativeCorePath "Native CMake-staged core"
foreach ($pluginName in $pluginNames) {
    Require-File (Join-Path $pluginRelease $pluginName) "CMake-staged SQL connector $pluginName"
}
Require-File (Join-Path $management "UniversalData.Management.dll") "Management assembly"
Require-File (Join-Path $worker "ud_worker.dll") "Worker assembly"
Require-File (Join-Path $management "ud_data.dll") "Management native core"
Require-File (Join-Path $worker "ud_data.dll") "Worker native core"

$managementCoreHash = (Get-FileHash -LiteralPath (Join-Path $management "ud_data.dll") -Algorithm SHA256).Hash
$workerCoreHash = (Get-FileHash -LiteralPath (Join-Path $worker "ud_data.dll") -Algorithm SHA256).Hash
if ($managementCoreHash -ne $workerCoreHash) {
    throw "Management and Worker native core hashes differ: $managementCoreHash vs $workerCoreHash"
}
$nativeCoreHash = (Get-FileHash -LiteralPath $nativeCorePath -Algorithm SHA256).Hash
$nativeCoreMatchesPublish = $nativeCoreHash -eq $managementCoreHash
if (-not $nativeCoreMatchesPublish) {
    Write-Warning "Native staging core $nativeCoreHash differs from the matched Management/Worker source core $managementCoreHash; the Native staging core will be authoritative in the delivery."
}

$sdkVersion = Get-SdkVersion (Join-Path $repoRoot "include/ud/version.h")
$stage = Join-Path $artifactParent (".{0}.stage-{1}" -f (Split-Path -Leaf $output), [Guid]::NewGuid().ToString("N"))
$backup = Join-Path $artifactParent (".{0}.previous-{1}" -f (Split-Path -Leaf $output), [Guid]::NewGuid().ToString("N"))
$backupMoved = $false
$stagePromoted = $false

try {
    New-Directory $stage
    $managementTarget = Join-Path $stage "management"
    $workerTarget = Join-Path $stage "worker"
    $pluginTarget = Join-Path $stage "plugins"
    $vendorTarget = Join-Path $stage "vendor"
    $checksumTarget = Join-Path $stage "checksums"
    foreach ($directory in @($managementTarget, $workerTarget, $pluginTarget, $vendorTarget, $checksumTarget)) {
        New-Directory $directory
    }

    # Preserve complete .NET publish trees, including runtimes/ and localized
    # resource subtrees. Native staging then contributes only runtime closure.
    Copy-Tree $management $managementTarget "Management publish"
    Copy-Tree $worker $workerTarget "Worker publish"
    $guiTarget = Join-Path $managementTarget "wwwroot"
    if (Test-Path -LiteralPath $guiTarget -PathType Container) {
        Assert-NoReparsePoints $guiTarget "publish GUI subtree"
        [System.IO.Directory]::Delete($guiTarget, $true)
    }
    Copy-Tree $gui $guiTarget "Management GUI"
    foreach ($nativeRuntimeSource in $nativeRuntimeSources) {
        Copy-NativeRuntime $nativeRuntimeSource @($managementTarget, $workerTarget) $managementCoreHash "Native CMake runtime closure"
    }
    Copy-RuntimeTree $pluginRelease $pluginTarget "CMake-staged SQL plugin runtime closure"

    Copy-Required (Join-Path $PSScriptRoot "run-bi-management.ps1") $stage "package launcher" "run-management.ps1"

    # Resolve notices from the actual dependency trees first. The ICU package
    # carries its official Unicode license under the installed major/minor
    # directory, and the Arrow notices are shipped in the matching pyarrow
    # dist-info tree. Repository copies are only a fallback for product builds
    # that do not retain the wheel metadata.
    $icuRoot = Join-Path $repoRoot "build/deps/icu-win-x64"
    $icuLicenseSource = Find-NoticeFile $icuRoot '^(LICENSE|LICENSE\.txt|COPYRIGHT|COPYING)$' '(?i)UNICODE LICENSE|ICU' "ICU official license"

    $arrowLicenseSource = $null
    $arrowNoticeSource = $null
    $arrowWheelRoot = Join-Path $repoRoot "build/bi-verification/wheel"
    if (Test-Path -LiteralPath $arrowWheelRoot -PathType Container) {
        $arrowDistInfo = Get-ChildItem -LiteralPath $arrowWheelRoot -Directory -Force |
            Where-Object { $_.Name -match '^pyarrow-[^/\\]+\.dist-info$' } |
            Sort-Object Name -Descending |
            Select-Object -First 1
        if ($null -ne $arrowDistInfo) {
            $wheelLicense = Join-Path $arrowDistInfo.FullName "licenses/LICENSE.txt"
            $wheelNotice = Join-Path $arrowDistInfo.FullName "licenses/NOTICE.txt"
            if ((Test-Path -LiteralPath $wheelLicense -PathType Leaf) -and
                (Test-Path -LiteralPath $wheelNotice -PathType Leaf)) {
                $arrowLicenseSource = [System.IO.Path]::GetFullPath($wheelLicense)
                $arrowNoticeSource = [System.IO.Path]::GetFullPath($wheelNotice)
            }
        }
    }
    if ([string]::IsNullOrWhiteSpace($arrowLicenseSource) -or [string]::IsNullOrWhiteSpace($arrowNoticeSource)) {
        $licenseRoot = Join-Path $repoRoot "licenses"
        $arrowLicenseSource = Get-ChildItem -LiteralPath $licenseRoot -File -Force |
            Where-Object { $_.Name -match '^Arrow.*-LICENSE\.txt$' } |
            Sort-Object Name -Descending |
            Select-Object -First 1 |
            ForEach-Object { [System.IO.Path]::GetFullPath($_.FullName) }
        $arrowNoticeSource = Get-ChildItem -LiteralPath $licenseRoot -File -Force |
            Where-Object { $_.Name -match '^Arrow.*-NOTICE\.txt$' } |
            Sort-Object Name -Descending |
            Select-Object -First 1 |
            ForEach-Object { [System.IO.Path]::GetFullPath($_.FullName) }
    }
    if ([string]::IsNullOrWhiteSpace($arrowLicenseSource) -or [string]::IsNullOrWhiteSpace($arrowNoticeSource)) {
        throw "Unable to resolve matching Apache Arrow LICENSE and NOTICE files from the current dependency inputs"
    }

    $licenseCopies = @(
        @{ Source = "licenses/Apache-2.0.txt"; Name = "Apache-2.0.txt" },
        @{ Source = $arrowLicenseSource; Name = "Arrow-LICENSE.txt" },
        @{ Source = $arrowNoticeSource; Name = "Arrow-NOTICE.txt" },
        @{ Source = $icuLicenseSource; Name = "ICU-LICENSE.txt" },
        @{ Source = "build/deps/bi-sql-win-x64/mysql/LICENSE"; Name = "MySQL-LICENSE" },
        @{ Source = "build/deps/bi-sql-win-x64/mysql/README"; Name = "MySQL-README" },
        @{ Source = "build/deps/bi-sql-win-x64/maria/COPYING"; Name = "MariaDB-COPYING" },
        @{ Source = "build/deps/bi-sql-win-x64/maria/README.md"; Name = "MariaDB-README.md" },
        @{ Source = "build/deps/bi-sql-win-x64/oracle/BASIC_LICENSE"; Name = "Oracle-BASIC_LICENSE" },
        @{ Source = "build/deps/bi-sql-win-x64/oracle/BASIC_README"; Name = "Oracle-BASIC_README" },
        @{ Source = "build/deps/bi-sql-win-x64/oracle/SDK_LICENSE"; Name = "Oracle-SDK_LICENSE" },
        @{ Source = "build/deps/bi-sql-win-x64/oracle/SDK_README"; Name = "Oracle-SDK_README" },
        @{ Source = "build/deps/expat-2.10.0/COPYING"; Name = "expat-COPYING" }
    )
    foreach ($copy in $licenseCopies) {
        $source = if ([System.IO.Path]::IsPathRooted($copy.Source)) {
            $copy.Source
        } else {
            Join-Path $repoRoot $copy.Source
        }
        Copy-Required $source $vendorTarget "vendor notice $source" $copy.Name
    }
    $noticeProvenance = $licenseCopies | ForEach-Object {
        $source = if ([System.IO.Path]::IsPathRooted($_.Source)) { $_.Source } else { Join-Path $repoRoot $_.Source }
        "- $($_.Name) <= $([System.IO.Path]::GetFullPath($source))"
    }

    $nativeRuntimeFiles = foreach ($nativeRuntimeSource in $nativeRuntimeSources) {
        Get-RelativeFileList $nativeRuntimeSource | Where-Object {
            @(".dll", ".json", ".dat", ".xml", ".ini", ".cfg", ".config", ".cnf", ".lic", ".txt") -contains ([System.IO.Path]::GetExtension($_).ToLowerInvariant())
        } | ForEach-Object { "$nativeRuntimeSource/$_" }
    }
    $pluginRuntimeFiles = Get-RelativeFileList $pluginTarget
    $noticeFiles = Get-RelativeFileList $vendorTarget
    $buildInfo = @"
# UniversalData BI Windows x64 delivery

- Generated (UTC): $([DateTime]::UtcNow.ToString("o"))
- Delivery root: $output
- Native CMake staging input: $native
- Native core staging input: $nativeCoreSource
- Native runtime staging inputs: $($nativeRuntimeSources -join '; ')
- Plugin Release input: $pluginRelease
- Management publish input: $management
- Worker publish input: $worker
- GUI input: $gui
- SDK version read from: $(Join-Path $repoRoot "include/ud/version.h")
- Management/Worker source core SHA-256: $managementCoreHash
- Native staging core SHA-256: $nativeCoreHash
- Native staging core matched publish core: $nativeCoreMatchesPublish

The Management and Worker publish trees were copied recursively, preserving
runtime and localized subdirectories. The CMake-staged Native core and runtime
closure are authoritative in the delivery and are copied into both host trees;
the source publish trees are not modified. The package contains no credentials
or database data.

Native source freeze is not asserted by this script. Re-run packaging after the
Native source is frozen and confirm the staging core hash is the same as the
Management/Worker pair.
"@
    Write-TextFile (Join-Path $stage "BUILD-INFO.md") ($buildInfo.TrimStart() + "`n")

    $components = @"
# Windows x64 runtime inventory

This file is generated from the actual package inputs; it intentionally does
not claim vendor versions that are not read from those inputs.

## CMake-staged native runtime candidates

$($nativeRuntimeFiles | ForEach-Object { "- $_" } | Out-String)
## Packaged plugin runtime files

$($pluginRuntimeFiles | ForEach-Object { "- $_" } | Out-String)
## Vendor notices copied

$($noticeFiles | ForEach-Object { "- $_" } | Out-String)

## Notice source provenance

$($noticeProvenance | Out-String)

The complete publish trees remain under `management/` and `worker/`; their
recursive contents, including `runtimes/`, are covered by checksums/SHA256SUMS.txt.
"@
    Write-TextFile (Join-Path $vendorTarget "COMPONENTS.md") ($components.TrimStart() + "`n")

    Assert-NoReparsePoints $stage "staged delivery"
    $loaderResult = Invoke-LoaderValidation $stage $sdkVersion $pluginNames $repoRoot
    $verification = @"
# Verification record

- Recursive Management and Worker publish-tree copy: passed.
- CMake-staged Native core overlay into both host trees: passed.
- Recursive GUI copy: passed.
- CMake-staged six-connector presence check: passed.
- Staged delivery reparse-point check: passed.
- System-only PATH native loader: passed.
- Loader evidence: $loaderResult
- Management/Worker source core SHA-256: $managementCoreHash
- Native staging core SHA-256: $nativeCoreHash
- Delivery core selected from Native staging: $nativeCoreHash
- Native source freeze: not asserted; this is a current verification package.
- Complete-file SHA-256 manifest: generated after this record.

The package scripts do not embed historical database preview or publication
claims. Database and Management health evidence must be reported with the
matching runtime process and current fixture separately.
"@
    Write-TextFile (Join-Path $stage "VERIFICATION.md") ($verification.TrimStart() + "`n")
    Write-TextFile (Join-Path $checksumTarget "README.md") "# Checksums`n`nSHA256SUMS.txt contains every delivered file except itself, with paths relative to the delivery root.`n"
    Write-ShaManifest $stage
    Assert-NoReparsePoints $stage "staged delivery"

    if (Test-Path -LiteralPath $output) {
        Assert-ExistingDeliveryTarget $output
        [System.IO.Directory]::Move($output, $backup)
        $backupMoved = $true
    }
    [System.IO.Directory]::Move($stage, $output)
    $stagePromoted = $true

    if ($backupMoved -and (Test-Path -LiteralPath $backup)) {
        Assert-NoReparsePoints $backup "previous delivery target"
        [System.IO.Directory]::Delete($backup, $true)
    }
    Write-Output "Packaged BI Windows x64 delivery: $output"
    Write-Output "Management/Worker core SHA-256: $managementCoreHash"
    Write-Output "Native staging core SHA-256: $nativeCoreHash"
    Write-Output "SQL plugins: $($pluginNames.Count)"
    Write-Output "All delivered-file SHA-256 entries: $([int]((Get-Content -LiteralPath (Join-Path $output "checksums/SHA256SUMS.txt") | Where-Object { $_ -and $_ -notmatch '^#' }).Count))"
} catch {
    if ($backupMoved -and -not $stagePromoted -and -not (Test-Path -LiteralPath $output) -and (Test-Path -LiteralPath $backup)) {
        [System.IO.Directory]::Move($backup, $output)
    }
    if (Test-Path -LiteralPath $stage) {
        Assert-NoReparsePoints $stage "failed staging directory"
        [System.IO.Directory]::Delete($stage, $true)
    }
    throw
}
