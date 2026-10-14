#Requires -Version 7.0
param(
    [Parameter(Mandatory)][string]$BuildDirectory,
    [ValidateSet('Release')][string]$Configuration = 'Release',
    [ValidateSet('win-x64','linux-x64')][string]$RuntimeIdentifier = $(if ($IsWindows) { 'win-x64' } else { 'linux-x64' }),
    [string]$Output = "$PSScriptRoot/../artifacts",
    [string]$LogDirectory = (Join-Path ([IO.Path]::GetTempPath()) ("UniversalData-package-" + [guid]::NewGuid().ToString('N')))
)
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$repository = (Resolve-Path -LiteralPath "$PSScriptRoot/..").Path
$build = (Resolve-Path -LiteralPath $BuildDirectory).Path
$outputDirectory = [IO.Path]::GetFullPath($Output)
$logDirectoryPath = [IO.Path]::GetFullPath($LogDirectory)
$product = (Get-Content -LiteralPath (Join-Path $repository 'metadata/product.json') -Raw | ConvertFrom-Json).product
$version = $product.sdkVersion
$abi = [uint32]$product.abiVersion
$cachePath = Join-Path $build 'CMakeCache.txt'
if (!(Test-Path -LiteralPath $cachePath -PathType Leaf)) { throw 'Configure the native build before packaging.' }
$cache = @{}
foreach ($line in Get-Content -LiteralPath $cachePath) {
    if ($line -match '^([^#/][^:]*):[^=]+=(.*)$') { $cache[$Matches[1]] = $Matches[2] }
}
if ([IO.Path]::GetFullPath($cache['CMAKE_HOME_DIRECTORY']) -ne $repository) { throw 'Native build belongs to another source tree.' }
if ($cache['CMAKE_PROJECT_VERSION'] -ne $version) { throw 'Native build version differs from VERSION; reconfigure it.' }
if ($cache['UD_SANITIZE'] -eq 'ON') { throw 'A distributable Release SDK cannot use the sanitizer runtime.' }
if ($cache['UD_BUILD_HOSTS'] -ne 'ON' -or $cache['UD_BUILD_JNI'] -ne 'ON') { throw 'The full SDK bundle requires hosts and JNI.' }
if (($IsWindows -and $RuntimeIdentifier -ne 'win-x64') -or (!$IsWindows -and $RuntimeIdentifier -ne 'linux-x64')) {
    throw 'Package only native binaries built for the current platform.'
}
if (!$IsWindows -and $cache['CMAKE_BUILD_TYPE'] -ne $Configuration) { throw 'Configure the single-config native build with CMAKE_BUILD_TYPE=Release.' }
$destination = Join-Path $outputDirectory "$version/$RuntimeIdentifier"
$archiveName = "UniversalData-$version-$RuntimeIdentifier.zip"
$archiveDestination = Join-Path $outputDirectory $archiveName
if ((Test-Path -LiteralPath $destination) -or (Test-Path -LiteralPath $archiveDestination)) {
    throw "Delivery already exists. Review it before replacing: $destination"
}
New-Item -ItemType Directory -Force -Path $logDirectoryPath | Out-Null
$stage = Join-Path $logDirectoryPath 'bundle'
if (Test-Path -LiteralPath $stage) { throw "Staging directory already exists: $stage" }
New-Item -ItemType Directory -Path $stage | Out-Null
function Invoke-PackageTool([string]$Command, [string[]]$ToolArguments, [string]$LogName) {
    $logPath = Join-Path $logDirectoryPath $LogName
    & $Command @ToolArguments *> $logPath
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        $errors = Get-Content -LiteralPath $logPath | Select-String -Pattern 'error|failed|exception' | Select-Object -First 12
        throw ("$LogName failed (exit $exitCode). Log: $logPath" + [Environment]::NewLine + ($errors -join [Environment]::NewLine))
    }
    Write-Output "$LogName exit=0"
}
$cmake = (Get-Command cmake -ErrorAction Stop).Source
$dotnet = (Get-Command dotnet -ErrorAction Stop).Source
$maven = (Get-Command mvn -ErrorAction Stop).Source
$python = (Get-Command python -ErrorAction Stop).Source
Push-Location -LiteralPath $repository
try {
    Invoke-PackageTool $python @('scripts/product_model.py','check') 'product-model.log'
    $buildArguments = @('--build',$build,'--config',$Configuration,'--parallel','4')
    if ($IsWindows) { $buildArguments += @('--','/nologo','/verbosity:quiet') }
    Invoke-PackageTool $cmake $buildArguments 'native-build.log'
    $sdk = Join-Path $stage 'sdk'
    Invoke-PackageTool $cmake @('--install',$build,'--config',$Configuration,'--prefix',$sdk) 'native-install.log'
    $nativeDirectory = Join-Path $sdk $(if ($IsWindows) { 'bin' } else { 'lib' })
    $nativeLibrary = if ($IsWindows) { 'ud_data.dll' } else { 'libud_data.so' }
    $jniLibrary = if ($IsWindows) { 'ud_java.dll' } else { 'libud_java.so' }
    foreach ($file in @($nativeLibrary,$jniLibrary)) {
        if (!(Test-Path -LiteralPath (Join-Path $nativeDirectory $file) -PathType Leaf)) { throw "Missing installed library: $file" }
    }
    foreach ($hostName in @('ud_cli','ud_worker')) {
        $folder = if ($hostName -eq 'ud_cli') { 'cli' } else { 'worker' }
        $hostFile = if ($IsWindows) { "$hostName.exe" } else { $hostName }
        if (!(Test-Path -LiteralPath (Join-Path $sdk "bin/universaldata/$folder/$hostFile") -PathType Leaf)) { throw "Missing installed host: $hostFile" }
    }
    if (!(Test-Path -LiteralPath (Join-Path $sdk 'bin/universaldata/management/wwwroot/index.html'))) { throw 'Management/designer product assets are missing.' }
    $header = Get-Content -LiteralPath (Join-Path $sdk 'include/ud/version.h') -Raw
    if ($header -notmatch "#define UD_ABI_VERSION $($abi)u") { throw 'The installed SDK ABI differs from the product model.' }
    $capabilities = [ordered]@{}
    foreach ($feature in @('sqlite','postgresql','odbc','mysql','mariadb','oracle_oci','arrow_files','http','xml','network')) { $capabilities[$feature] = ($cache['UD_WITH_' + $feature.ToUpperInvariant()] -eq 'ON') }
    if ($cache['UD_BUILD_PROFILE'] -eq 'PRODUCT' -and $capabilities.Values -contains $false) { throw 'Complete product dependencies cannot silently downgrade.' }
    foreach ($driver in @('sqlite','postgresql','odbc','mysql','mariadb','oracle_oci')) {
        if ($capabilities[$driver]) {
            $plugin = "ud_connector_$driver" + $(if ($IsWindows) { '.dll' } else { '.so' })
            if (!(Test-Path -LiteralPath (Join-Path $sdk "lib/UniversalData/plugins/$plugin") -PathType Leaf)) { throw "Missing enabled connector: $plugin" }
        }
    }
    $packages = Join-Path $stage 'packages'
    New-Item -ItemType Directory -Path $packages | Out-Null
    Invoke-PackageTool $dotnet @('pack','bindings/dotnet/UniversalData/UniversalData.csproj','--configuration','Release','--nologo','--verbosity:minimal',"-p:NativeLibraryDirectory=$nativeDirectory","-p:NativeRuntimeIdentifier=$RuntimeIdentifier",'--output',$packages) 'nuget-pack.log'
    Invoke-PackageTool $dotnet @('restore','examples/dotnet/Consumer.csproj','--nologo') 'consumer-dependencies.log'
    Invoke-PackageTool $python @('packaging/nuget_feed.py','--assets','bindings/dotnet/UniversalData/obj/project.assets.json','--assets','examples/dotnet/obj/project.assets.json','--output',$packages) 'nuget-dependencies.log'
    Invoke-PackageTool $maven @('-B','-q','-f','bindings/java/pom.xml','package') 'maven-package.log'
    $jar = Join-Path $repository "bindings/java/target/universal-data-$version.jar"
    if (!(Test-Path -LiteralPath $jar -PathType Leaf)) { throw 'Maven did not produce the current JAR.' }
    Copy-Item -LiteralPath $jar -Destination $packages
    Copy-Item -LiteralPath (Join-Path $repository 'bindings/java/pom.xml') -Destination (Join-Path $packages "universal-data-$version.pom")
    $fingerprint = & $python scripts/release_guard.py --fingerprint
    if ($LASTEXITCODE -ne 0 -or $fingerprint -notmatch '^[a-f0-9]{64}$') { throw 'Unable to fingerprint the current source.' }
    $files = @(Get-ChildItem -LiteralPath $stage -Recurse -File | Sort-Object FullName | ForEach-Object {
        [ordered]@{ path = [IO.Path]::GetRelativePath($stage,$_.FullName).Replace('\','/'); bytes = $_.Length; sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant() }
    })
    $manifest = [ordered]@{
        schemaVersion = $product.protocolVersion; protocolVersion = $product.protocolVersion; buildProfile = $cache['UD_BUILD_PROFILE']; sdkVersion = $version; abiVersion = $abi; runtimeIdentifier = $RuntimeIdentifier
        configuration = $Configuration; sourceFingerprint = $fingerprint; capabilities = $capabilities
        files = $files
    }
    $manifest | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $stage 'manifest.json') -Encoding utf8NoBOM
    $archive = Join-Path $logDirectoryPath $archiveName
    [IO.Compression.ZipFile]::CreateFromDirectory($stage,$archive)
    $archiveHash = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant()
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $destination) | Out-Null
    if (![IO.Path]::GetFullPath($stage).StartsWith($logDirectoryPath + [IO.Path]::DirectorySeparatorChar)) { throw 'Invalid staging path.' }
    Move-Item -LiteralPath $stage -Destination $destination
    Move-Item -LiteralPath $archive -Destination $archiveDestination
    "$archiveHash  $archiveName" | Set-Content -LiteralPath "$archiveDestination.sha256" -Encoding utf8NoBOM
    Write-Output "SDK: $destination/sdk"
    Write-Output "Archive: $archiveDestination"
    Write-Output "SHA256: $archiveHash"
    Write-Output "Logs: $logDirectoryPath"
} finally {
    Pop-Location
}
