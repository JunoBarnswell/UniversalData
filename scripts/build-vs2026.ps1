[CmdletBinding()]
param(
    [ValidateSet('Build', 'Rebuild', 'Clean')]
    [string]$Action = 'Build',
    [ValidateSet('Debug', 'Release')]
    [string]$Configuration = 'Debug',
    [string]$VisualStudioPath,
    [ValidateRange(1, 256)]
    [int]$Jobs = 4
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
$projectRoot = Split-Path -Parent $PSScriptRoot
$buildDirectory = Join-Path $projectRoot 'build\vs2026'
$logDirectory = Join-Path ([IO.Path]::GetTempPath()) 'UniversalData-vs2026'
[IO.Directory]::CreateDirectory($logDirectory) | Out-Null
$logPath = Join-Path $logDirectory ((Get-Date -Format 'yyyyMMdd-HHmmss') + '-' + [Guid]::NewGuid().ToString('N') + '.log')

function Invoke-CMakeStep([string]$Step, [string[]]$Arguments) {
    Write-Host "UniversalData.Native: $Step ($Configuration)"
    & $script:cmakePath @Arguments *> $script:stepLogPath
    $stepExitCode = $LASTEXITCODE
    Get-Content -LiteralPath $script:stepLogPath | Add-Content -LiteralPath $script:logPath
    Remove-Item -LiteralPath $script:stepLogPath
    if ($stepExitCode -ne 0) {
        Write-Host "CMake $Step failed with exit code $stepExitCode. Log: $script:logPath"
        $errors = @(Select-String -LiteralPath $script:logPath -Pattern 'CMake Error|error [A-Z]+[0-9]+|Could NOT|FAILED:' -Context 0,2 | Select-Object -First 12)
        if ($errors.Count) { $errors | ForEach-Object { Write-Host $_.Line; $_.Context.PostContext | ForEach-Object { Write-Host $_ } } }
        else { Get-Content -LiteralPath $script:logPath -Tail 12 | ForEach-Object { Write-Host $_ } }
        exit $stepExitCode
    }
}

try {
    $vswherePath = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio\Installer\vswhere.exe'
    if (!(Test-Path -LiteralPath $vswherePath)) { throw 'Visual Studio Installer (vswhere.exe) is missing. Install VS2026 with Desktop development with C++.' }
    $instanceJson = & $vswherePath -products '*' -version '[18.0,19.0)' -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -format json -utf8
    if ($LASTEXITCODE -ne 0) { throw 'Unable to query VS2026 installations with vswhere.exe.' }
    $instances = ConvertFrom-Json -InputObject ($instanceJson -join [Environment]::NewLine)
    $instances = @($instances)
    if (!$instances.Count) { throw 'VS2026 C++ tools are missing. Install the Desktop development with C++ workload and a Windows SDK.' }
    if (!$VisualStudioPath) { $VisualStudioPath = $env:VSINSTALLDIR }
    if ($VisualStudioPath) {
        $requestedPath = [IO.Path]::GetFullPath($VisualStudioPath).TrimEnd([char[]]'\/')
        $selectedInstance = $instances | Where-Object { [IO.Path]::GetFullPath($_.installationPath).TrimEnd([char[]]'\/') -eq $requestedPath } | Select-Object -First 1
        if (!$selectedInstance) { throw "The selected installation is not a usable VS2026 C++ instance: $requestedPath" }
    }
    else { $selectedInstance = $instances | Sort-Object { [version]$_.installationVersion } -Descending | Select-Object -First 1 }

    $cmakePath = Join-Path $selectedInstance.installationPath 'Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe'
    if (!(Test-Path -LiteralPath $cmakePath)) {
        $cmakeCommand = Get-Command cmake.exe -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
        if (!$cmakeCommand) { throw 'CMake is missing. Install C++ CMake tools for Windows in VS2026 or add CMake 4.2+ to PATH.' }
        $cmakePath = $cmakeCommand.Source
    }
    $cmakeVersionText = & $cmakePath --version
    if ($LASTEXITCODE -ne 0 -or ($cmakeVersionText -join "`n") -notmatch 'cmake version (\d+\.\d+\.\d+)') { throw 'Unable to determine the selected CMake version.' }
    if ([version]$Matches[1] -lt [version]'4.2.0') { throw 'VS2026 requires CMake 4.2+. Update C++ CMake tools for Windows or the CMake installation.' }
    Write-Host "VS2026: $($selectedInstance.installationPath)"
    Write-Host "CMake: $cmakePath"
    Write-Host "Log: $logPath"

    $cachePath = Join-Path $buildDirectory 'CMakeCache.txt'
    if (Test-Path -LiteralPath $cachePath) {
        $cachedInstance = Select-String -LiteralPath $cachePath -Pattern '^CMAKE_GENERATOR_INSTANCE:[^=]+=(.+)$' | Select-Object -First 1
        if ($cachedInstance -and [IO.Path]::GetFullPath($cachedInstance.Matches[0].Groups[1].Value).TrimEnd([char[]]'\/') -ne [IO.Path]::GetFullPath($selectedInstance.installationPath).TrimEnd([char[]]'\/')) {
            throw "build/vs2026 was configured with a different VS installation. Remove only build/vs2026 and build again with the selected instance."
        }
    }
    $stepLogPath = $logPath + '.step'
    Push-Location -LiteralPath $projectRoot
    try {
        if ($Action -eq 'Clean') {
            if (!(Test-Path -LiteralPath $cachePath)) { Write-Host 'No configured VS2026 build to clean.'; exit 0 }
            Invoke-CMakeStep 'Clean' @('--build', $buildDirectory, '--config', $Configuration, '--target', 'clean', '--', '/nologo', '/verbosity:quiet')
        }
        else {
            Invoke-CMakeStep 'Configure' @('--preset', 'vs2026', ('-DCMAKE_GENERATOR_INSTANCE=' + $selectedInstance.installationPath.Replace('\', '/')))
            $buildArguments = @('--build', $buildDirectory, '--config', $Configuration, '--parallel', "$Jobs")
            if ($Action -eq 'Rebuild') { $buildArguments += '--clean-first' }
            $buildArguments += @('--', '/nologo', '/verbosity:quiet')
            Invoke-CMakeStep $Action $buildArguments
        }
    }
    finally { Pop-Location }
    Write-Host "UniversalData.Native: $Action succeeded ($Configuration)."
    exit 0
}
catch {
    [Console]::Error.WriteLine("error UDVS2026: $($_.Exception.Message)")
    exit 1
}
