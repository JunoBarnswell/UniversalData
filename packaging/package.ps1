param([Parameter(Mandatory)][string]$NativeArtifacts,[string]$Output = "$PSScriptRoot/../artifacts/packages")
$ErrorActionPreference = 'Stop'
foreach ($name in @('win-x64/ud_data.dll','linux-x64/libud_data.so')) {
    if (!(Test-Path -LiteralPath (Join-Path $NativeArtifacts $name))) { throw "Missing native artifact: $name" }
}
New-Item -ItemType Directory -Force -Path $Output | Out-Null
dotnet pack "$PSScriptRoot/../bindings/dotnet/UniversalData/UniversalData.csproj" --nologo --verbosity:minimal -p:NativeArtifacts=$NativeArtifacts -o $Output
if ($LASTEXITCODE -ne 0) { throw "NuGet pack failed ($LASTEXITCODE)" }
mvn -q -f "$PSScriptRoot/../bindings/java/pom.xml" package
if ($LASTEXITCODE -ne 0) { throw "Maven package failed ($LASTEXITCODE)" }
Copy-Item -LiteralPath "$PSScriptRoot/../bindings/java/target/universal-data-0.1.0.jar" -Destination $Output
