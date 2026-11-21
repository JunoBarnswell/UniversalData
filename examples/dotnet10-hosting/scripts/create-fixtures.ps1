[CmdletBinding()]
param(
    [ValidateSet('sqlite')]
    [string]$Mode = 'sqlite',
    [string]$Output = '',
    [string]$ApplicationBase = ''
)

$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
if ([string]::IsNullOrWhiteSpace($Output)) { $Output = Join-Path $Root '.local' }
if (-not [IO.Path]::IsPathRooted($Output)) { $Output = Join-Path $Root $Output }
if ([string]::IsNullOrWhiteSpace($ApplicationBase)) {
    $debug = Join-Path $Root 'bin/Debug/net10.0'
    $ApplicationBase = if (Test-Path $debug) { $debug } else { Join-Path $Root 'bin/Release/net10.0' }
}
$ApplicationBase = [IO.Path]::GetFullPath($ApplicationBase)
$sqlite = Join-Path $Output 'sqlite'
$resources = Join-Path $sqlite 'resources'
$private = Join-Path $sqlite 'private'
$state = Join-Path $sqlite 'state'
New-Item -ItemType Directory -Force -Path $ApplicationBase, $resources, $private, $state | Out-Null

$database = Join-Path $resources 'orders.sqlite'
$policyPath = Join-Path $private 'host-policy.json'
$credentialPath = Join-Path $private 'orders-sqlite.connection'
[IO.File]::WriteAllText($credentialPath, "Data Source=$database")

$grants = @{ $database = @('Open', 'Read', 'Write', 'Discover') }
$policy = @{
    dataDirectory = $state
    fileRoots = @($resources)
    networkOrigins = @()
    pluginRoots = @($ApplicationBase)
    credentialFiles = @{ 'orders-sqlite' = $credentialPath }
    resources = $grants
    tenants = @{
        'dotnet10-hosting' = @{
            fileRoots = @($resources)
            networkOrigins = @()
            pluginRoots = @($ApplicationBase)
            credentialReferences = @('orders-sqlite')
            resources = $grants
        }
    }
}
$policy | ConvertTo-Json -Depth 8 | Set-Content -Path $policyPath -Encoding utf8NoBOM

@"
hostPolicy:
  path: '$policyPath'
  tenant: dotnet10-hosting
sources:
  orders-sqlite:
    kind: sqlite
    resource: '$database'
    credentialReference: orders-sqlite
worker:
  maxConcurrentSessions: 2
  maxConnectionsPerSource: 2
"@ | Set-Content -Path (Join-Path $sqlite 'universaldata.yaml') -Encoding utf8NoBOM
Write-Host "SQLite fixture generated at $sqlite"
