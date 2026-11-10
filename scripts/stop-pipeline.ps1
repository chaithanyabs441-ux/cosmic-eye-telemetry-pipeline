$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$compose = Join-Path $root 'infra\compose.yaml'

# The broker uses temporary in-memory storage. Removing the container also
# removes all broker data created during this local run.
docker compose -f $compose down --volumes --remove-orphans
if ($LASTEXITCODE -ne 0) { throw 'Could not stop the local pipeline containers.' }
Write-Host 'Local pipeline containers stopped; temporary broker data was discarded.'
