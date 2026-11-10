$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$compose = Join-Path $root 'infra\compose.yaml'

Write-Host 'Checking Docker Engine...'
docker info --format 'Docker Engine: {{.ServerVersion}}'
if ($LASTEXITCODE -ne 0) { throw 'Docker Engine is not reachable. Start Docker Desktop and retry.' }

Write-Host 'Starting the local Redpanda broker...'
docker compose -f $compose up -d
if ($LASTEXITCODE -ne 0) { throw 'Could not start Redpanda. Check Docker Desktop resources and logs.' }

docker compose -f $compose ps
