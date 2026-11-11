$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$tempRoot = Join-Path $env:TEMP "cosmic-eye-phase6"
$pidFile = Join-Path $tempRoot "processes.json"

if (Test-Path -LiteralPath $pidFile) {
    $processIds = Get-Content -LiteralPath $pidFile -Raw | ConvertFrom-Json
    if ($processIds.dashboard) { Stop-Process -Id ([int]$processIds.dashboard) -Force -ErrorAction SilentlyContinue }
    Remove-Item -LiteralPath $pidFile -Force
}

docker compose --env-file (Join-Path $projectRoot ".env") -f (Join-Path $projectRoot "infra\compose.yaml") --profile streaming down
Write-Host "Stopped the Phase 6 processes and Redpanda container."
