param(
    [string]$ProjectId = 'cosmic-eye-telemetry-demo',
    [switch]$AfterViva,
    [switch]$Execute
)
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$compose = Join-Path $root 'infra\compose.yaml'

if (-not $Execute) {
    Write-Host 'DRY RUN: would stop Compose services and remove their volumes.'
    Write-Host "DRY RUN: would delete BigQuery datasets ${ProjectId}:raw and ${ProjectId}:curated."
    Write-Host 'Atlas cluster and GCP project deletion are manual; see docs/TEARDOWN_CHECKLIST.md.'
    exit 0
}
if (-not $AfterViva) { throw 'Refusing: rerun with -AfterViva only after your final demo/viva is finished.' }
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw 'Docker CLI is required.' }
if (-not (Get-Command bq -ErrorAction SilentlyContinue)) { throw 'Google Cloud bq CLI is required.' }

Write-Host 'This permanently deletes local Compose volumes and BigQuery datasets:'
Write-Host "  ${ProjectId}:raw"
Write-Host "  ${ProjectId}:curated"
Write-Host 'It does not delete MongoDB Atlas or the GCP project.'
$confirmation = Read-Host 'Type DELETE COSMIC EYE RESOURCES to continue'
if ($confirmation -cne 'DELETE COSMIC EYE RESOURCES') { Write-Host 'Cancelled; no resources changed.'; exit 1 }

docker compose -f $compose down --volumes --remove-orphans
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose cleanup failed; BigQuery datasets were not deleted.' }
bq rm -r -f "${ProjectId}:raw"
if ($LASTEXITCODE -ne 0) { throw 'Deleting the raw dataset failed.' }
bq rm -r -f "${ProjectId}:curated"
if ($LASTEXITCODE -ne 0) { throw 'Deleting the curated dataset failed.' }
Write-Host 'Local Compose services/volumes and the two named BigQuery datasets were removed.'
Write-Host 'Complete the remaining manual steps in docs/TEARDOWN_CHECKLIST.md.'
