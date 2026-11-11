$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composeFile = Join-Path $projectRoot "infra\compose.yaml"
$composeEnvFile = Join-Path $projectRoot ".env"
$pythonExe = Join-Path $projectRoot "pipeline\.venv\Scripts\python.exe"
$tempRoot = Join-Path $env:TEMP "cosmic-eye-phase6"
$dashboardLog = Join-Path $tempRoot "dashboard.log"
$dashboardError = Join-Path $tempRoot "dashboard-error.log"
$pidFile = Join-Path $tempRoot "processes.json"

if (-not (Test-Path -LiteralPath $pythonExe)) {
    throw "Pipeline Python environment not found: $pythonExe"
}
New-Item -ItemType Directory -Path $tempRoot -Force | Out-Null

Write-Host "Starting Redpanda..."
& docker compose --env-file $composeEnvFile -f $composeFile up -d redpanda
$containerId = (& docker compose --env-file $composeEnvFile -f $composeFile ps -q redpanda).Trim()
if (-not $containerId) { throw "Redpanda container did not start. Is Docker Desktop running?" }

$health = ""
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    $health = (& docker inspect --format '{{.State.Health.Status}}' $containerId).Trim()
    if ($health -eq "healthy") { break }
    Start-Sleep -Seconds 2
}
if ($health -ne "healthy") { throw "Redpanda health check did not pass (state: $health)." }

Write-Host "Creating the live topic before Spark subscribes..."
& $pythonExe pipeline\live_producer.py --create-topic-only
if ($LASTEXITCODE -ne 0) { throw "Could not create the live topic. Check Redpanda and KAFKA_BOOTSTRAP_SERVERS." }

Write-Host "Starting the Linux Spark worker..."
$sparkImage = (& docker image inspect infra-spark-streaming:latest --format '{{.Id}}' 2>$null | Out-String).Trim()
if ($LASTEXITCODE -eq 0 -and $sparkImage) {
    & docker compose --env-file $composeEnvFile -f $composeFile --profile streaming up -d spark-streaming
} else {
    Write-Host "Spark worker image is not built yet; building it now..."
    & docker compose --env-file $composeEnvFile -f $composeFile --profile streaming up -d --build spark-streaming
}
if ($LASTEXITCODE -ne 0) { throw "Could not start the Spark worker. Check Docker Desktop and the image build output." }

Write-Host "Waiting for the Linux Spark worker and its Kafka connector..."
$streamStarted = $false
for ($attempt = 0; $attempt -lt 120; $attempt++) {
    $logText = (& docker compose --env-file $composeEnvFile -f $composeFile --profile streaming logs --no-color spark-streaming 2>&1 | Out-String)
    if ($logText -match "three queries active") { $streamStarted = $true; break }
    $sparkContainerId = (& docker compose --env-file $composeEnvFile -f $composeFile --profile streaming ps -q spark-streaming).Trim()
    $sparkState = if ($sparkContainerId) { (& docker inspect --format '{{.State.Status}}' $sparkContainerId).Trim() } else { "missing" }
    if ($sparkState -eq "exited" -or $sparkState -eq "dead") { break }
    Start-Sleep -Seconds 2
}
if (-not $streamStarted) {
    $details = (& docker compose --env-file $composeEnvFile -f $composeFile --profile streaming logs --no-color spark-streaming 2>&1 | Out-String)
    throw "Spark did not start. Check Java, MongoDB access, or the Maven connector download. $details"
}

Write-Host "Starting Streamlit dashboard..."
$dashboardProcess = Start-Process -FilePath $pythonExe `
    -ArgumentList @("-u", "-m", "streamlit", "run", "dashboard\app.py", "--server.headless", "true") `
    -WorkingDirectory $projectRoot -PassThru -WindowStyle Hidden `
    -RedirectStandardOutput $dashboardLog -RedirectStandardError $dashboardError
$dashboardReady = $false
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8501/_stcore/health" -TimeoutSec 2
        if ($response.StatusCode -eq 200) { $dashboardReady = $true; break }
    } catch { }
    if ($dashboardProcess.HasExited) { break }
    Start-Sleep -Seconds 2
}
if (-not $dashboardReady) {
    $details = if (Test-Path -LiteralPath $dashboardError) { Get-Content -LiteralPath $dashboardError -Raw } else { "No dashboard log was created." }
    Stop-Process -Id $dashboardProcess.Id -Force -ErrorAction SilentlyContinue
    & docker compose --env-file $composeEnvFile -f $composeFile --profile streaming stop spark-streaming | Out-Null
    throw "Streamlit did not start. $details"
}

@{ dashboard = $dashboardProcess.Id } |
    ConvertTo-Json | Set-Content -LiteralPath $pidFile -Encoding UTF8

Write-Host "Publishing 40 synthetic events with injected anomalies..."
& $pythonExe pipeline\live_producer.py --count 40 --rate 2 --inject-anomaly-every 3
if ($LASTEXITCODE -ne 0) { throw "Live producer failed with exit code $LASTEXITCODE." }

Start-Process "http://localhost:8501"
Write-Host "Demo is live at http://localhost:8501"
Write-Host "Spark log: docker compose --env-file .env -f infra\compose.yaml --profile streaming logs -f spark-streaming"
Write-Host "To stop Spark/dashboard, run: powershell -ExecutionPolicy Bypass -File scripts\stop_streaming_demo.ps1"
