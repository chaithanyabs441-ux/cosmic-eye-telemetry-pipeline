# Phase 1 setup guide (Windows 10)

This setup uses the machine details you gave: Windows 10, Intel i5-11400F,
GTX 1650, and 16 GB RAM. The GPU is not used by this CPU-only course pipeline.
No paid cloud resource is required by these steps. Do not enable GCP billing.

## 1. Check Windows, memory, and free disk

Open PowerShell and run:

```powershell
Get-ComputerInfo | Select-Object WindowsProductName, WindowsVersion, OsBuildNumber
Get-CimInstance Win32_OperatingSystem | Select-Object @{N='TotalGiB';E={[math]::Round($_.TotalVisibleMemorySize / 1MB, 2)}}, @{N='FreeGiB';E={[math]::Round($_.FreePhysicalMemory / 1MB, 2)}}
Get-PSDrive C | Select-Object Name, @{N='FreeGiB';E={[math]::Round($_.Free / 1GB, 2)}}
```

Docker's current Windows requirements specify Windows 10 22H2 build 19045 for
supported editions, WSL 2.1.5+, virtualization enabled, and 8 GB RAM. Keep at
least 4 GiB available before starting the broker, as Redpanda's Docker
development setup asks for this. Close memory-heavy apps first if needed.

## 2. Install or start Docker Desktop

1. Open PowerShell and run `wsl --version`. You should see a WSL version and a
   kernel version. If the command is missing or shows the old inbox version,
   open **PowerShell as Administrator**, run `wsl --install` (or `wsl --update`),
   restart Windows if prompted, then run `wsl --version` again.
2. Install Docker Desktop from the [official Windows install page](https://docs.docker.com/desktop/setup/install/windows-install/).
   If offered, choose the per-user install and WSL 2 backend. Start Docker
   Desktop from the Start menu and wait for “Engine running”. Docker Desktop is
   free for personal, education, and qualifying small-business use; check its
   current terms if your use does not fit those categories.
3. In PowerShell, run:

   ```powershell
   docker version
   docker compose version
   docker info --format 'Docker Engine: {{.ServerVersion}}'
   ```

   Each command should print a version. If `docker info` says it cannot connect,
   start Docker Desktop, wait for the engine, then retry. If Windows reports
   virtualization is disabled, enable Intel VT-x in BIOS/UEFI or ask the PC
   administrator for help.

## 3. Python environment

Use Python 3.11 for the separate pipeline environment; leave the existing
backend environment alone. From the repository root in PowerShell:

```powershell
py -3.11 --version
py -3.11 -m venv pipeline\.venv
pipeline\.venv\Scripts\python.exe -m pip install --upgrade pip
pipeline\.venv\Scripts\python.exe -m pip install -r pipeline\requirements.txt
```

The first line should report Python 3.11.x. If the Python launcher cannot find
3.11, install the 64-bit Python 3.11 release from [python.org](https://www.python.org/downloads/windows/),
select **Add python.exe to PATH**, close and reopen PowerShell, and retry.
`pipeline\.venv` is ignored by Git.

## 4. Java for Spark

Install the Temurin 17 JDK from [Adoptium's official Windows installer page](https://adoptium.net/installation/windows/).
On the setup screen, enable the options to add Java to `PATH` and set
`JAVA_HOME`. Close and reopen PowerShell, then run:

```powershell
java -version
$env:JAVA_HOME
```

The Java version should start with `17`. Spark uses Java; the GPU is not needed.

## 5. Start the local broker

After Docker is running and at least 4 GiB memory is available, start one
single-node Redpanda broker:

```powershell
./scripts/start-pipeline.ps1
```

The last output should list service `redpanda` as running. Its Kafka endpoint is
`localhost:9092`. Broker data lives in temporary container memory; stopping the
container discards it. Stop it with:

```powershell
./scripts/stop-pipeline.ps1
```

The command removes the container and its temporary volume. It does not delete
the downloaded Docker image.

## 6. Create the BigQuery Sandbox project

1. Sign in to the [Google Cloud Console](https://console.cloud.google.com/) with
   your own Google account and create a project. Use suggested ID
   `cosmic-eye-telemetry-demo` if available; otherwise choose an ID and put it
   in your local `.env` file. Project IDs cannot be changed after creation.
2. Open BigQuery in the console and follow **Try BigQuery** / **Enable sandbox**.
   Do not add a billing account or payment card. You should see a Sandbox notice
   in BigQuery. If the console asks you to enable billing, stop and do not
   proceed; the Phase 1 design depends on Sandbox mode.
3. Sandbox currently allows 10 GiB lifetime storage (deleting data does not
   restore this quota), 1 TiB query processing per month, and 60-day default
   table expiration. It does not support streaming inserts, DML, or Data
   Transfer Service. The smoke test uses a one-row batch load job and deletes
   its temporary table; that tiny load still consumes some lifetime storage.
4. Create a dataset named `phase1_smoke` in BigQuery using the console. Then
   create a service account in **IAM & Admin → Service Accounts** (name it
   `cosmic-eye-pipeline`). Grant **BigQuery Job User**
   (`roles/bigquery.jobUser`) at project level, and grant **BigQuery Data
   Editor** (`roles/bigquery.dataEditor`) only on the `phase1_smoke` dataset.
   The smoke test creates, reads, and deletes a uniquely named temporary table
   there. Create a JSON key only if your course/project policy permits
   downloadable keys. Save it directly into the repository's ignored `secrets`
   folder as `secrets\gcp-service-account.json`.
5. Copy `.env.example` to `.env` in the repository root. Fill in
   `GCP_PROJECT_ID`, set `BIGQUERY_SMOKE_DATASET=phase1_smoke`, and keep
   `GOOGLE_APPLICATION_CREDENTIALS=secrets/gcp-service-account.json`. Never
   paste the key or its contents into chat or commit it.

### Budget and usage alerts

Google Cloud monetary budgets require a Cloud Billing account. Since the selected
Sandbox project must stay billing-disabled, a GCP spend budget alert is not
available and there is no billing alert to set up without changing this
free-only design. Do not link a billing account. Instead, keep the Sandbox
storage/query limits visible and later use query byte caps and warehouse usage
queries. GitHub Actions usage notifications can be configured in GitHub account
**Settings → Billing and licensing → Usage** after creating the repository.
Those notifications monitor included Actions usage; they are not a GCP budget
alert.

## 7. Create MongoDB Atlas Free

1. Sign in at [MongoDB Atlas](https://cloud.mongodb.com/) and create a new
   project, then choose **Build a Database → M0 Free**. Select a supported
   provider and nearby region. Do not select a paid tier, backup, or add-on.
2. Create a database user with a unique password. Save the URI and password
   locally only.
3. Under **Security → Network Access**, add your current public IP using
   **Add Current IP Address**. Do not use `0.0.0.0/0`; that permits connections
   from every public IP.
4. Choose **Connect → Drivers → Python** and copy the connection URI. Replace
   the password placeholder locally. Put it in `MONGODB_URI` in the root `.env`
   file; leave `MONGODB_URI` empty in `.env.example`.
5. Free cluster limits currently include 0.5 GB total storage (documents plus
   indexes), 100 read/write operations per second, and one free cluster per
   Atlas project. The smoke test creates then drops one temporary database.

## 8. GitHub and automated-workflow account

1. Create a **private** repository on GitHub (for example
   `cosmic-eye-telemetry-pipeline`). Do not upload `.env`, `secrets/`, service
   account keys, or generated data. Review `git status --short` before every
   commit.
2. This workspace currently has no Git repository metadata or remote. In
   PowerShell at the project root, run these commands, replacing the URL with
   the HTTPS URL shown on your repository page:

   ```powershell
   git init
   git branch -M main
   git status --short
   git add .
   git status --short
   git commit -m "Set up course pipeline environment"
   git remote add origin https://github.com/OWNER/REPOSITORY.git
   git push -u origin main
   ```

   Before committing, check that `.env`, `secrets/`, service account keys, and
   generated data do not appear in the staged file list. If one appears, stop
   and remove it from the Git index before committing. Use GitHub's browser
   sign-in flow if prompted; do not share a token here.
3. GitHub Free currently includes 2,000 Actions minutes/month for private
   repositories; standard hosted runners are free for public repositories.
   Scheduled workflows have a 5-minute minimum interval and can start late.
   Phase 4 will add the actual scheduled ETL job once its code and safe cloud
   authentication are ready; no empty/no-op schedule is created in this phase.
   To review included usage, open GitHub profile menu → **Settings → Billing
   and licensing → Usage**; enable usage notifications for Actions if shown.

## 9. Run the three smoke checks

1. Confirm the private local `.env` contains your project ID, local service
   account file path, and Atlas URI. Keep Docker Desktop open and start the
   broker using section 5.
2. From the repository root, run:

   ```powershell
   pipeline\.venv\Scripts\python.exe pipeline\smoke_test.py
   ```

3. The successful output is three lines ending in `PASS` (BigQuery, MongoDB
   Atlas, and Redpanda), then `Smoke tests: 3/3 PASS`. Each temporary resource
   is deleted by the script. If a row fails, copy only the error text (remove
   project IDs, usernames, hostnames, and any URI/key fragments) and share it;
   never share credential contents.
4. Stop the broker with `./scripts/stop-pipeline.ps1` when the local session is
   over. Paste the three output lines and the summary back here so I can verify
   the Phase 1 gate.

## Common errors

- **Docker daemon unavailable:** open Docker Desktop, wait for the engine, run
  `docker info`, and retry the start script.
- **Redpanda cannot start / not enough memory:** close apps, recheck free RAM,
  and confirm Docker Desktop is using WSL 2. Do not run Spark simultaneously
  with the broker on a low-memory session.
- **BigQuery permission denied:** confirm the active project ID and the two IAM
  roles above; do not enable billing to bypass the error.
- **BigQuery billing-required prompt:** stop. Verify that no billing account is
  attached and that you followed the Sandbox activation flow.
- **Atlas server selection timeout:** confirm Atlas cluster state, DB username
  and password in the URI, and that your current public IP is in the allow-list.
- **Kafka connection refused:** run `docker compose -f infra\compose.yaml ps`,
  then `docker compose -f infra\compose.yaml logs redpanda` and share only the
  non-secret error output.
