# Project teardown checklist

**Do not start teardown until the final demo and viva are finished.** This is
permanent cleanup. The scripts default to a dry run; do not add `--execute` or
`-Execute` before the viva is over.

## What will be lost

- BigQuery datasets `raw` and `curated`: generated/replayed events, rejected
  records, curated facts/dimensions, and their table metadata.
- Local Docker Compose volumes: Redpanda broker data and Spark checkpoints.
- MongoDB Atlas database/cluster: summaries, live metrics, and alert documents.
- GitHub Actions schedule: future hourly dbt runs stop when the workflow is
  disabled or the repository is archived/deleted.
- The GCP project, if separately deleted: all APIs, IAM settings, datasets,
  service accounts, and other resources in that project.

The scripts do **not** delete the BigQuery project, MongoDB Atlas cluster,
GitHub repository, downloaded Docker images, or local source code.

## Before cleanup

1. Confirm both team members have finished the demo/viva and saved any evidence
   they need (screenshots, Actions run links, query outputs, and report copy).
2. In MongoDB Atlas, verify the project and database are only for this course
   demo. Remove `0.0.0.0/0` from **Security → Network Access**; delete the Atlas
   database/cluster only if it contains no other work you need.
3. In GitHub **Actions → BigQuery raw-to-curated ETL**, disable the workflow or
   remove its schedule. The schedule is `17 * * * *` in `Asia/Kolkata` (hourly
   at minute 17, subject to GitHub scheduling delays), so it can keep querying
   BigQuery until disabled or the datasets/project are removed.
4. Keep the repository and reports if you need them for submission. The ignored
   local `actions-runner/` directory is not part of the repository; remove its
   local installation only after the final Actions/self-hosted-runner work is
   finished. Never commit its credentials.

## Local and BigQuery cleanup

PowerShell (Windows):

```powershell
./scripts/teardown.ps1
```

This prints the cleanup plan and changes nothing. After the final demo/viva,
review the project and type the required phrase when prompted:

```powershell
./scripts/teardown.ps1 -AfterViva -Execute
```

The script stops Compose containers, removes Compose volumes, then deletes only
the named `raw` and `curated` BigQuery datasets. It requires Docker, the `bq`
command-line tool, and credentials authorized to delete those datasets. If a
step fails, stop and inspect the output before retrying.

Git Bash / Linux equivalent:

```bash
bash scripts/teardown.sh --project cosmic-eye-telemetry-demo
```

That is also a dry run. After the final demo/viva, explicitly execute:

```bash
bash scripts/teardown.sh --project cosmic-eye-telemetry-demo --after-viva --execute
```

Both scripts ask for the exact phrase `DELETE COSMIC EYE RESOURCES` before
changing anything. They deliberately do not remove the GCP project or Atlas
cluster.

## Remaining cloud and credential cleanup (manual)

1. In BigQuery, confirm `raw` and `curated` are gone. If the entire dedicated
   `cosmic-eye-telemetry-demo` project is no longer needed, open Google Cloud
   Console → **IAM & Admin → Settings → Shut down** for that exact project and
   follow Google's confirmation steps. Project shutdown removes every resource
   in the project; do not do this if it contains anything else.
2. In Google Cloud Console → **IAM & Admin → Service Accounts**, delete the
   project service account and any downloaded service-account key. Also delete
   the local ignored `secrets/` key file securely. Remove the corresponding
   `GCP_SERVICE_ACCOUNT_KEY` GitHub Actions secret from repository settings.
3. In MongoDB Atlas, delete the course database and then the M0 cluster/project
   from **Database Deployments** / project settings. Delete the database user
   and revoke any remaining connection strings or credentials.
4. Remove any GitHub self-hosted runner registration in **Repository Settings →
   Actions → Runners** if one was registered. Then remove the local ignored
   `actions-runner/` folder; do not publish its credentials.
5. Confirm no recurring Actions run remains, and keep or delete the GitHub repo
   according to the course submission requirements.

Record the date and final verification in the project report. No cleanup has
been run by the project assistant; the owner must choose when the viva is over.
