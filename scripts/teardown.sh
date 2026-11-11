#!/usr/bin/env bash
# Destructive cleanup helper. Default mode prints the plan and changes nothing.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_ID=""
EXECUTE=false
AFTER_VIVA=false

usage() {
  cat <<'EOF'
Usage: scripts/teardown.sh [--project PROJECT_ID] [--after-viva] [--execute]

Default is a dry run. No resources are changed unless BOTH --after-viva and
--execute are supplied and the operator types the exact confirmation phrase.
This helper stops local Compose services and deletes only BigQuery datasets
named raw and curated. Atlas and GCP project deletion remain manual steps.
EOF
}

while (($#)); do
  case "$1" in
    --project) PROJECT_ID="${2:?Missing project ID}"; shift 2 ;;
    --after-viva) AFTER_VIVA=true; shift ;;
    --execute) EXECUTE=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ "$EXECUTE" != true ]]; then
  echo "DRY RUN: would stop Compose services and remove their volumes."
  if [[ -n "$PROJECT_ID" ]]; then
    echo "DRY RUN: would delete BigQuery datasets ${PROJECT_ID}:raw and ${PROJECT_ID}:curated."
  else
    echo "DRY RUN: BigQuery datasets raw and curated would be deleted after a project ID is supplied."
  fi
  echo "Atlas cluster and GCP project are manual-only; see docs/TEARDOWN_CHECKLIST.md."
  exit 0
fi

if [[ "$AFTER_VIVA" != true ]]; then
  echo "Refusing: confirm the final demo/viva is finished with --after-viva." >&2
  exit 2
fi
if [[ -z "$PROJECT_ID" ]]; then
  echo "Refusing: supply the exact GCP project with --project PROJECT_ID." >&2
  exit 2
fi
if ! command -v docker >/dev/null 2>&1 || ! command -v bq >/dev/null 2>&1; then
  echo "Refusing: Docker and the Google BigQuery bq CLI must both be installed." >&2
  exit 2
fi

echo "This permanently deletes local Compose volumes and BigQuery datasets:"
echo "  ${PROJECT_ID}:raw"
echo "  ${PROJECT_ID}:curated"
echo "It does not delete MongoDB Atlas or the GCP project."
read -r -p 'Type DELETE COSMIC EYE RESOURCES to continue: ' confirmation
if [[ "$confirmation" != 'DELETE COSMIC EYE RESOURCES' ]]; then
  echo "Cancelled; no resources changed."
  exit 1
fi

docker compose -f "$ROOT/infra/compose.yaml" down --volumes --remove-orphans
bq rm -r -f "${PROJECT_ID}:raw"
bq rm -r -f "${PROJECT_ID}:curated"
echo "Local Compose services/volumes and the two named BigQuery datasets were removed."
echo "Complete the remaining manual steps in docs/TEARDOWN_CHECKLIST.md."
