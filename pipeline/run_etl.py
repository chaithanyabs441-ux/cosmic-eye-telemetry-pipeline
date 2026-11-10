"""Run dbt transformations and data-quality checks against BigQuery."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import argparse
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "pipeline" / "dbt"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parse-only", action="store_true", help="validate dbt project configuration without querying BigQuery")
    args = parser.parse_args()
    load_dotenv(ROOT / ".env")
    required = ("GCP_PROJECT_ID", "GOOGLE_APPLICATION_CREDENTIALS")
    missing = [key for key in required if not os.getenv(key, "").strip()]
    if missing:
        print("ETL configuration is incomplete. Check the named settings in .env: " + ", ".join(missing))
        return 2
    if not Path(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]).is_file():
        print("The service-account credential file configured in .env was not found. Check its local path.")
        return 2

    windows_dbt = Path(sys.executable).with_name("dbt.exe")
    dbt = str(windows_dbt) if windows_dbt.is_file() else shutil.which("dbt")
    if dbt:
        command = [dbt]
    else:
        command = [sys.executable, "-m", "dbt.cli.main"]
    command += ["parse" if args.parse_only else "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if args.parse_only:
        print("Checking dbt project configuration only; no BigQuery query or table changes will run.")
    else:
        print("Starting scheduled dbt build: curated tables followed by schema/data-quality tests.")
    completed = subprocess.run(command, cwd=ROOT, check=False)
    if completed.returncode == 0:
        print("ETL project parse: PASS." if args.parse_only else "ETL result: PASS (dbt build and tests succeeded).")
    else:
        print(f"ETL result: FAIL (dbt exited with code {completed.returncode}).")
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
