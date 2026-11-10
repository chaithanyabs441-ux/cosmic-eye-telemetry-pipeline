# References

**Checked:** 2026-11-09. Service limits and terms can change; re-check the linked official pages in Phase 1 before creating accounts or resources.

## Course notice and repository material

- Jain University, *Big Data Analytics Project Notice*, Course 23BSDA5D54, issued 2026-10-17: [local course notice](Big_Data_Analytics_Project_Notice.docx).
- User-supplied phase and implementation instructions: [CODEX_PROMPT.md](CODEX_PROMPT.md).
- Existing application architecture and current behavior: [README.md](../README.md), [current handoff](../handoffs/handoff_updated.md), [data provenance](data_provenance.md), and [science and limitations](science_and_limitations.md).

## SPHEREx public data and archive metadata

- NASA/IPAC Infrared Science Archive (IRSA), [SPHEREx mission/archive page](https://irsa.ipac.caltech.edu/Missions/spherex.html) — public data releases and archive tools.
- NASA/IPAC IRSA, [programmatic Image Server / SIA interface](https://irsa.ipac.caltech.edu/ibe/) — programmatic searches of image metadata.
- NASA/IPAC IRSA, [SPHEREx archive data-access guide](https://caltech-ipac.github.io/spherex-archive-documentation/spherex-data-access/) — SIA/TAP access patterns and archive product metadata.
- NASA/IPAC IRSA, [SPHEREx cloud data access](https://irsa.ipac.caltech.edu/cloud_access/) — public AWS Open Data access and anonymous access guidance. The proposed pipeline uses metadata lookup only and does not copy FITS products.
- NASA/IPAC IRSA, [SPHEREx Explanatory Supplement and product documentation index](https://irsa.ipac.caltech.edu/data/SPHEREx/docs/) — product fields and interpretation.
- SIMBAD Astronomical Database, [SIMBAD](https://simbad.u-strasbg.fr/simbad/) — existing app name-resolution source.
- NASA/JPL, [Horizons System](https://ssd.jpl.nasa.gov/horizons/) and [Small-Body Database API](https://ssd-api.jpl.nasa.gov/doc/sbdb_query.html) — existing app's Solar System data sources.
- NASA, [Image and Video Library API](https://images.nasa.gov/docs/images.nasa.gov_api_docs.pdf) — existing app's optional selected-body image lookup.

## Proposed pipeline services and cost limits

- Docker, [Windows installation and WSL requirements](https://docs.docker.com/desktop/setup/install/windows-install/), [WSL 2 backend](https://docs.docker.com/desktop/features/wsl), and [Docker Desktop license](https://docs.docker.com/subscription-billing/desktop-license/) â€” current Windows requirements, local container setup, and education/personal-use terms.
- Eclipse Adoptium, [Temurin Windows installer](https://adoptium.net/installation/windows) and [Temurin releases](https://adoptium.net/temurin/releases/) â€” Java 17 JDK for Spark.
- Google Cloud, [BigQuery datasets](https://docs.cloud.google.com/bigquery/docs/datasets), [IAM access control](https://docs.cloud.google.com/bigquery/docs/access-control), and [load data quickstart](https://docs.cloud.google.com/bigquery/docs/quickstarts/load-data-console) â€” dataset-scoped access, job roles, and batch load verification.
- Redpanda, [Docker quickstart](https://docs.redpanda.com/streaming/current/get-started/quick-start/) â€” single-node local broker development settings and Docker lifecycle.
- MongoDB Atlas, [deploy an M0 Free cluster](https://www.mongodb.com/docs/atlas/tutorial/deploy-free-tier-cluster/) and [connect with a driver](https://www.mongodb.com/docs/atlas/connect-to-database-deployment/) â€” free cluster creation, database users, network access, and driver connection.

- Google Cloud, [BigQuery Sandbox](https://docs.cloud.google.com/bigquery/docs/sandbox) — current documented limits: 10 GiB lifetime storage (not restored after deletion), 1 TiB monthly query processing, automatic 60-day table/view/partition expiration, and no streaming data, DML, or BigQuery Data Transfer Service in Sandbox.
- Google Cloud, [load data into BigQuery](https://docs.cloud.google.com/bigquery/docs/loading-data) and [optimize load jobs](https://docs.cloud.google.com/bigquery/docs/optimize-load-jobs) — batch load jobs are the proposed ingestion path; batch objects will be held in memory rather than saved as local dataset files.
- Google Cloud, [BigQuery quotas and limits](https://docs.cloud.google.com/bigquery/quotas) — per-job and per-table constraints that must be rechecked when setting micro-batch frequency.
- Google Cloud Billing, [budgets and alerts](https://docs.cloud.google.com/billing/docs/how-to/budgets) — billing-account/project scope and alert behavior. Alerts-only budgets do not cap spending; a billing-disabled Sandbox project cannot supply a monetary budget alert.
- Google Cloud IAM, [Workload Identity Federation](https://docs.cloud.google.com/iam/docs/workload-identity-federation) and [deployment-pipeline setup, including GitHub Actions](https://docs.cloud.google.com/iam/docs/workload-identity-federation-with-deployment-pipelines) — proposed keyless authentication option; verify any project/billing prerequisites before using it.
- MongoDB Atlas, [Free Cluster Limits](https://www.mongodb.com/docs/atlas/reference/free-shared-limitations/) — current documented limits include 0.5 GB storage, 100 read/write operations per second, one free cluster per project, no configurable backups, and no automatic storage expansion.
- GitHub, [included plan usage](https://docs.github.com/en/billing/reference/product-usage-included) and [Actions billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions) — GitHub Free includes 2,000 standard-runner minutes/month for private repos; public standard-runner use is free. Private overage may be charged if billing is enabled.
- GitHub, [workflow schedule syntax](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax) and [scheduled workflow reliability notes](https://docs.github.com/en/enterprise-cloud%40latest/actions/monitoring-and-troubleshooting-workflows/troubleshooting-workflows) — shortest supported interval is 5 minutes; scheduled jobs can be delayed under load.
- GitHub, [using Actions secrets](https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/use-secrets) — repository-secret setup and workflow access.
- Redpanda, [licensing overview](https://docs.redpanda.com/streaming/current/get-started/licensing/overview/) — Community Edition is free/source-available under the Business Source License, with restrictions; do not enable Enterprise-only features for this project.
- Redpanda, [Docker quickstart](https://docs.redpanda.com/streaming/current/get-started/quick-start/) — development quickstart prerequisite of at least 4 GiB free memory before starting containers.
- Apache Spark, [Structured Streaming programming guide](https://spark.apache.org/docs/latest/streaming/index.html), [Kafka integration guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html), and [current platform overview](https://spark.apache.org/docs/latest/) — proposed local stream processor; current docs list Windows support and Java 17/21/25.
- Confluent, [Cloud trial](https://docs.confluent.io/cloud/current/get-started/free-trial.html) and [billing FAQ](https://docs.confluent.io/cloud/current/billing/billing-faq.html) — $400 credit lasting up to 30 days, payment method required to provision resources, and no hard dollar spending cap; not selected for a free-only project.
- dbt Labs, [BigQuery adapter setup](https://docs.getdbt.com/docs/local/connect-data-platform/bigquery-setup) and [supported data platforms](https://docs.getdbt.com/docs/supported-data-platforms) — dbt SQL workflow and BigQuery adapter reference.
- dbt Labs, [BigQuery adapter configuration](https://docs.getdbt.com/reference/resource-configs/bigquery-configs) — service-account profiles, table materializations, partitioning, and clustering configuration.
- Google Cloud, [JSON functions](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/json_functions) and [conversion functions](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions) — JSON field extraction and safe type conversion in ETL SQL.
- Streamlit, [documentation](https://docs.streamlit.io/) — proposed local dashboard framework.
- Docker, [Compose documentation](https://docs.docker.com/compose/) — proposed local broker lifecycle and cleanup.

## Pipeline libraries proposed

- Google Cloud BigQuery, [partitioned tables](https://docs.cloud.google.com/bigquery/docs/creating-partitioned-tables), [clustered tables](https://docs.cloud.google.com/bigquery/docs/creating-clustered-tables), [managing clustered tables](https://docs.cloud.google.com/bigquery/docs/manage-clustered-tables), [`INFORMATION_SCHEMA.PARTITIONS`](https://docs.cloud.google.com/bigquery/docs/information-schema-partitions), and [`INFORMATION_SCHEMA.TABLE_STORAGE`](https://docs.cloud.google.com/bigquery/docs/information-schema-table-storage) â€” warehouse layout and measured metadata queries.

- Python, [Python 3 documentation](https://docs.python.org/3/).
- Pydantic, [documentation](https://docs.pydantic.dev/latest/) — event schema validation.
- Confluent, [Python Kafka client](https://docs.confluent.io/kafka-clients/python/current/overview.html) — Kafka API client for Redpanda production/consumption; using the client does not require Confluent Cloud.
- Google Cloud, [BigQuery Python client reference](https://cloud.google.com/python/docs/reference/bigquery/latest) — batch load and query jobs.
- MongoDB, [PyMongo driver documentation](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/) — BigQuery result retrieval, indexes, and dashboard reads.
- dbt Labs, [dbt-bigquery adapter](https://pypi.org/project/dbt-bigquery/) — installation/version source for the adapter package.

## Existing application libraries (cited as project background)

### Frontend

- React, [documentation](https://react.dev/); TypeScript, [documentation](https://www.typescriptlang.org/docs/); Vite, [documentation](https://vite.dev/guide/); React Router, [documentation](https://reactrouter.com/); Three.js, [documentation](https://threejs.org/docs/); Lucide React, [documentation](https://lucide.dev/guide/packages/lucide-react); `react-zoom-pan-pinch`, [project repository](https://github.com/BetterTyped/react-zoom-pan-pinch).

### Backend and scientific/data stack

- FastAPI, [documentation](https://fastapi.tiangolo.com/); Uvicorn, [documentation](https://www.uvicorn.org/); Pydantic, [documentation](https://docs.pydantic.dev/latest/); pydantic-settings, [settings documentation](https://docs.pydantic.dev/latest/concepts/pydantic_settings/).
- HTTPX, [documentation](https://www.python-httpx.org/); Requests, [documentation](https://requests.readthedocs.io/); python-dotenv, [project page](https://github.com/theskumar/python-dotenv).
- Astroquery, [documentation](https://astroquery.readthedocs.io/en/latest/); Astropy, [documentation](https://docs.astropy.org/en/stable/); PyVO, [documentation](https://pyvo.readthedocs.io/en/stable/) — archive/data-access support.
- NumPy, [documentation](https://numpy.org/doc/stable/); SciPy, [documentation](https://docs.scipy.org/doc/scipy/); scikit-image, [documentation](https://scikit-image.org/docs/stable/); Matplotlib, [documentation](https://matplotlib.org/stable/); Pillow, [documentation](https://pillow.readthedocs.io/en/stable/) — numerical and image rendering dependencies in the existing app.
- scikit-learn, [documentation](https://scikit-learn.org/stable/); XGBoost, [documentation](https://xgboost.readthedocs.io/en/stable/) — listed dependencies in the existing app; current application docs state no trained model/inference path is active.
- pytest, [documentation](https://docs.pytest.org/en/stable/); pytest-asyncio, [documentation](https://pytest-asyncio.readthedocs.io/en/latest/); Ruff, [documentation](https://docs.astral.sh/ruff/) — existing quality tooling.

