# Cosmic Eye — Current Project Handoff

Updated 2026-11-13. This file describes the current application and local setup. Source code is authoritative if implementation changes later. Older staged handoffs and screenshots may describe earlier versions.

## Project summary

Cosmic Eye has two connected but separately launched parts:

1. The main astronomy app: React/Three.js frontend, FastAPI backend, and SPHEREx QR3 access through NASA/IPAC IRSA.
2. The big-data project: Python ingestion and telemetry, Redpanda, BigQuery/dbt, Spark/MongoDB, and a multipage Streamlit dashboard.

The app is an independent prototype for the 2026 NASA Space Apps Challenge, not an official NASA product. It is not a discovery or planetary-detection system.

## Current user interface

| Route | Current behavior |
| --- | --- |
| `/` | Home page and featured targets. Some Home thumbnails are decorative external images. |
| `/explore` | Unified Search page. Search by object name or by RA/Dec. Name resolution uses local catalog entries first, then SIMBAD. |
| `/ai-search` | Compatibility redirect to `/explore`; the former separate AI Search page has been removed. |
| `/skymap` | Immersive full-window sky view. App header/sidebar are hidden while on this route; use **Exit sky map** to return. Supports drag-to-rotate, scroll-to-zoom, coordinate grid, constellation guides, bright-star labels, full screen, and a hideable SPHEREx target panel. Selecting a target checks IRSA QR3 and can open a real archive tile. |
| `/browse` | Recent archive results from a fixed sample field; it is not a mission-wide latest-data feed. |
| `/popular-regions` | Curated region list with archive searches and image previews. |
| `/rgb` | Color Lab: wavelength groups, individual bands, palettes, and scan selection. |
| `/learn` | Infrared explainer and illustrative signal graphic. |
| `/wavelengths` | Redirects to `/rgb`. |

The Sky Map plots 4,992 bundled Hipparcos star positions and 117 built-in SPHEREx target records. Twenty bright stars have name labels; five familiar constellation outlines are included. Target markers use catalog RA/Dec values, in ICRS/J2000 coordinates. The Milky Way guide band is illustrative, not an image or measurement. Symbols identify target categories; galaxy symbols do not show measured morphology. Users can add a name only when the resolver finds it and the QR3 archive query returns coverage.

The old Solar System visualizer source and `/observations/solar-system-model` API remain in the repository, but the current frontend does not link to or render a Solar System tab. Do not describe it as an active Sky Map feature.

## Data, AI, and interpretation

- Active archive searches use IRSA collection `spherex_qr3`. QR2 measurements and screenshots in earlier materials are historical; QR2 is not the active collection.
- Object-name resolution is a local catalog/alias lookup followed by SIMBAD for other names. There is no active generative AI model or trained image-classification model.
- Band names and classifications are deterministic wavelength-range labels, not image-content predictions.
- Observation cards display archive field centers. An image footprint can include the searched target even if its center coordinate differs.
- Preview images are false-color renderings from FITS data. A first preview can take time because the backend downloads and processes the FITS product; generated JPEGs are cached under `backend/tmp/previews/`.
- A tile is a wide infrared sky field and is not necessarily a resolved image of the named object. An empty result means no tile was returned for the current query, not that the object is absent.
- BigQuery warehouse observation metadata comes from the archive. Pipeline quality values, request activity, latency, and HTTP errors are synthetic course-demo data, not spacecraft telemetry or personal user analytics.

## Services and ports

| Service | Local address | Purpose |
| --- | --- | --- |
| React/Vite | `http://localhost:5173` | Main frontend. Proxies `/api` to FastAPI in development. |
| FastAPI | `http://localhost:8000` | Name resolution, archive search, FITS previews, catalog, and Color Lab APIs. |
| Streamlit | `http://localhost:8501` | Cosmic Eye Command Center, interactive Data Flow page, and BigQuery Warehouse Insights page. |
| Redpanda | `localhost:9092` | Local Kafka-compatible pipeline transport. |
| MongoDB | Configured by `MONGODB_URI` | Live dashboard metrics/alerts and pipeline summaries. It may be local or Atlas based on configuration. |
| BigQuery | Google Cloud | Raw and curated pipeline warehouse; not a local container. |

## Local start instructions (Windows PowerShell)

Keep credentials in the ignored repository-root `.env`; never commit it. Copy `.env.example` as a starting point, then set actual local values. The GCP project shown in `.env.example` is only a suggestion, not an accessible project guarantee.

### Main web app

Backend, from a terminal at the repository root:

```powershell
py -3.11 -m venv backend\.venv
backend\.venv\Scripts\python.exe -m pip install -r backend\requirements.txt
cd backend
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload --port 8000
```

Frontend, from a second terminal:

```powershell
cd frontend
npm ci
npm run dev
```

Open `http://localhost:5173`. Health: `http://localhost:8000/api/v1/health`; API docs: `http://localhost:8000/docs`.

### Docker and live dashboard

Compose needs the root `.env` loaded explicitly when run from PowerShell:

```powershell
docker compose --env-file .env -f infra/compose.yaml up -d
```

This starts the Redpanda service. The Spark container is behind the `streaming` profile and needs the pipeline Python environment and Java 17. See [`docs/PIPELINE_README.md`](../docs/PIPELINE_README.md) and [`docs/PHASE1_SETUP.md`](../docs/PHASE1_SETUP.md) for batch ingestion and streaming demo steps.

Install pipeline/dashboard dependencies and launch Streamlit from the repository root:

```powershell
py -3.11 -m venv pipeline\.venv
pipeline\.venv\Scripts\python.exe -m pip install -r pipeline\requirements.txt
pipeline\.venv\Scripts\python.exe -m streamlit run dashboard\app.py --server.port 8501
```

The Streamlit main page is the **Cosmic Eye Command Center**. It checks Redpanda broker reachability, recent Spark output, and MongoDB connectivity, then shows stored stream metrics and charts. Use **Data Flow** to switch between an interactive live-stream or batch-warehouse diagram, click stages for their inputs/processing/outputs, and inspect captured-field catalogs. **Warehouse Insights** queries BigQuery separately. The live demo stream is synthetic; its Spark schema intentionally retains only fields needed for aggregates and alerts.

### BigQuery Warehouse Insights

Set `GCP_PROJECT_ID` to the real Google Cloud project containing the `curated` dataset, and set `BIGQUERY_LOCATION` to the dataset's location. Authenticate with Application Default Credentials (`gcloud auth application-default login`) or set `GOOGLE_APPLICATION_CREDENTIALS` to a local service-account JSON path. The active identity needs BigQuery Job User and read access to `curated`. Restart Streamlit after changing `.env` or credentials.

Current workstation note: `docs/PROPOSAL.md` and the warehouse SQL identify `cosmic-eye-telemetry-demo` as the project, so the local ignored `.env` now uses that ID. The active Google identity receives `403 Forbidden` when checking its `curated` dataset; Warehouse Insights cannot run queries until the owner signs in with an identity that has BigQuery Job User and dataset read access. No service-account key exists at the documented local path. Do not paste keys into chat.

The GitHub Actions workflow `.github/workflows/warehouse-etl.yml` runs `dbt build` hourly at 17 minutes past the hour (Asia/Kolkata) and on relevant pushes to `main`. It rebuilds/tests curated tables from data already in `raw`; it does not fetch new IRSA records. The repository must have an Actions secret named `GCP_SERVICE_ACCOUNT_KEY` containing the service-account JSON. That service account needs BigQuery Job User on the project, read access to `raw`, and write access to `curated`. The workflow is in the default branch only after the commit is pushed; verify successful scheduled runs in GitHub Actions.

## Data pipeline overview

The two paths are separate. Batch ingestion can use public IRSA SPHEREx metadata plus explicitly synthetic quality and usage data; it passes through Python/Redpanda to BigQuery raw, dbt-cleaned BigQuery curated tables, Warehouse Insights, and an optional MongoDB daily-summary sync. The live demo producer emits synthetic messages through Redpanda; three Spark queries write compact metric windows and alerts to MongoDB for the Command Center. The Data Flow page documents these steps, schemas, retention, and provenance.

See [`docs/PIPELINE_README.md`](../docs/PIPELINE_README.md) for pipeline commands and architecture, [`docs/FINAL_REPORT.md`](../docs/FINAL_REPORT.md) for measured runs, and [`docs/REQUIREMENTS_CHECKLIST.md`](../docs/REQUIREMENTS_CHECKLIST.md) for requirement evidence. Historical counts in reports describe their run dates and should not be represented as current counts.

## API routes

All active API routes are under `/api/v1` in `backend/app/api/observations.py`:

- `GET /health`
- `GET /observations?ra=...&dec=...` — defaults to QR3; supports radius and paging.
- `GET /observations/ai-search?q=...` — legacy endpoint name used for local/SIMBAD coordinate resolution.
- `GET /observations/featured` — curated sky targets.
- `GET /observations/wavelengths?ra=...&dec=...` — Color Lab band groups.
- `GET /observations/preview?url=...&palette=...` — cached/generated single-band JPEG.
- `GET /observations/preview/rgb?url1=...&url2=...&url3=...` — experimental RGB composite.
- `GET /observations/random` — random sky coordinate.
- `GET /observations/solar-system-model` — retained legacy endpoint; not used by current frontend.

Use `/docs` for full parameters and schemas.

## Key source locations

```text
backend/app/adapters/irsa_spherex.py         QR3 archive adapter
backend/app/services/ai_service.py            Local aliases and SIMBAD resolution
backend/app/services/archive_service.py       Archive result normalization
backend/app/services/preview_service.py       FITS-to-JPEG preview and cache
backend/app/catalogs/messier_targets.json     Bundled Messier target catalog
frontend/src/routes/ExplorePage.tsx           Unified name/coordinate Search
frontend/src/routes/SkyMapPage.tsx            Full-window Sky Map interface
frontend/src/components/SkyMapGlobe.tsx       Three.js stars, lines, and target markers
frontend/src/catalogs/hipparcos_bright_stars.json  4,992 Hipparcos coordinates
frontend/src/styles/global.css                Application and planetarium layout
dashboard/app.py                              Streamlit Command Center
dashboard/pages/1_Warehouse_Insights.py       BigQuery insights page
dashboard/pages/2_Data_Flow.py                 Interactive flowchart and field catalog
pipeline/                                      Ingestion, ETL, and stream processing
infra/compose.yaml                             Redpanda and optional Spark services
```

## Known operational notes

- The first preview of a FITS product may be slow; the backend caches generated previews locally.
- BigQuery needs a valid project, ADC/service-account credentials, dataset location, and permissions. A successful Streamlit health page does not prove BigQuery access.
- `infra/compose.yaml` requires the root environment file; use `--env-file .env`.
- The frontend production build has a known Three.js vendor-chunk size warning. The build succeeds.
- Earlier `stage*_handoff.md` files are historical project notes. Use this file, README, and current source for the live UI behavior.

## License

Source code is MIT-licensed. IRSA products, SIMBAD records, star catalogs, and third-party imagery retain their own terms and attribution requirements.

