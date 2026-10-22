# Data mining pipeline with Airflow

The `shorts_user_logs` DAG runs these tasks in order:

```text
Daily schedule (00:00 UTC / 07:00 Vietnam)
    -> crawl_shorts (raw/crawl-short.py)
    -> synthesize_logs (raw/user-logs.py)
    -> data_lake/raw/ on your host
```

The crawler fetches YouTube metadata, not video files. The simulator reads the
entire deduplicated video catalog, then generates synthetic interactions for one
day starting at Airflow's `data_interval_start`. A scheduled run processes the
previous UTC day, after that interval ends. These are simulated logs, not real
YouTube user activity. Session events can spill into the following hour/day.

## Start Airflow (PowerShell)

Start Docker Desktop with Linux containers, then run from this repository:

```powershell
New-Item -ItemType Directory -Force data_lake | Out-Null
docker compose build airflow-init
docker compose up -d airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Wait for `airflow-init` to exit with code 0. It migrates the metadata database and
creates the administrator. Open http://localhost:8080 and log in with `admin` /
`admin`. Enable the `shorts_user_logs` DAG with its toggle, then click **Trigger
DAG** to run immediately. Enabling the DAG also allows its daily schedule.
`catchup=False` avoids replaying every missed date since the DAG's start date.

Only the Airflow services and their dedicated PostgreSQL 16 database are needed
for this workflow. `LocalExecutor` runs tasks in the scheduler container. The
image retains the repository's Airflow 2.10.0 version and installs NumPy, pandas
and yt-dlp at build time.

The new metadata database starts with an empty Airflow history. Existing Airflow
metadata in the warehouse PostgreSQL database is not migrated. Credentials in
Compose are local development defaults.

## Verify and monitor

```powershell
docker compose ps -a
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags trigger shorts_user_logs
docker compose logs -f airflow-scheduler
```

Offline tests (with pipeline dependencies installed) run with
`python -m unittest discover -s tests -v`. The two Airflow-specific tests run when
Airflow is installed, including inside the custom image; they are skipped in a
plain Python environment.

The DAG's Graph view shows the dependency and each task's Logs tab shows crawler
or simulator output. Both tasks retry twice, five minutes apart. Crawling has a
six-hour timeout; simulation has a two-hour timeout. Runs are serialized with
`max_active_runs=1` so two runs of this DAG cannot race on the shared catalog.

## Outputs and retry behavior

```text
data_lake/raw/
  youtube_shorts/<run-hash>.jsonl
  user_interaction_logs/run=<run-hash>/
    year=2026/month=10/day=21/hour=00/user_interactions_raw.jsonl
    ...
```

The run hash derives from Airflow's run ID. Crawl retries scan existing batches
and skip previously stored video IDs. Successful searches with no new videos are
valid: simulation can use the existing catalog. If all searches fail, the task
fails; if the catalog is empty, simulation fails with a clear error.

Simulation writes to a temporary directory and publishes the run directory only
after success. Retrying or clearing that task in the same DAG run reuses a
completed directory instead of appending duplicate logs. A **new** DAG run creates
a new synthetic batch, even if it covers the same date. Downstream ingestion must
choose the intended batches if reruns cover overlapping intervals. Hidden
`.staging-*` directories left by forcibly terminated tasks are incomplete outputs;
consumers should read only `run=*` directories.

The data lake is a bind mount, so outputs survive container replacement. Task
logs and Airflow metadata use named Docker volumes. This workflow writes local
files; it does not yet upload to MinIO, stream through Kafka, or load the warehouse.

## Change the workload

The DAG defaults to 10 search results per query across all 50 seed queries and
1,000 synthetic users. Set overrides in a repository `.env` file, for example:

```dotenv
CRAWL_MAX_RESULTS_PER_QUERY=5
SIMULATION_NUM_USERS=100
```

Then apply the environment changes:

```powershell
docker compose up -d airflow-webserver airflow-scheduler
```

User count changes the simulated user population; session volume is controlled
by the hourly Poisson rate in `user-logs.py`. Change the DAG's `schedule` in
`dags/shorts_user_logs.py` to change the cadence. Its simulation window is one day,
so adjust that too if choosing an hourly schedule. Python scripts and DAGs are
mounted from the host; dependency changes require rebuilding the image. YouTube
can throttle or block requests; check task logs if crawling fails.

## Run scripts without Airflow

Install the pipeline dependencies into your Python environment:

```powershell
python -m pip install -r requirements-airflow.txt
python raw/crawl-short.py --max-results-per-query 5
python raw/user-logs.py --start-date 2026-10-21T00:00:00+00:00 --days 1 --num-users 100
```

Direct script runs preserve the original append behavior; the DAG provides the
atomic publication and completed-run reuse described above. Without CLI options,
the simulator retains its original October 1, 2026 start date and three-day range.

## Other platform services

MinIO, Kafka, Debezium, the warehouse and Superset remain in Compose. Start these
separately if needed:

```powershell
docker compose up -d postgres minio zookeeper kafka debezium
```

For a fresh warehouse, create Superset's database once:

```powershell
docker compose exec postgres psql -U dataeng -d dwh -c "CREATE DATABASE superset;"
docker compose up -d superset
docker compose exec superset superset db upgrade
docker compose exec superset superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin
docker compose exec superset superset init
```

Superset is available at http://localhost:8088. Airflow no longer requires manually
creating an `airflow` database inside the warehouse service.

Reference: [Airflow 2.10.0 Docker setup and custom images](https://airflow.apache.org/docs/apache-airflow/2.10.0/howto/docker-compose/index.html).
