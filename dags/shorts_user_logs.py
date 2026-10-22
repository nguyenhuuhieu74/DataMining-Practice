"""Daily metadata ingestion followed by synthetic interaction generation."""

from datetime import timedelta
import hashlib
import logging
import os
from pathlib import Path
import runpy
import tempfile

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(
    dag_id="shorts_user_logs",
    description="Crawl Shorts, then synthesize interactions for the scheduled UTC day",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["raw", "youtube", "synthetic"],
)
def shorts_user_logs():
    @task(execution_timeout=timedelta(hours=6))
    def crawl_shorts():
        # Load these hyphenated script names only during task execution.
        script = runpy.run_path("/opt/airflow/raw/crawl-short.py")
        context = get_current_context()
        batch = hashlib.sha256(context["run_id"].encode()).hexdigest()[:16]
        script["crawl_youtube_shorts"](
            topics_dict=script["SEARCH_TOPICS"],
            max_results_per_query=int(os.environ.get("CRAWL_MAX_RESULTS_PER_QUERY", "10")),
            output_dir="/opt/airflow/data_lake/raw/youtube_shorts",
            batch_name=batch,
        )

    @task(execution_timeout=timedelta(hours=2))
    def synthesize_logs():
        context = get_current_context()
        batch = hashlib.sha256(context["run_id"].encode()).hexdigest()[:16]
        root = Path("/opt/airflow/data_lake/raw/user_interaction_logs")
        root.mkdir(parents=True, exist_ok=True)
        destination = root / f"run={batch}"
        if destination.exists():
            logging.info("Reusing completed output: %s", destination)
            return str(destination)

        script = runpy.run_path("/opt/airflow/raw/user-logs.py")
        # Publish the whole run only after success. Failed attempts cannot leave
        # partial logs that the next attempt appends to.
        with tempfile.TemporaryDirectory(prefix=".staging-", dir=root) as staging:
            script["run_user_simulation_pipeline"](
                items_dir="/opt/airflow/data_lake/raw/youtube_shorts",
                output_base_dir=staging,
                start_date=context["data_interval_start"],
                simulation_days=1,
                num_users=int(os.environ.get("SIMULATION_NUM_USERS", "1000")),
            )
            Path(staging).rename(destination)
        return str(destination)

    crawl_shorts() >> synthesize_logs()


shorts_user_logs()
