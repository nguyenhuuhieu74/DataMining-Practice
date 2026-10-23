"""
Airflow DAG: etl_bronze_silver_gold
====================================
Orchestrates the full Bronze -> Silver -> Gold ETL pipeline.

Tasks
-----
1. bronze_to_silver_videos       : youtube_shorts JSONL -> Silver Parquet
2. bronze_to_silver_interactions : user_interaction_logs JSONL -> Silver Parquet
3. silver_to_gold                : Silver Parquet -> PostgreSQL DWH

Schedule: @daily (runs after shorts_user_logs produces new Bronze data)
"""

import logging
import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task

# Make the etl package importable inside the Airflow image
# (raw/ and etl/ are mounted at /opt/airflow/raw and /opt/airflow/etl)
_ETL_PATH = Path("/opt/airflow")
if str(_ETL_PATH) not in sys.path:
    sys.path.insert(0, str(_ETL_PATH))


@dag(
    dag_id="etl_bronze_silver_gold",
    description="Bronze (JSONL) -> Silver (Parquet/MinIO) -> Gold (PostgreSQL DWH)",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["etl", "silver", "gold", "warehouse"],
)
def etl_bronze_silver_gold():
    """Full Bronze -> Silver -> Gold pipeline."""

    @task(execution_timeout=timedelta(hours=1))
    def upload_raw_to_bronze_minio(**context):
        from etl.upload_to_bronze_minio import sync_local_raw_to_bronze

        youtube_dir = os.environ.get(
            "BRONZE_YOUTUBE_DIR",
            "/opt/airflow/data_lake/raw/youtube_shorts",
        )
        logs_dir = os.environ.get(
            "BRONZE_INTERACTIONS_DIR",
            "/opt/airflow/data_lake/raw/user_interaction_logs",
        )
        stats = sync_local_raw_to_bronze(youtube_dir=youtube_dir, logs_dir=logs_dir)
        logging.info("upload_raw_to_bronze_minio complete: %s", stats)
        return stats

    @task(execution_timeout=timedelta(hours=1))
    def bronze_to_silver_videos(**context):
        from etl.bronze_to_silver import etl_youtube_bronze_to_silver

        youtube_dir = os.environ.get(
            "BRONZE_YOUTUBE_DIR",
            "/opt/airflow/data_lake/raw/youtube_shorts",
        )
        count = etl_youtube_bronze_to_silver(youtube_dir=youtube_dir)
        logging.info("bronze_to_silver_videos: wrote %d rows to Silver", count)
        return count

    @task(execution_timeout=timedelta(hours=2))
    def bronze_to_silver_interactions(**context):
        from etl.bronze_to_silver import etl_interactions_bronze_to_silver

        logs_dir = os.environ.get(
            "BRONZE_INTERACTIONS_DIR",
            "/opt/airflow/data_lake/raw/user_interaction_logs",
        )
        count = etl_interactions_bronze_to_silver(logs_dir=logs_dir)
        logging.info(
            "bronze_to_silver_interactions: wrote %d rows to Silver", count
        )
        return count

    @task(execution_timeout=timedelta(hours=1))
    def silver_to_gold(videos_count: int, interactions_count: int, **context):
        from etl.silver_to_gold import load_silver_to_gold

        logging.info(
            "silver_to_gold starting with %d videos, %d interactions in Silver",
            videos_count,
            interactions_count,
        )
        counts = load_silver_to_gold()
        logging.info("silver_to_gold complete: %s", counts)
        return counts

    # Task dependencies:
    # 1. Sync raw files to MinIO Bronze bucket
    # 2. Parallel Silver transformations from MinIO Bronze
    # 3. Load Silver Parquet into Gold PostgreSQL DWH
    u = upload_raw_to_bronze_minio()
    v = bronze_to_silver_videos()
    i = bronze_to_silver_interactions()
    g = silver_to_gold(v, i)

    u >> [v, i]


etl_bronze_silver_gold()
