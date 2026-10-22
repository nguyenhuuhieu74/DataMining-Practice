"""
ETL Pipeline Configuration
Bronze (local JSONL) -> Silver (MinIO Parquet) -> Gold (PostgreSQL)

Values are read from environment variables first (Docker / Airflow),
then fall back to local development defaults.
"""
import os

# -- MinIO / S3 --
MINIO = {
    "endpoint": os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
    "key": os.environ.get("MINIO_KEY", "minioadmin"),
    "secret": os.environ.get("MINIO_SECRET", "minioadmin123"),
    "use_ssl": False,
}

S3_OPTS = {
    "key": MINIO["key"],
    "secret": MINIO["secret"],
    "client_kwargs": {"endpoint_url": "http://" + MINIO["endpoint"]},
}

# Silver bucket paths
SILVER_BUCKET = "silver"
SILVER_VIDEOS_PATH = "s3://silver/youtube_shorts/videos.parquet"
SILVER_INTERACTIONS_PATH = "s3://silver/user_interactions/interactions.parquet"

# -- PostgreSQL DWH --
PG = {
    "host": os.environ.get("PG_HOST", "localhost"),
    "port": int(os.environ.get("PG_PORT", "5432")),
    "dbname": os.environ.get("PG_DB", "dwh"),
    "user": os.environ.get("PG_USER", "dataeng"),
    "password": os.environ.get("PG_PASSWORD", "dataeng123"),
}

PG_DSN = (
    "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**PG)
)

# -- Bronze source paths --
BRONZE_YOUTUBE_DIR = os.environ.get(
    "BRONZE_YOUTUBE_DIR",
    "./raw/data_lake/raw/youtube_shorts",
)
BRONZE_INTERACTIONS_DIR = os.environ.get(
    "BRONZE_INTERACTIONS_DIR",
    "./raw/data_lake/raw/user_interaction_logs",
)

# -- ETL tunables --
BATCH_SIZE_ROWS = 50_000    # rows per Polars collect() batch
COPY_BUFFER_MB = 16         # CSV buffer size for pg COPY
MAX_WATCH_SECONDS = 3_600   # outlier threshold (> 1 hour -> drop)

