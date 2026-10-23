"""
Silver -> Gold ETL
==================
Reads cleaned Parquet from MinIO Silver, applies dimensional modelling
transforms, and loads into PostgreSQL Data Warehouse using streaming
COPY for efficiency.

Gold schema
-----------
dim_video              - video catalog dimension
dim_user               - synthetic user dimension (derived from events)
fact_interactions      - cleaned event fact table
agg_video_daily        - daily aggregation per video

Run standalone
--------------
    python etl/silver_to_gold.py

Or import load_silver_to_gold() for the Airflow DAG.
"""

import io
import logging
import sys
from pathlib import Path

import polars as pl
import psycopg2
import psycopg2.extras
import s3fs

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.config import PG, S3_OPTS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("silver_to_gold")


# ---------------------------------------------------------------------------
# DDL: create Gold tables if they do not exist
# ---------------------------------------------------------------------------

DDL_DIM_VIDEO = """
CREATE TABLE IF NOT EXISTS dim_video (
    video_id              TEXT        PRIMARY KEY,
    video_url             TEXT,
    title                 TEXT,
    description           TEXT,
    channel_id            TEXT,
    channel_title         TEXT,
    duration_seconds      FLOAT,
    categories            TEXT,
    tags                  TEXT,
    view_count_at_crawl   BIGINT,
    like_count_at_crawl   BIGINT,
    comment_count_at_crawl BIGINT,
    upload_date           TEXT,
    thumbnail_url         TEXT,
    crawled_at            TEXT,
    etl_loaded_at         TIMESTAMPTZ DEFAULT NOW()
);
"""

DDL_DIM_USER = """
CREATE TABLE IF NOT EXISTS dim_user (
    user_id               TEXT        PRIMARY KEY,
    first_seen_date       DATE,
    device_os_mode        TEXT,
    etl_loaded_at         TIMESTAMPTZ DEFAULT NOW()
);
"""

DDL_FACT_INTERACTIONS = """
CREATE TABLE IF NOT EXISTS fact_interactions (
    event_id                    TEXT        PRIMARY KEY,
    session_id                  TEXT,
    user_id                     TEXT,
    video_id                    TEXT,
    event_type                  TEXT,
    impression_index            INT,
    watch_duration_seconds      FLOAT,
    completion_rate             FLOAT,
    comment_read_duration_seconds FLOAT,
    click_profile               SMALLINT,
    action_like                 SMALLINT,
    action_comment              SMALLINT,
    action_share                SMALLINT,
    action_skip                 SMALLINT,
    action_block_creator        SMALLINT,
    device_os                   TEXT,
    app_version                 TEXT,
    timestamp_ms                BIGINT,
    server_received_timestamp_ms BIGINT,
    event_date                  DATE,
    event_hour                  SMALLINT,
    etl_loaded_at               TIMESTAMPTZ DEFAULT NOW()
);
"""

DDL_AGG_VIDEO_DAILY = """
CREATE TABLE IF NOT EXISTS agg_video_daily (
    agg_date              DATE,
    video_id              TEXT,
    total_views           BIGINT,
    unique_users          BIGINT,
    total_watch_seconds   FLOAT,
    avg_completion_rate   FLOAT,
    total_likes           BIGINT,
    total_shares          BIGINT,
    total_skips           BIGINT,
    total_comments        BIGINT,
    PRIMARY KEY (agg_date, video_id)
);
"""

ALL_DDL = [DDL_DIM_VIDEO, DDL_DIM_USER, DDL_FACT_INTERACTIONS, DDL_AGG_VIDEO_DAILY]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=S3_OPTS["key"],
        secret=S3_OPTS["secret"],
        client_kwargs=S3_OPTS["client_kwargs"],
    )


def _get_pg_conn():
    return psycopg2.connect(**PG)


def _create_tables(conn) -> None:
    with conn.cursor() as cur:
        for ddl in ALL_DDL:
            cur.execute(ddl)
    conn.commit()
    log.info("Gold tables created / verified")


def _streaming_copy(conn, df: pl.DataFrame, table: str, columns: list) -> int:
    """
    Stream a Polars DataFrame into PostgreSQL using COPY FROM STDIN.
    Returns rows copied.
    """
    buf = io.StringIO()
    # Write CSV without header into buffer
    df.select(columns).write_csv(buf, include_header=False)
    buf.seek(0)

    col_list = ", ".join(columns)
    copy_sql = f"COPY {table} ({col_list}) FROM STDIN WITH (FORMAT csv, NULL '')"

    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buf)
    conn.commit()

    rows = len(df)
    log.info("  COPY %d rows -> %s", rows, table)
    return rows


def _upsert_copy(conn, df: pl.DataFrame, table: str, columns: list, pk: str) -> int:
    """
    COPY into a temp table then INSERT ... ON CONFLICT DO UPDATE (upsert).
    Ensures idempotency on DAG re-runs.
    """
    tmp = f"tmp_{table}"
    col_list = ", ".join(columns)
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c != pk
    )

    with conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE {tmp} (LIKE {table} INCLUDING ALL) ON COMMIT DROP;")
        buf = io.StringIO()
        df.select(columns).write_csv(buf, include_header=False)
        buf.seek(0)
        cur.copy_expert(
            f"COPY {tmp} ({col_list}) FROM STDIN WITH (FORMAT csv, NULL '')",
            buf,
        )
        cur.execute(f"""
            INSERT INTO {table} ({col_list})
            SELECT {col_list} FROM {tmp}
            ON CONFLICT ({pk}) DO UPDATE SET {update_set};
        """)
    conn.commit()
    rows = len(df)
    log.info("  UPSERT %d rows -> %s", rows, table)
    return rows


# ---------------------------------------------------------------------------
# 1. dim_video
# ---------------------------------------------------------------------------

DIM_VIDEO_COLS = [
    "video_id", "video_url", "title", "description",
    "channel_id", "channel_title", "duration_seconds", "categories", "tags",
    "view_count_at_crawl", "like_count_at_crawl", "comment_count_at_crawl",
    "upload_date", "thumbnail_url", "crawled_at",
]


def load_dim_video(conn, fs: s3fs.S3FileSystem) -> int:
    log.info("Loading dim_video ...")
    with fs.open("s3://silver/youtube_shorts/videos.parquet", "rb") as f:
        df = pl.read_parquet(f)

    # Ensure all expected columns exist
    for col in DIM_VIDEO_COLS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    df = df.select(DIM_VIDEO_COLS).with_columns([
        pl.col("view_count_at_crawl").cast(pl.Int64).fill_null(0),
        pl.col("like_count_at_crawl").cast(pl.Int64).fill_null(0),
        pl.col("comment_count_at_crawl").cast(pl.Int64).fill_null(0),
        pl.col("duration_seconds").cast(pl.Float64).fill_null(0.0),
    ])

    return _upsert_copy(conn, df, "dim_video", DIM_VIDEO_COLS, "video_id")


# ---------------------------------------------------------------------------
# 2. dim_user (derived from interactions)
# ---------------------------------------------------------------------------

DIM_USER_COLS = ["user_id", "first_seen_date", "device_os_mode"]


def load_dim_user(conn, fs: s3fs.S3FileSystem) -> int:
    log.info("Loading dim_user ...")
    with fs.open("s3://silver/user_interactions/interactions.parquet", "rb") as f:
        df = pl.read_parquet(f)

    # Only real users (not nulls / bots - already filtered in Silver)
    df = df.filter(pl.col("user_id").is_not_null())

    dim_user = (
        df.group_by("user_id")
        .agg([
            pl.col("event_date").min().alias("first_seen_date"),
            pl.col("device_os").mode().first().alias("device_os_mode"),
        ])
    )

    return _upsert_copy(conn, dim_user, "dim_user", DIM_USER_COLS, "user_id")


# ---------------------------------------------------------------------------
# 3. fact_interactions
# ---------------------------------------------------------------------------

FACT_COLS = [
    "event_id", "session_id", "user_id", "video_id", "event_type",
    "impression_index", "watch_duration_seconds", "completion_rate",
    "comment_read_duration_seconds",
    "click_profile", "action_like", "action_comment", "action_share",
    "action_skip", "action_block_creator",
    "device_os", "app_version",
    "timestamp_ms", "server_received_timestamp_ms",
    "event_date", "event_hour",
]


def load_fact_interactions(conn, fs: s3fs.S3FileSystem, batch_rows: int = 50_000) -> int:
    """
    Stream Silver interactions into fact_interactions in batches.
    Uses COPY for high-throughput loading.
    """
    log.info("Loading fact_interactions (streaming COPY) ...")
    with fs.open("s3://silver/user_interactions/interactions.parquet", "rb") as f:
        df = pl.read_parquet(f)

    # Rename timestamp columns to avoid PostgreSQL reserved word clash
    df = df.rename({
        "timestamp": "timestamp_ms",
        "server_received_timestamp": "server_received_timestamp_ms",
    })

    # Ensure all required columns
    for col in FACT_COLS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    df = df.select(FACT_COLS).with_columns([
        pl.col("watch_duration_seconds").cast(pl.Float64),
        pl.col("completion_rate").cast(pl.Float64),
        pl.col("comment_read_duration_seconds").cast(pl.Float64).fill_null(0.0),
        pl.col("impression_index").cast(pl.Int32).fill_null(0),
        pl.col("click_profile").cast(pl.Int16).fill_null(0),
        pl.col("action_like").cast(pl.Int16).fill_null(0),
        pl.col("action_comment").cast(pl.Int16).fill_null(0),
        pl.col("action_share").cast(pl.Int16).fill_null(0),
        pl.col("action_skip").cast(pl.Int16).fill_null(0),
        pl.col("action_block_creator").cast(pl.Int16).fill_null(0),
        pl.col("event_hour").cast(pl.Int16),
    ])

    # Truncate and reload for full refresh (idempotent on re-runs)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE fact_interactions;")
    conn.commit()

    total = 0
    n = len(df)
    for start in range(0, n, batch_rows):
        chunk = df.slice(start, batch_rows)
        total += _streaming_copy(conn, chunk, "fact_interactions", FACT_COLS)

    return total


# ---------------------------------------------------------------------------
# 4. agg_video_daily
# ---------------------------------------------------------------------------

AGG_COLS = [
    "agg_date", "video_id", "total_views", "unique_users",
    "total_watch_seconds", "avg_completion_rate",
    "total_likes", "total_shares", "total_skips", "total_comments",
]


def load_agg_video_daily(conn, fs: s3fs.S3FileSystem) -> int:
    log.info("Loading agg_video_daily ...")
    with fs.open("s3://silver/user_interactions/interactions.parquet", "rb") as f:
        df = pl.read_parquet(f)

    agg = (
        df.group_by(["event_date", "video_id"])
        .agg([
            pl.len().alias("total_views"),
            pl.col("user_id").n_unique().alias("unique_users"),
            pl.col("watch_duration_seconds").sum().alias("total_watch_seconds"),
            pl.col("completion_rate").mean().alias("avg_completion_rate"),
            pl.col("action_like").sum().alias("total_likes"),
            pl.col("action_share").sum().alias("total_shares"),
            pl.col("action_skip").sum().alias("total_skips"),
            pl.col("action_comment").sum().alias("total_comments"),
        ])
        .rename({"event_date": "agg_date"})
        .select(AGG_COLS)
    )

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE agg_video_daily;")
    conn.commit()

    return _upsert_copy(conn, agg, "agg_video_daily", AGG_COLS, "agg_date, video_id")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def load_silver_to_gold() -> dict:
    """Run all Silver -> Gold loads. Returns row counts per table."""
    fs = _get_fs()
    conn = _get_pg_conn()

    try:
        _create_tables(conn)

        counts = {
            "dim_video":        load_dim_video(conn, fs),
            "dim_user":         load_dim_user(conn, fs),
            "fact_interactions": load_fact_interactions(conn, fs),
            "agg_video_daily":  load_agg_video_daily(conn, fs),
        }
        log.info("Silver -> Gold complete: %s", counts)
        return counts

    finally:
        conn.close()


if __name__ == "__main__":
    result = load_silver_to_gold()
    print(f"\nDone. {result}")
