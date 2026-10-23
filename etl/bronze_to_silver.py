"""
Bronze -> Silver ETL
====================
Reads JSONL files from the local Bronze data lake using Polars
in streaming/batched mode, applies data-quality rules, and writes
cleaned Parquet files to the MinIO Silver bucket.

Two source tables
-----------------
1. youtube_shorts   (video catalog, crawled JSONL)
2. user_interactions (synthetic events, partitioned JSONL)

Run standalone
--------------
    python etl/bronze_to_silver.py [--youtube-dir ...] [--logs-dir ...]

Or import functions for the Airflow DAG.
"""

import argparse
import glob
import io
import json
import logging
import os
import sys
from pathlib import Path
from typing import Iterator, List, Optional

import polars as pl
import s3fs

# Allow running from project root or as a module
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.config import (
    BATCH_SIZE_ROWS,
    BRONZE_BUCKET,
    BRONZE_INTERACTIONS_DIR,
    BRONZE_INTERACTIONS_S3_PREFIX,
    BRONZE_VIDEOS_S3_PREFIX,
    BRONZE_YOUTUBE_DIR,
    MAX_WATCH_SECONDS,
    S3_OPTS,
    SILVER_BUCKET,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("bronze_to_silver")


# ---------------------------------------------------------------------------
# Helper: ensure Silver bucket exists
# ---------------------------------------------------------------------------

def _get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=S3_OPTS["key"],
        secret=S3_OPTS["secret"],
        client_kwargs=S3_OPTS["client_kwargs"],
    )


def ensure_silver_bucket(fs: s3fs.S3FileSystem) -> None:
    if not fs.exists(SILVER_BUCKET):
        fs.mkdir(SILVER_BUCKET)
        log.info("Created Silver bucket: %s", SILVER_BUCKET)


# ---------------------------------------------------------------------------
# Helper: read JSONL files in batches (generator)
# ---------------------------------------------------------------------------

def _jsonl_batch_reader(
    file_paths: List[str],
    batch_size: int = BATCH_SIZE_ROWS,
    fs: Optional[s3fs.S3FileSystem] = None,
) -> Iterator[pl.DataFrame]:
    """
    Read multiple JSONL files, yielding Polars DataFrames of ~batch_size rows.
    Supports both local file paths and MinIO S3 paths (s3://...).
    Uses streaming line-by-line reading - memory stays bounded.
    """
    buffer: List[dict] = []

    for path in file_paths:
        log.debug("Reading %s", path)
        try:
            is_s3 = path.startswith("s3://") or (fs is not None and not os.path.exists(path))
            if is_s3 and fs is not None:
                clean_path = path[5:] if path.startswith("s3://") else path
                ctx = fs.open(clean_path, "r", encoding="utf-8")
            else:
                ctx = open(path, "r", encoding="utf-8")

            with ctx as fh:
                for raw in fh:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        row = json.loads(raw)
                        buffer.append(row)
                    except json.JSONDecodeError as exc:
                        log.warning("Bad JSON in %s: %s", path, exc)
                        continue

                    if len(buffer) >= batch_size:
                        yield pl.DataFrame(buffer)
                        buffer = []
        except OSError as exc:
            log.error("Cannot open %s: %s", path, exc)

    if buffer:
        yield pl.DataFrame(buffer)


# ---------------------------------------------------------------------------
# 1. Bronze -> Silver: youtube_shorts
# ---------------------------------------------------------------------------

def clean_video_catalog(df: pl.DataFrame) -> pl.DataFrame:
    """
    Quality rules applied to each batch of raw video records:
    - Cast types
    - Normalise tags / categories (list -> comma-separated string for Parquet portability)
    - Drop records missing video_id or duration
    - Enforce valid duration range 5-60 s (same as crawler filter)
    - Fill null numeric columns with 0
    """
    df = df.with_columns([
        pl.col("video_id").cast(pl.Utf8),
        pl.col("video_url").cast(pl.Utf8),
        pl.col("title").cast(pl.Utf8).fill_null(""),
        pl.col("description").cast(pl.Utf8).fill_null(""),
        pl.col("channel_id").cast(pl.Utf8).fill_null(""),
        pl.col("channel_title").cast(pl.Utf8).fill_null(""),
        pl.col("duration_seconds").cast(pl.Float64).fill_null(0.0),
        pl.col("view_count_at_crawl").cast(pl.Int64).fill_null(0),
        pl.col("like_count_at_crawl").cast(pl.Int64).fill_null(0),
        pl.col("comment_count_at_crawl").cast(pl.Int64).fill_null(0),
        pl.col("upload_date").cast(pl.Utf8).fill_null(""),
        pl.col("thumbnail_url").cast(pl.Utf8).fill_null(""),
        pl.col("crawled_at").cast(pl.Utf8),
    ])

    # Normalise list columns -> comma-separated string
    for col_name in ("categories", "tags"):
        if col_name in df.columns:
            # Cast list[str] -> JSON string, then strip brackets/quotes
            df = df.with_columns(
                pl.when(pl.col(col_name).is_null())
                .then(pl.lit(""))
                .otherwise(
                    pl.col(col_name)
                    .cast(pl.List(pl.Utf8))
                    .list.join(",")
                )
                .alias(col_name)
            )
        else:
            df = df.with_columns(pl.lit("").alias(col_name))

    # Drop rows missing essential fields
    df = df.filter(
        pl.col("video_id").is_not_null()
        & (pl.col("video_id") != "")
        & pl.col("duration_seconds").is_not_null()
        & (pl.col("duration_seconds") >= 5.0)
        & (pl.col("duration_seconds") <= 60.0)
    )

    # Deduplicate within batch
    df = df.unique(subset=["video_id"], keep="first")

    return df


def etl_youtube_bronze_to_silver(
    youtube_dir: str = BRONZE_YOUTUBE_DIR,
) -> int:
    """
    Load all youtube_shorts JSONL files, clean them, and write a single
    Parquet to s3://silver/youtube_shorts/videos.parquet.

    Returns number of rows written.
    """
    log.info("=== Bronze -> Silver: youtube_shorts ===")
    fs = _get_fs()
    ensure_silver_bucket(fs)

    jsonl_files: List[str] = []
    try:
        if fs.exists(BRONZE_BUCKET):
            s3_matches = sorted(fs.glob(f"{BRONZE_VIDEOS_S3_PREFIX}/*.jsonl"))
            if s3_matches:
                jsonl_files = [f"s3://{f}" for f in s3_matches]
                log.info("Found %d JSONL batch files in MinIO Bronze bucket (s3://%s)", len(jsonl_files), BRONZE_VIDEOS_S3_PREFIX)
    except Exception as exc:
        log.warning("Could not query MinIO Bronze bucket: %s", exc)

    if not jsonl_files:
        resolved_dir = None
        for candidate in [youtube_dir, "./data_lake/raw/youtube_shorts", "./raw/data_lake/raw/youtube_shorts"]:
            if candidate and os.path.exists(candidate):
                resolved_dir = candidate
                break
        if resolved_dir:
            jsonl_files = sorted(glob.glob(os.path.join(resolved_dir, "*.jsonl")))
            log.info("Found %d JSONL batch files from local disk (%s)", len(jsonl_files), resolved_dir)

    if not jsonl_files:
        raise FileNotFoundError(f"No JSONL files found in MinIO Bronze or local path {youtube_dir}")

    batches: List[pl.DataFrame] = []
    total_raw = 0
    total_clean = 0

    for batch_df in _jsonl_batch_reader(jsonl_files, batch_size=BATCH_SIZE_ROWS, fs=fs):
        total_raw += len(batch_df)
        cleaned = clean_video_catalog(batch_df)
        total_clean += len(cleaned)
        batches.append(cleaned)
        log.info("  Batch processed: %d raw -> %d clean rows", len(batch_df), len(cleaned))

    if not batches:
        raise RuntimeError("No data after cleaning video catalog")

    # Combine all batches and deduplicate globally
    combined = pl.concat(batches).unique(subset=["video_id"], keep="first")
    total_clean = len(combined)

    silver_path = "s3://silver/youtube_shorts/videos.parquet"
    log.info("Writing %d video rows to %s", total_clean, silver_path)

    with fs.open(silver_path, "wb") as f:
        combined.write_parquet(f, compression="snappy")

    log.info(
        "youtube_shorts: %d raw rows -> %d clean rows written to Silver",
        total_raw, total_clean,
    )
    return total_clean


# ---------------------------------------------------------------------------
# 2. Bronze -> Silver: user_interactions
# ---------------------------------------------------------------------------

def clean_interactions(df: pl.DataFrame) -> pl.DataFrame:
    """
    Quality rules applied to each batch of raw interaction events.

    Noise types injected in user-logs.py that we REMOVE:
    - MISSING_USER_ID      : user_id IS NULL
    - NEGATIVE_TIMER_BUG   : watch_duration_seconds < 0
    - OUTLIER_WATCH_TIME   : watch_duration_seconds > MAX_WATCH_SECONDS
    - BOT_SPAM             : app_version == 'bot_v1.0'
    - OFFLINE_CLOCK_SKEW   : server_received_timestamp - timestamp > 3 days (ms)
    - DUPLICATE_EVENT      : same event_id (keep first occurrence)

    BLOCK_CREATOR_NULL_WATCH is retained (null watch metrics are legitimate signals).
    """
    # Cast / standardise types
    df = df.with_columns([
        pl.col("event_id").cast(pl.Utf8),
        pl.col("session_id").cast(pl.Utf8),
        pl.col("user_id").cast(pl.Utf8),
        pl.col("video_id").cast(pl.Utf8),
        pl.col("event_type").cast(pl.Utf8).fill_null("video_watch"),
        pl.col("impression_index").cast(pl.Int32).fill_null(0),
        pl.col("watch_duration_seconds").cast(pl.Float64),
        pl.col("completion_rate").cast(pl.Float64),
        pl.col("comment_read_duration_seconds").cast(pl.Float64).fill_null(0.0),
        pl.col("click_profile").cast(pl.Int8).fill_null(0),
        pl.col("action_like").cast(pl.Int8).fill_null(0),
        pl.col("action_comment").cast(pl.Int8).fill_null(0),
        pl.col("action_share").cast(pl.Int8).fill_null(0),
        pl.col("action_skip").cast(pl.Int8).fill_null(0),
        pl.col("action_block_creator").cast(pl.Int8).fill_null(0),
        pl.col("device_os").cast(pl.Utf8).fill_null("Unknown"),
        pl.col("app_version").cast(pl.Utf8).fill_null(""),
        pl.col("timestamp").cast(pl.Int64),
        pl.col("server_received_timestamp").cast(pl.Int64),
    ])

    # Derive server_dt for partitioning
    # server_received_timestamp is Unix milliseconds -> cast directly to Datetime("ms")
    df = df.with_columns(
        pl.col("server_received_timestamp")
        .cast(pl.Datetime("ms"))
        .dt.replace_time_zone("UTC")
        .alias("server_dt")
    )

    three_days_ms = 3 * 24 * 3_600 * 1_000

    # Apply noise-removal filters
    df = df.filter(
        # MISSING_USER_ID
        pl.col("user_id").is_not_null()
        & (pl.col("user_id") != "")
        # NEGATIVE_TIMER_BUG
        & (
            pl.col("watch_duration_seconds").is_null()
            | (pl.col("watch_duration_seconds") >= 0.0)
        )
        # OUTLIER_WATCH_TIME
        & (
            pl.col("watch_duration_seconds").is_null()
            | (pl.col("watch_duration_seconds") <= MAX_WATCH_SECONDS)
        )
        # BOT_SPAM
        & (pl.col("app_version") != "bot_v1.0")
        # OFFLINE_CLOCK_SKEW
        & (
            (pl.col("server_received_timestamp") - pl.col("timestamp"))
            < three_days_ms
        )
    )

    # Deduplicate by event_id (DUPLICATE_EVENT noise)
    df = df.unique(subset=["event_id"], keep="first")

    # Add derived date columns for Gold partitioning
    df = df.with_columns([
        pl.col("server_dt").dt.date().alias("event_date"),
        pl.col("server_dt").dt.hour().alias("event_hour"),
    ])

    return df


def etl_interactions_bronze_to_silver(
    logs_dir: str = BRONZE_INTERACTIONS_DIR,
) -> int:
    """
    Walk the partitioned JSONL tree under logs_dir, clean in streaming batches,
    and write cleaned Parquet to s3://silver/user_interactions/interactions.parquet.

    Returns number of rows written.
    """
    log.info("=== Bronze -> Silver: user_interactions ===")
    fs = _get_fs()
    ensure_silver_bucket(fs)

    jsonl_files: List[str] = []
    try:
        if fs.exists(BRONZE_BUCKET):
            s3_matches = sorted(fs.glob(f"{BRONZE_INTERACTIONS_S3_PREFIX}/**/*.jsonl"))
            if s3_matches:
                jsonl_files = [f"s3://{f}" for f in s3_matches]
                log.info("Found %d JSONL partition files in MinIO Bronze bucket (s3://%s)", len(jsonl_files), BRONZE_INTERACTIONS_S3_PREFIX)
    except Exception as exc:
        log.warning("Could not query MinIO Bronze bucket: %s", exc)

    if not jsonl_files:
        resolved_dir = None
        for candidate in [logs_dir, "./data_lake/raw/user_interaction_logs", "./raw/data_lake/raw/user_interaction_logs"]:
            if candidate and os.path.exists(candidate):
                resolved_dir = candidate
                break
        if resolved_dir:
            jsonl_files = sorted(glob.glob(os.path.join(resolved_dir, "**", "*.jsonl"), recursive=True))
            log.info("Found %d JSONL partition files from local disk (%s)", len(jsonl_files), resolved_dir)

    if not jsonl_files:
        raise FileNotFoundError(f"No JSONL files found under MinIO Bronze or local path {logs_dir}")

    batches: List[pl.DataFrame] = []
    total_raw = 0
    total_clean = 0

    for batch_df in _jsonl_batch_reader(jsonl_files, batch_size=BATCH_SIZE_ROWS, fs=fs):
        total_raw += len(batch_df)
        cleaned = clean_interactions(batch_df)
        total_clean += len(cleaned)
        batches.append(cleaned)
        log.info("  Batch: %d raw -> %d clean rows", len(batch_df), len(cleaned))

    if not batches:
        raise RuntimeError("No interaction data after cleaning")

    combined = pl.concat(batches).unique(subset=["event_id"], keep="first")
    total_clean = len(combined)

    silver_path = "s3://silver/user_interactions/interactions.parquet"
    log.info("Writing %d interaction rows to %s", total_clean, silver_path)

    with fs.open(silver_path, "wb") as f:
        combined.write_parquet(f, compression="snappy")

    log.info(
        "user_interactions: %d raw rows -> %d clean rows written to Silver",
        total_raw, total_clean,
    )
    return total_clean


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def run_bronze_to_silver(
    youtube_dir: str = BRONZE_YOUTUBE_DIR,
    logs_dir: str = BRONZE_INTERACTIONS_DIR,
) -> dict:
    """Run both ETL jobs; return row counts."""
    videos_count = etl_youtube_bronze_to_silver(youtube_dir)
    interactions_count = etl_interactions_bronze_to_silver(logs_dir)
    return {
        "videos_written": videos_count,
        "interactions_written": interactions_count,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Bronze -> Silver ETL using Polars streaming batches"
    )
    parser.add_argument(
        "--youtube-dir",
        default=BRONZE_YOUTUBE_DIR,
        help="Directory containing youtube_shorts *.jsonl files",
    )
    parser.add_argument(
        "--logs-dir",
        default=BRONZE_INTERACTIONS_DIR,
        help="Root directory of user_interaction_logs partitions",
    )
    args = parser.parse_args()

    result = run_bronze_to_silver(
        youtube_dir=args.youtube_dir,
        logs_dir=args.logs_dir,
    )
    print(f"\nDone. {result}")
