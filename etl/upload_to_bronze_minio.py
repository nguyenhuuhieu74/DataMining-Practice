"""
Upload Local Raw Data -> MinIO Bronze Bucket
============================================
Syncs raw JSONL files from local Data Lake to MinIO bucket 'bronze'.
This establishes the Bronze Layer on MinIO as an S3-compatible Raw Object Lake.

Bronze Layout on MinIO:
-----------------------
s3://bronze/youtube_shorts/<filename>.jsonl
s3://bronze/user_interaction_logs/year=YYYY/month=MM/day=DD/hour=HH/user_interactions_raw.jsonl

Run standalone:
---------------
    python etl/upload_to_bronze_minio.py [--force]
"""

import argparse
import glob
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import s3fs

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.config import (
    BRONZE_BUCKET,
    BRONZE_INTERACTIONS_DIR,
    BRONZE_INTERACTIONS_S3_PREFIX,
    BRONZE_VIDEOS_S3_PREFIX,
    BRONZE_YOUTUBE_DIR,
    S3_OPTS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("upload_to_bronze_minio")


def get_s3_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=S3_OPTS["key"],
        secret=S3_OPTS["secret"],
        client_kwargs=S3_OPTS["client_kwargs"],
    )


def ensure_bronze_bucket(fs: s3fs.S3FileSystem) -> None:
    if not fs.exists(BRONZE_BUCKET):
        fs.mkdir(BRONZE_BUCKET)
        log.info("Created MinIO Bronze bucket: %s", BRONZE_BUCKET)
    else:
        log.debug("MinIO Bronze bucket already exists: %s", BRONZE_BUCKET)


def _resolve_dir(candidate_dirs: List[str]) -> Optional[str]:
    for d in candidate_dirs:
        if d and os.path.exists(d):
            return os.path.abspath(d)
    return None


def upload_youtube_shorts_to_bronze(
    fs: s3fs.S3FileSystem,
    local_dir: Optional[str] = None,
    force: bool = False,
) -> Tuple[int, int]:
    """Uploads youtube_shorts JSONL files to s3://bronze/youtube_shorts/"""
    resolved = _resolve_dir([
        local_dir,
        BRONZE_YOUTUBE_DIR,
        "./data_lake/raw/youtube_shorts",
        "./raw/data_lake/raw/youtube_shorts",
    ])
    if not resolved:
        log.warning("No local youtube_shorts directory found")
        return 0, 0

    jsonl_files = sorted(glob.glob(os.path.join(resolved, "*.jsonl")))
    uploaded = 0
    skipped = 0

    for local_file in jsonl_files:
        filename = os.path.basename(local_file)
        remote_path = f"{BRONZE_VIDEOS_S3_PREFIX}/{filename}"
        local_size = os.path.getsize(local_file)

        if not force and fs.exists(remote_path):
            remote_info = fs.info(remote_path)
            if remote_info.get("size") == local_size:
                skipped += 1
                continue

        fs.put(local_file, remote_path)
        uploaded += 1
        log.info("  Uploaded -> s3://%s (%d bytes)", remote_path, local_size)

    return uploaded, skipped


def upload_user_logs_to_bronze(
    fs: s3fs.S3FileSystem,
    local_dir: Optional[str] = None,
    force: bool = False,
) -> Tuple[int, int]:
    """Uploads partitioned user_interaction_logs to s3://bronze/user_interaction_logs/..."""
    resolved = _resolve_dir([
        local_dir,
        BRONZE_INTERACTIONS_DIR,
        "./data_lake/raw/user_interaction_logs",
        "./raw/data_lake/raw/user_interaction_logs",
    ])
    if not resolved:
        log.warning("No local user_interaction_logs directory found")
        return 0, 0

    jsonl_files = sorted(glob.glob(os.path.join(resolved, "**", "*.jsonl"), recursive=True))
    uploaded = 0
    skipped = 0

    for local_file in jsonl_files:
        rel_path = os.path.relpath(local_file, resolved).replace("\\", "/")
        remote_path = f"{BRONZE_INTERACTIONS_S3_PREFIX}/{rel_path}"
        local_size = os.path.getsize(local_file)

        if not force and fs.exists(remote_path):
            remote_info = fs.info(remote_path)
            if remote_info.get("size") == local_size:
                skipped += 1
                continue

        fs.put(local_file, remote_path)
        uploaded += 1
        log.info("  Uploaded -> s3://%s (%d bytes)", remote_path, local_size)

    return uploaded, skipped


def sync_local_raw_to_bronze(
    youtube_dir: Optional[str] = None,
    logs_dir: Optional[str] = None,
    force: bool = False,
) -> Dict[str, int]:
    """
    Main function to sync local raw data to MinIO Bronze bucket.
    """
    log.info("=== Syncing Local Raw Data -> MinIO Bronze (S3) ===")
    fs = get_s3_fs()
    ensure_bronze_bucket(fs)

    yt_uploaded, yt_skipped = upload_youtube_shorts_to_bronze(fs, youtube_dir, force=force)
    log.info("YouTube Shorts: %d uploaded, %d skipped (up to date)", yt_uploaded, yt_skipped)

    logs_uploaded, logs_skipped = upload_user_logs_to_bronze(fs, logs_dir, force=force)
    log.info("User Logs: %d uploaded, %d skipped (up to date)", logs_uploaded, logs_skipped)

    result = {
        "youtube_uploaded": yt_uploaded,
        "youtube_skipped": yt_skipped,
        "logs_uploaded": logs_uploaded,
        "logs_skipped": logs_skipped,
    }
    log.info("Sync complete: %s", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload local raw data to MinIO Bronze bucket")
    parser.add_argument("--youtube-dir", default=None, help="Local youtube_shorts directory")
    parser.add_argument("--logs-dir", default=None, help="Local user_interaction_logs directory")
    parser.add_argument("--force", action="store_true", help="Re-upload even if file exists with same size")
    args = parser.parse_args()

    sync_local_raw_to_bronze(
        youtube_dir=args.youtube_dir,
        logs_dir=args.logs_dir,
        force=args.force,
    )
