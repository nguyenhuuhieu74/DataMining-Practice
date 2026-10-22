"""Offline checks: python -m unittest discover -s tests -v."""

from datetime import datetime, timezone
import json
from pathlib import Path
import runpy
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
CRAWLER = runpy.run_path(str(ROOT / "raw/crawl-short.py"))
SIMULATOR = runpy.run_path(str(ROOT / "raw/user-logs.py"))


class CrawlTests(unittest.TestCase):
    def test_retry_skips_existing_video_ids(self):
        client = MagicMock()
        client.__enter__.return_value = client
        client.extract_info.side_effect = lambda url, **kwargs: (
            {"entries": [{"id": "video1", "duration": 20}]}
            if url.startswith("ytsearch") else {"title": "Example"}
        )
        with tempfile.TemporaryDirectory() as tmp, patch(
            "yt_dlp.YoutubeDL", return_value=client
        ):
            for _ in range(2):
                CRAWLER["crawl_youtube_shorts"](
                    {"tech": ["python"]}, output_dir=tmp, batch_name="retry"
                )
            records = [json.loads(line) for line in (Path(tmp) / "retry.jsonl").read_text().splitlines()]
            self.assertEqual([row["video_id"] for row in records], ["video1"])

    def test_all_failed_queries_raise(self):
        client = MagicMock()
        client.__enter__.return_value = client
        client.extract_info.side_effect = RuntimeError("Network unavailable")
        with tempfile.TemporaryDirectory() as tmp, patch(
            "yt_dlp.YoutubeDL", return_value=client
        ), self.assertRaisesRegex(RuntimeError, "All search queries failed"):
            CRAWLER["crawl_youtube_shorts"]({"tech": ["python"]}, output_dir=tmp)


class SimulationTests(unittest.TestCase):
    def test_empty_catalog_fails_clearly(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "No video data found"):
                SIMULATOR["run_user_simulation_pipeline"](tmp, str(Path(tmp) / "output"))

    def test_requested_window_users_and_timestamp_partitions(self):
        start = datetime(2026, 10, 21, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            catalog = Path(tmp) / "items"
            catalog.mkdir()
            (catalog / "batch.jsonl").write_text(
                json.dumps({"video_id": "video1", "duration_seconds": 20}) + "\n",
                encoding="utf-8",
            )
            output = Path(tmp) / "logs"
            with patch.object(np.random, "poisson", return_value=1):
                count = SIMULATOR["run_user_simulation_pipeline"](
                    str(catalog), str(output), start_date=start,
                    simulation_days=1, num_users=2,
                )
            records = []
            for path in output.rglob("*.jsonl"):
                for line in path.read_text().splitlines():
                    event = json.loads(line)
                    records.append(event)
                    received = datetime.fromtimestamp(
                        event["server_received_timestamp"] / 1000, timezone.utc
                    )
                    expected = f"year={received:%Y}/month={received:%m}/day={received:%d}/hour={received:%H}"
                    self.assertEqual(path.parent.relative_to(output).as_posix(), expected)
                    self.assertEqual(event["video_id"], "video1")
                    user = event["user_id"]
                    if user and user.startswith("usr_"):
                        self.assertLess(int(user[4:]), 2)
                    self.assertGreaterEqual(received, start)
                    self.assertLess(received.timestamp(), start.timestamp() + 25 * 3600)
            self.assertEqual(count, len(records))
            self.assertGreater(count, 0)


if __name__ == "__main__":
    unittest.main()
