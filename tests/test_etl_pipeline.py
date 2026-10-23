"""
Tests for the Bronze -> Silver -> Gold ETL pipeline.

Run:  python -m unittest discover -s tests -v
"""

import io
import json
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl


class BronzeToSilverVideoTests(unittest.TestCase):
    """Unit tests for the video catalog cleaning step."""

    def _make_video_df(self, rows):
        return pl.DataFrame(rows)

    def test_valid_video_passes_through(self):
        from etl.bronze_to_silver import clean_video_catalog

        df = self._make_video_df([{
            "video_id": "abc123",
            "video_url": "https://youtube.com/shorts/abc123",
            "title": "Test video",
            "description": "desc",
            "channel_id": "ch1",
            "channel_title": "Channel",
            "duration_seconds": 20.0,
            "categories": ["tech"],
            "tags": ["python", "data"],
            "view_count_at_crawl": 1000,
            "like_count_at_crawl": 50,
            "comment_count_at_crawl": 5,
            "upload_date": "20260101",
            "thumbnail_url": "https://img.jpg",
            "crawled_at": "2026-10-01T00:00:00+00:00",
        }])
        result = clean_video_catalog(df)
        self.assertEqual(len(result), 1)
        self.assertEqual(result["video_id"][0], "abc123")

    def test_video_missing_id_is_dropped(self):
        from etl.bronze_to_silver import clean_video_catalog

        df = self._make_video_df([{
            "video_id": None,
            "video_url": "",
            "title": "No ID",
            "description": "",
            "channel_id": "",
            "channel_title": "",
            "duration_seconds": 20.0,
            "categories": ["tech"],
            "tags": [],
            "view_count_at_crawl": 0,
            "like_count_at_crawl": 0,
            "comment_count_at_crawl": 0,
            "upload_date": "",
            "thumbnail_url": "",
            "crawled_at": "2026-10-01T00:00:00+00:00",
        }])
        result = clean_video_catalog(df)
        self.assertEqual(len(result), 0)

    def test_video_too_long_is_dropped(self):
        from etl.bronze_to_silver import clean_video_catalog

        df = self._make_video_df([{
            "video_id": "long1",
            "video_url": "",
            "title": "",
            "description": "",
            "channel_id": "",
            "channel_title": "",
            "duration_seconds": 61.0,   # > 60s limit
            "categories": ["tech"],
            "tags": [],
            "view_count_at_crawl": 0,
            "like_count_at_crawl": 0,
            "comment_count_at_crawl": 0,
            "upload_date": "",
            "thumbnail_url": "",
            "crawled_at": "2026-10-01T00:00:00+00:00",
        }])
        result = clean_video_catalog(df)
        self.assertEqual(len(result), 0)

    def test_list_columns_joined_to_string(self):
        from etl.bronze_to_silver import clean_video_catalog

        df = self._make_video_df([{
            "video_id": "vid1",
            "video_url": "",
            "title": "",
            "description": "",
            "channel_id": "",
            "channel_title": "",
            "duration_seconds": 15.0,
            "categories": ["tech", "education"],
            "tags": ["python", "data"],
            "view_count_at_crawl": 0,
            "like_count_at_crawl": 0,
            "comment_count_at_crawl": 0,
            "upload_date": "",
            "thumbnail_url": "",
            "crawled_at": "2026-10-01T00:00:00+00:00",
        }])
        result = clean_video_catalog(df)
        self.assertEqual(result["categories"][0], "tech,education")
        self.assertEqual(result["tags"][0], "python,data")

    def test_duplicate_videos_deduplicated(self):
        from etl.bronze_to_silver import clean_video_catalog

        row = {
            "video_id": "dup1",
            "video_url": "",
            "title": "",
            "description": "",
            "channel_id": "",
            "channel_title": "",
            "duration_seconds": 20.0,
            "categories": ["food"],
            "tags": [],
            "view_count_at_crawl": 0,
            "like_count_at_crawl": 0,
            "comment_count_at_crawl": 0,
            "upload_date": "",
            "thumbnail_url": "",
            "crawled_at": "2026-10-01T00:00:00+00:00",
        }
        df = pl.DataFrame([row, row])
        result = clean_video_catalog(df)
        self.assertEqual(len(result), 1)


def _make_interaction(**overrides):
    base = {
        "event_id": "evt_aaa111",
        "session_id": "sess_bbb",
        "user_id": "usr_00001",
        "video_id": "vid1",
        "event_type": "video_watch",
        "impression_index": 1,
        "watch_duration_seconds": 20.0,
        "completion_rate": 0.95,
        "comment_read_duration_seconds": 0.0,
        "click_profile": 0,
        "action_like": 0,
        "action_comment": 0,
        "action_share": 0,
        "action_skip": 0,
        "action_block_creator": 0,
        "device_os": "Android",
        "app_version": "v3.12.0",
        "timestamp": 1790814724000,           # 2026-10-01 UTC in ms
        "server_received_timestamp": 1790814724021,
    }
    base.update(overrides)
    return base


class BronzeToSilverInteractionTests(unittest.TestCase):
    """Unit tests for each noise type removal."""

    def _clean(self, rows):
        from etl.bronze_to_silver import clean_interactions
        return clean_interactions(pl.DataFrame(rows))

    def test_valid_event_retained(self):
        result = self._clean([_make_interaction()])
        self.assertEqual(len(result), 1)

    def test_missing_user_id_dropped(self):
        result = self._clean([_make_interaction(user_id=None)])
        self.assertEqual(len(result), 0)

    def test_negative_watch_duration_dropped(self):
        result = self._clean([_make_interaction(watch_duration_seconds=-1.0)])
        self.assertEqual(len(result), 0)

    def test_outlier_watch_time_dropped(self):
        result = self._clean([_make_interaction(watch_duration_seconds=7200.0)])
        self.assertEqual(len(result), 0)

    def test_bot_spam_dropped(self):
        result = self._clean([_make_interaction(app_version="bot_v1.0")])
        self.assertEqual(len(result), 0)

    def test_offline_clock_skew_dropped(self):
        # server_ts 4 days ahead of event ts -> exceeds 3-day threshold
        server_ts = 1790814724021 + (4 * 24 * 3600 * 1000)
        result = self._clean([_make_interaction(server_received_timestamp=server_ts)])
        self.assertEqual(len(result), 0)

    def test_duplicate_event_deduplicated(self):
        row = _make_interaction()
        result = self._clean([row, row])
        self.assertEqual(len(result), 1)

    def test_block_creator_null_watch_retained(self):
        # BLOCK_CREATOR events have null watch metrics but ARE kept
        row = _make_interaction(
            watch_duration_seconds=None,
            completion_rate=None,
            action_block_creator=1,
            action_skip=1,
        )
        result = self._clean([row])
        self.assertEqual(len(result), 1)

    def test_event_date_and_hour_derived_correctly(self):
        result = self._clean([_make_interaction()])
        self.assertEqual(result["event_date"][0], date(2026, 10, 1))
        # hour=0 UTC for ts 1790814724021
        self.assertIn(result["event_hour"][0], range(24))


if __name__ == "__main__":
    unittest.main()
