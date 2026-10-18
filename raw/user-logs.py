import glob
import json
import math
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
import numpy as np
import pandas as pd

NUM_USERS = 1000
LATENT_DIM = 8
SIMULATION_DAYS = 3
START_DATE = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
NOISE_RATE = 0.12 


def inject_noise(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    if random.random() > NOISE_RATE:
        return [event]

    noise_type = random.choice(
        [
            "BLOCK_CREATOR_NULL_WATCH",
            "DUPLICATE_EVENT",
            "OUTLIER_WATCH_TIME",
            "NEGATIVE_TIMER_BUG",
            "MISSING_USER_ID",
            "OFFLINE_CLOCK_SKEW",
            "BOT_SPAM",
        ]
    )

    if noise_type == "BLOCK_CREATOR_NULL_WATCH":
        evt = event.copy()
        evt["watch_duration_seconds"] = None
        evt["completion_rate"] = None
        evt["comment_read_duration_seconds"] = 0.0
        evt["action_block_creator"] = 1
        evt["action_skip"] = 1
        return [evt]

    elif noise_type == "DUPLICATE_EVENT":
        evt1 = event.copy()
        evt2 = event.copy()
        evt2["server_received_timestamp"] += random.randint(100, 800)
        return [evt1, evt2]

    elif noise_type == "OUTLIER_WATCH_TIME":
        evt = event.copy()
        outlier_sec = round(random.uniform(3600.0, 28800.0), 2)
        evt["watch_duration_seconds"] = outlier_sec
        evt["completion_rate"] = round(outlier_sec / 15.0, 2)
        return [evt]

    elif noise_type == "NEGATIVE_TIMER_BUG":
        evt = event.copy()
        evt["watch_duration_seconds"] = -1.0
        evt["completion_rate"] = None
        return [evt]

    elif noise_type == "MISSING_USER_ID":
        evt = event.copy()
        evt["user_id"] = None
        return [evt]

    elif noise_type == "OFFLINE_CLOCK_SKEW":
        evt = event.copy()
        two_days_ms = 2 * 24 * 3600 * 1000
        evt["timestamp"] -= two_days_ms
        return [evt]

    elif noise_type == "BOT_SPAM":
        evt = event.copy()
        evt["user_id"] = f"bot_{random.randint(10, 99)}"
        evt["watch_duration_seconds"] = 0.01
        evt["completion_rate"] = 0.001
        evt["app_version"] = "bot_v1.0"
        return [evt]

    return [event]


def initialize_simulation_space(items_df: pd.DataFrame, num_users: int):
    user_vectors = np.random.dirichlet(
        alpha=np.ones(LATENT_DIM), size=num_users
    )
    item_vectors = np.random.dirichlet(
        alpha=np.ones(LATENT_DIM), size=len(items_df)
    )

    raw_pop = np.random.zipf(a=1.5, size=len(items_df))
    item_popularity = (raw_pop - raw_pop.min()) / (
        raw_pop.max() - raw_pop.min() + 1e-5
    )

    items_df["item_idx"] = np.arange(len(items_df))
    items_df["pop_score"] = item_popularity

    return user_vectors, item_vectors, items_df


def get_hourly_activity_multiplier(hour: int) -> float:
    hourly_weights = [
        0.2,
        0.1,
        0.1,
        0.1,
        0.1,
        0.2,
        0.3,
        0.5,
        0.7,
        0.6,
        0.8,
        1.2,
        1.5,
        1.1,
        0.8,
        0.7,
        0.8,
        1.0,
        1.3,
        1.8,
        2.2,
        2.5,
        2.0,
        1.0,
    ]
    return hourly_weights[hour]


def run_user_simulation_pipeline(items_dir: str, output_base_dir: str):
    unique_items_map = {}
    jsonl_files = glob.glob(os.path.join(items_dir, "*.jsonl"))

    print(f"Tìm thấy {len(jsonl_files)} file batch .jsonl trong thư mục {items_dir}")

    for fpath in jsonl_files:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        record = json.loads(line)
                        v_id = record.get("video_id")
                        if v_id and v_id not in unique_items_map:
                            unique_items_map[v_id] = record
        except Exception as e:
            print(f"Lỗi khi đọc file {fpath}: {e}")

    items = list(unique_items_map.values())

    if not items:
        print("No video data found")

    items_df = pd.DataFrame(items)
    user_vecs, item_vecs, items_df = initialize_simulation_space(
        items_df, NUM_USERS
    )

    print(
        f"Synthesizing {NUM_USERS} Users & {len(items_df)} unique video"
    )
    total_events_written = 0

    for day in range(SIMULATION_DAYS):
        for hour in range(24):
            current_time = START_DATE + timedelta(days=day, hours=hour)
            hour_multiplier = get_hourly_activity_multiplier(hour)
            active_sessions_count = int(
                np.random.poisson(lam=60 * hour_multiplier)
            )

            hourly_events_to_write = []

            for _ in range(active_sessions_count):
                user_id_idx = np.random.randint(0, NUM_USERS)
                user_id_str = f"usr_{user_id_idx:05d}"
                session_id_str = f"sess_{uuid.uuid4().hex[:10]}"

                session_length = int(
                    np.random.lognormal(mean=2.0, sigma=0.5)
                )
                session_length = max(1, min(session_length, 30))

                curr_event_time = current_time + timedelta(
                    seconds=random.randint(0, 3500)
                )

                sample_probs = (
                    items_df["pop_score"] + 0.1
                ) / (items_df["pop_score"] + 0.1).sum()
                selected_item_indices = np.random.choice(
                    items_df["item_idx"].values,
                    size=session_length,
                    replace=True,
                    p=sample_probs,
                )

                for idx, item_idx in enumerate(selected_item_indices):
                    item_row = items_df.iloc[item_idx]
                    duration = float(item_row["duration_seconds"])

                    u_v = user_vecs[user_id_idx]
                    i_v = item_vecs[item_idx]
                    raw_dot = np.dot(u_v, i_v)
                    affinity = 1.0 / (
                        1.0 + math.exp(-(raw_dot * 3.0 + item_row["pop_score"]))
                    )

                    if random.random() < affinity:
                        watch_ratio = np.random.uniform(0.80, 1.80)
                        action_skip = 0
                    else:
                        watch_ratio = np.random.uniform(
                            0.02, min(3.0 / duration, 0.25)
                        )
                        action_skip = 1

                    watch_duration = round(watch_ratio * duration, 2)
                    completion_rate = round(watch_duration / duration, 3)

                    if random.random() < 0.25:
                        comment_read_duration = round(
                            np.random.uniform(3.0, 25.0), 2
                        )
                        action_comment_flag = (
                            1 if random.random() < 0.2 else 0
                        )
                    else:
                        comment_read_duration = 0.0
                        action_comment_flag = 0

                    click_profile = (
                        1
                        if (affinity > 0.7 and random.random() < 0.3)
                        else 0
                    )
                    action_like = (
                        1
                        if (
                            completion_rate >= 0.85 and random.random() < 0.60
                        )
                        else 0
                    )
                    action_share = (
                        1
                        if (completion_rate >= 1.1 and random.random() < 0.10)
                        else 0
                    )

                    ts_ms = int(curr_event_time.timestamp() * 1000)
                    server_ts_ms = ts_ms + random.randint(20, 150)

                    base_event = {
                        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
                        "session_id": session_id_str,
                        "user_id": user_id_str,
                        "video_id": item_row["video_id"],
                        "event_type": "video_watch",
                        "impression_index": idx + 1,
                        "watch_duration_seconds": watch_duration,
                        "completion_rate": completion_rate,
                        "comment_read_duration_seconds": comment_read_duration,
                        "click_profile": click_profile,
                        "action_like": action_like,
                        "action_comment": action_comment_flag,
                        "action_share": action_share,
                        "action_skip": action_skip,
                        "action_block_creator": 0,
                        "device_os": random.choice(["Android", "iOS"]),
                        "app_version": "v3.12.0",
                        "timestamp": ts_ms,
                        "server_received_timestamp": server_ts_ms,
                    }

                    processed_events = inject_noise(base_event)
                    hourly_events_to_write.extend(processed_events)

                    curr_event_time += timedelta(
                        seconds=watch_duration
                        + comment_read_duration
                        + random.uniform(0.5, 1.5)
                    )
                    
            if hourly_events_to_write:
                events_by_partition = {}
                for evt in hourly_events_to_write:
                    server_dt = datetime.fromtimestamp(
                        evt["server_received_timestamp"] / 1000.0,
                        tz=timezone.utc,
                    )

                    partition_path = os.path.join(
                        output_base_dir,
                        f"year={server_dt.year}",
                        f"month={server_dt.month:02d}",
                        f"day={server_dt.day:02d}",
                        f"hour={server_dt.hour:02d}",
                    )
                    
                    if partition_path not in events_by_partition:
                        events_by_partition[partition_path] = []
                    events_by_partition[partition_path].append(evt)

                # Ghi dữ liệu theo Batch cho từng thư mục
                for p_path, evts in events_by_partition.items():
                    os.makedirs(p_path, exist_ok=True)
                    file_path = os.path.join(p_path, "user_interactions_raw.jsonl")
                    with open(file_path, "a", encoding="utf-8") as f_out:
                        for e in evts:
                            f_out.write(json.dumps(e, ensure_ascii=False) + "\n")

                total_events_written += len(hourly_events_to_write)

    print(
        f"Finish synthesizing {total_events_written} user interaction records"
    )


if __name__ == "__main__":
    run_user_simulation_pipeline(
        items_dir="./data_lake/raw/youtube_shorts",
        output_base_dir="./data_lake/raw/user_interaction_logs/",
    )