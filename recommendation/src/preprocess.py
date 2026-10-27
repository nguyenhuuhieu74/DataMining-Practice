"""Explicit derived preprocessing; original Gold, raw files and ETL stay read-only."""
import math
import numpy as np
import pandas as pd

from pipeline_common import (ROOT, ARTIFACTS, REPORTS, PROTOCOL, read_json, write_json,
                             checksum, snapshot_sources, load_frame, save_frame, verify_sources)


def clean_events(frame, origin):
    frame = frame.copy()
    if origin == "historical":
        frame["cohort"] = "historical_assumed_single"
        frame = frame.rename(columns={"timestamp": "timestamp_ms", "server_received_timestamp": "server_received_timestamp_ms"})
    frame["origin"] = origin
    audit, rejected = [], []
    def exclude(mask, reason):
        nonlocal frame
        mask = mask.fillna(True)
        removed = frame.loc[mask].copy()
        removed["exclusion_reason"] = reason
        rejected.append(removed)
        audit.append({"origin": origin, "rule": reason, "removed": len(removed), "remaining": int((~mask).sum())})
        frame = frame.loc[~mask].copy()
    invalid_id = pd.Series(False, index=frame.index)
    for name in ["event_id", "session_id", "user_id", "video_id"]:
        invalid_id |= ~frame[name].map(lambda value: isinstance(value, str) and bool(value.strip()))
    exclude(invalid_id, "missing_identifier")
    exclude(frame["user_id"].str.startswith("bot_") | frame["app_version"].eq("bot_v1.0"), "bot")
    watch = pd.to_numeric(frame["watch_duration_seconds"], errors="coerce")
    exclude(watch.lt(0) | watch.gt(3600) | (watch.notna() & ~np.isfinite(watch)), "invalid_watch_time")
    actions = [name for name in frame.columns if name.startswith("action_")] + ["click_profile"]
    exclude(~frame[actions].isin([0, 1]).all(axis=1), "invalid_action_flag")
    delay = frame["server_received_timestamp_ms"] - frame["timestamp_ms"]
    exclude(delay.lt(0) | delay.ge(3 * 86400000) | ~np.isfinite(delay), "invalid_timestamp_delay")
    exclude(frame["watch_duration_seconds"].isna() & frame["action_block_creator"].eq(0), "missing_watch_without_block")
    # Preserve earliest receipt; all remaining duplicate payloads must agree.
    repeated = frame.loc[frame.duplicated(["cohort", "event_id"], keep=False)]
    compare = [c for c in frame.columns if c not in ["_source_file", "server_received_timestamp_ms", "event_id"]]
    if len(repeated) and repeated.groupby(["cohort", "event_id"])[compare].nunique(dropna=False).gt(1).any().any():
        raise ValueError("Event ID trùng có payload mâu thuẫn")
    frame = frame.sort_values(["server_received_timestamp_ms", "event_id"], kind="stable")
    exclude(frame.duplicated(["cohort", "event_id"], keep="first"), "duplicate_event_id")
    frame["raw_user_id"] = frame["user_id"]
    frame["user_id"] = frame["cohort"] + ":" + frame["raw_user_id"]
    frame["clock_skew_flag"] = (frame["server_received_timestamp_ms"] - frame["timestamp_ms"]).ge(86400000)
    frame["identity_verified"] = frame["origin"].eq("current")
    frame["positive_label"] = (
        frame["action_block_creator"].eq(0) & frame["action_skip"].eq(0)
        & frame["completion_rate"].ge(0.8)
    ).astype(int)
    frame["engagement_label"] = (
        frame["action_block_creator"].eq(0) & frame["action_skip"].eq(0)
        & (frame["action_like"].eq(1) | frame["action_share"].eq(1))
    ).astype(int)
    return frame, audit, pd.concat(rejected, ignore_index=True)


def split_pairs(events, historical_day=False):
    events = events.copy()
    if historical_day:
        mask = events["origin"].eq("historical")
        days = pd.to_datetime(events.loc[mask, "server_received_timestamp_ms"], unit="ms", utc=True).dt.strftime("%Y%m%d")
        events.loc[mask, "user_id"] = events.loc[mask, "cohort"] + ":day=" + days + ":" + events.loc[mask, "raw_user_id"]
    pairs = events.sort_values(["server_received_timestamp_ms", "event_id"], kind="stable").drop_duplicates(["cohort", "user_id", "video_id"], keep="first").copy()
    pairs["split"] = ""
    cutoffs = {}
    for cohort, group in pairs.groupby("cohort"):
        times = group["server_received_timestamp_ms"].sort_values().to_numpy()
        train_end = int(times[math.floor(len(times) * PROTOCOL["split"]["train_fraction"])-1])
        validation_end = int(times[math.floor(len(times) * PROTOCOL["split"]["validation_end_fraction"])-1])
        rows = group.index
        pairs.loc[rows, "split"] = np.where(group.server_received_timestamp_ms <= train_end, "train",
            np.where(group.server_received_timestamp_ms <= validation_end, "validation", "test"))
        cutoffs[cohort] = {"train_end_ms": train_end, "validation_end_ms": validation_end}
    if pairs.duplicated(["cohort", "user_id", "video_id"]).any():
        raise ValueError("Cặp xuất hiện nhiều split")
    return pairs, cutoffs


def build_preprocessing():
    snapshot_sources()
    destination = ARTIFACTS / "preprocessing"
    if (destination / "manifest.json").exists():
        manifest = read_json(destination / "manifest.json")
        if manifest["protocol_sha256"] != checksum(ROOT / "protocol.json"):
            raise ValueError("Protocol đã đổi; cần phiên bản mới")
        for name, digest in manifest["files"].items():
            if checksum(destination / name) != digest:
                raise ValueError("Preprocessing artefact đã đổi")
        return manifest
    sources = ARTIFACTS / "sources"
    gold, gold_audit, gold_rejected = clean_events(load_frame(sources / "gold.jsonl"), "current")
    history_frame = load_frame(sources / "historical.jsonl")
    if history_frame.empty:
        historical, history_audit, history_rejected = gold.iloc[:0].copy(), [], gold_rejected.iloc[:0].copy()
    else:
        historical, history_audit, history_rejected = clean_events(history_frame, "historical")
    events = pd.concat([gold, historical], ignore_index=True)
    metadata = load_frame(sources / "metadata.jsonl")
    # Select newest snapshot deterministically. No engagement or crawl-query fields enter learner.
    selected = metadata.sort_values(["crawled_at", "origin", "_source_file"], kind="stable").drop_duplicates("video_id", keep="last").sort_values("video_id")
    catalogue = []
    for row in selected.to_dict("records"):
        catalogue.append({"video_id": row["video_id"], "title": row["title"],
                          "description": row["description"] if isinstance(row["description"], str) else "",
                          "tags": row["tags"] if isinstance(row["tags"], list) else [],
                          "duration_seconds": row["duration_seconds"], "channel_id": row["channel_id"],
                          "metadata_origin": row["origin"], "crawled_at": row["crawled_at"]})
    durations = pd.Series({r["video_id"]: r["duration_seconds"] for r in catalogue})
    if not events.video_id.isin(durations.index).all():
        raise ValueError("Có video thiếu metadata")
    expected = events.watch_duration_seconds / events.video_id.map(durations)
    events["completion_consistency_flag"] = (events.completion_rate - expected).abs().gt(0.001)
    # Flag rather than silently repair: labels already based on observed actions/completion.
    destination.mkdir(parents=True, exist_ok=True)
    write_json(destination / "catalogue.json", catalogue)
    save_frame(events, destination / "cleaned_events.csv")
    save_frame(pd.concat([gold_rejected, history_rejected], ignore_index=True), destination / "excluded_events.csv")
    save_frame(pd.DataFrame(gold_audit + history_audit), destination / "cleaning_audit.csv")
    scenarios = {}
    for scenario, day_mode in [("primary", False), ("historical_day", True)]:
        pairs, cutoffs = split_pairs(events, day_mode)
        scenario_path = destination / scenario
        id_columns = ["cohort", "origin", "user_id", "video_id", "event_id", "server_received_timestamp_ms"]
        for split in ["train", "validation", "test"]:
            subset = pairs.loc[pairs.split.eq(split)]
            save_frame(subset[id_columns], scenario_path / f"{split}_pairs.csv")
            save_frame(subset[id_columns + ["positive_label", "engagement_label"]], scenario_path / f"{split}_responses.csv")
        scenarios[scenario] = {"cutoffs": cutoffs, "pairs": len(pairs), "users": pairs.user_id.nunique(),
                              "counts": pairs.groupby(["cohort", "split"]).size().unstack(fill_value=0).to_dict("index"),
                              "repeat_pair_events_reserved": len(events) - len(pairs)}
    manifest = {"version": PROTOCOL["version"], "protocol_sha256": checksum(ROOT / "protocol.json"),
                "source_manifest_sha256": checksum(sources / "manifest.json"),
                "gold_events": len(gold), "historical_clean_events": len(historical),
                "all_clean_events": len(events), "catalogue_videos": len(catalogue),
                "completion_mismatches_flagged": int(events.completion_consistency_flag.sum()),
                "scenarios": scenarios, "historical_identity_verified": False,
                "files": {p.relative_to(destination).as_posix(): checksum(p) for p in destination.rglob("*") if p.is_file()}}
    verify_sources()
    write_json(destination / "manifest.json", manifest)
    REPORTS.mkdir(parents=True, exist_ok=True)
    write_json(REPORTS / "preprocessing_summary.json", {k: v for k, v in manifest.items() if k != "files"})
    return manifest


if __name__ == "__main__":
    result = build_preprocessing()
    print("Đã lưu preprocessing:", ARTIFACTS / "preprocessing")
    print("Sự kiện dẫn xuất:", result["all_clean_events"], "| Video:", result["catalogue_videos"])
    print("Số cặp lần đầu:", result["scenarios"]["primary"]["pairs"])
