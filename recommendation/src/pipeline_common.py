"""Shared paths, immutable artefacts, and read-only source snapshots."""
import hashlib
import io
import json
import subprocess
import tarfile
from pathlib import Path

import pandas as pd

from inspect_sources import PROJECT_ROOT, RECOMMENDATION_DIR, parse_jsonl, source_checksums, protected_file_checksums
from inspect_gold_provenance import locate_docker, collect_event_runs, encode_pair

ROOT = RECOMMENDATION_DIR
CONFIG = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
PROTOCOL = json.loads((ROOT / "protocol.json").read_text(encoding="utf-8"))
ARTIFACTS = ROOT / "artifacts" / PROTOCOL["version"]
REPORTS = ROOT / "reports" / PROTOCOL["version"]


def checksum(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    path.write_text(text, encoding="utf-8")


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def verify_sources():
    manifest = read_json(ROOT / "reports/source_manifest_v1.json")
    if source_checksums(CONFIG["sources"]) != manifest["source_file_sha256"]:
        raise ValueError("Nguồn đổi so với kiểm kê EDA")
    if protected_file_checksums() != manifest["protected_file_sha256"]:
        raise ValueError("Code collaborator đổi so với kiểm kê EDA")
    return manifest


def historical_records(directory):
    result = subprocess.run(
        ["git", "archive", CONFIG["sources"]["historical_commit"], directory],
        cwd=PROJECT_ROOT, capture_output=True, check=True,
    )
    rows = []
    with tarfile.open(fileobj=io.BytesIO(result.stdout), mode="r:") as archive:
        for member in archive:
            if member.isfile() and member.name.endswith(".jsonl"):
                text = archive.extractfile(member).read().decode("utf-8-sig")
                rows.extend({**record, "_source_file": member.name}
                            for _, record in parse_jsonl(text, member.name))
    return rows


def snapshot_sources():
    destination = ARTIFACTS / "sources"
    if (destination / "manifest.json").exists():
        manifest = read_json(destination / "manifest.json")
        for name, digest in manifest["files"].items():
            if checksum(destination / name) != digest:
                raise ValueError("Snapshot nguồn thay đổi: " + name)
        return manifest
    verify_sources()
    frozen_source = PROTOCOL.get("source_snapshot")
    if frozen_source:
        source = (ROOT / "artifacts" / frozen_source).resolve()
        if not source.is_relative_to((ROOT / "artifacts").resolve()) or source == destination.resolve():
            raise ValueError("Đường dẫn snapshot không hợp lệ")
        manifest = read_json(source / "manifest.json")
        destination.mkdir(parents=True, exist_ok=True)
        for name, digest in manifest["files"].items():
            original = source / name
            if checksum(original) != digest:
                raise ValueError("Snapshot gốc đã đổi: " + name)
            (destination / name).write_bytes(original.read_bytes())
        manifest = {**manifest, "reused_from": frozen_source}
        write_json(destination / "manifest.json", manifest)
        return manifest
    query = """BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
    SELECT row_to_json(t)::text FROM public.fact_interactions t ORDER BY event_id;
    COMMIT;"""
    result = subprocess.run(
        [locate_docker(), "compose", "exec", "-T", "postgres", "psql", "-X", "-q", "-A", "-t",
         "-U", "dataeng", "-d", "dwh", "-v", "ON_ERROR_STOP=1", "-c", query],
        cwd=PROJECT_ROOT, capture_output=True, text=True, encoding="utf-8", check=True,
    )
    gold = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    provenance = read_json(ROOT / "reports/gold_provenance_v1.json")
    identities = [{key: row[key] for key in ["event_id", "user_id", "video_id"]} for row in gold]
    digest = hashlib.sha256(json.dumps(identities, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    if digest != provenance["gold_snapshot_sha256"]:
        raise ValueError("Gold không khớp provenance đã kiểm tra")
    raw_root = PROJECT_ROOT / CONFIG["sources"]["current_root"]
    runs, pairs, _, _ = collect_event_runs(raw_root / "user_interaction_logs")
    for row in gold:
        matches = runs.get(row["event_id"], set())
        if len(matches) != 1:
            raise ValueError("Run không xác định duy nhất")
        run = next(iter(matches))
        if encode_pair(row) not in pairs[(run, row["event_id"])]:
            raise ValueError("Cặp Gold không khớp raw")
        row["cohort"] = run
    include_history = CONFIG["sources"].get("include_historical", True)
    history = historical_records(CONFIG["sources"]["historical_root"] + "/user_interaction_logs") if include_history else []
    metadata = []
    for path in sorted((raw_root / "youtube_shorts").glob("*.jsonl")):
        metadata.extend({**row, "origin": "current", "_source_file": path.relative_to(PROJECT_ROOT).as_posix()}
                        for _, row in parse_jsonl(path.read_text(encoding="utf-8-sig"), str(path)))
    if include_history:
        metadata.extend({**row, "origin": "historical"} for row in
                        historical_records(CONFIG["sources"]["historical_root"] + "/youtube_shorts"))
    destination.mkdir(parents=True, exist_ok=True)
    for name, rows in [("gold.jsonl", gold), ("historical.jsonl", history), ("metadata.jsonl", metadata)]:
        with (destination / name).open("w", encoding="utf-8") as file:
            for row in rows:
                file.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")
    verify_sources()
    manifest = {"gold_rows": len(gold), "historical_rows": len(history), "metadata_rows": len(metadata),
                "files": {p.name: checksum(p) for p in sorted(destination.glob("*.jsonl"))},
                "source_manifest_sha256": checksum(ROOT / "reports/source_manifest_v1.json"),
                "protected_files_unchanged": True}
    write_json(destination / "manifest.json", manifest)
    return manifest


def load_frame(path):
    return pd.DataFrame([json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()])


def save_frame(frame, path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8-sig")


def load_csv(path):
    return pd.read_csv(path, keep_default_na=False)
