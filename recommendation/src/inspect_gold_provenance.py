"""Trace Gold event IDs to local runs using a read-only database transaction."""

import argparse
import hashlib
import json
import re
import shutil
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

from inspect_sources import (
    PROJECT_ROOT,
    RECOMMENDATION_DIR,
    parse_jsonl,
    protected_file_checksums,
    read_json,
)


GOLD_QUERY = """
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SELECT json_build_object(
    'event_id', event_id,
    'user_id', user_id,
    'video_id', video_id
)::text
FROM public.fact_interactions
ORDER BY event_id;
COMMIT;
"""


def checksum(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def encode_pair(record):
    return json.dumps(
        [record.get("user_id"), record.get("video_id")],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def locate_docker():
    candidates = [
        shutil.which("docker.exe"),
        shutil.which("docker"),
        Path.home() / "AppData/Local/Programs/DockerDesktop/resources/bin/docker.exe",
        Path("C:/Program Files/Docker/Docker/resources/bin/docker.exe"),
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return str(candidate)
    raise FileNotFoundError("Không tìm thấy Docker CLI trên máy")


def list_interaction_files(directory):
    files = []
    for run_directory in sorted(directory.glob("run=*")):
        if not run_directory.is_dir():
            continue
        for path in sorted(run_directory.rglob("*.jsonl")):
            if not any(part.startswith(".") for part in path.relative_to(run_directory).parts):
                files.append((run_directory.name, path))
    if not files:
        raise FileNotFoundError(f"Không có file tương tác run=* trong {directory}")
    return files


def collect_event_runs(directory):
    event_runs = defaultdict(set)
    event_pairs = defaultdict(set)
    file_checksums = {}
    raw_counts = Counter()
    for run_name, path in list_interaction_files(directory):
        content = path.read_bytes()
        file_checksums[path.relative_to(PROJECT_ROOT).as_posix()] = hashlib.sha256(content).hexdigest()
        for _, record in parse_jsonl(content.decode("utf-8-sig"), str(path)):
            raw_counts[run_name] += 1
            event_id = record.get("event_id")
            if isinstance(event_id, str) and event_id.strip():
                event_runs[event_id].add(run_name)
                event_pairs[(run_name, event_id)].add(encode_pair(record))
    return event_runs, event_pairs, file_checksums, dict(sorted(raw_counts.items()))


def read_gold_events():
    result = subprocess.run(
        [locate_docker(), "compose", "exec", "-T", "postgres", "psql",
         "-X", "-q", "-A", "-t", "-U", "dataeng", "-d", "dwh",
         "-v", "ON_ERROR_STOP=1", "-c", GOLD_QUERY],
        cwd=PROJECT_ROOT,
        capture_output=True,
        encoding="utf-8",
        errors="strict",
    )
    if result.returncode:
        raise RuntimeError("Không đọc được Gold:\n" + result.stderr.strip())
    records = [record for _, record in parse_jsonl(result.stdout, "Gold/public.fact_interactions")]
    if not records:
        raise ValueError("Gold không có sự kiện; chưa đủ dữ liệu để kiểm tra provenance")
    if any(not isinstance(row.get("event_id"), str) or not row["event_id"] for row in records):
        raise ValueError("Gold trả về event_id không hợp lệ")
    return records


def analyse_provenance(gold_events, event_runs, event_pairs):
    run_counts = Counter()
    run_users = defaultdict(set)
    run_videos = defaultdict(set)
    missing, ambiguous, mismatched = [], [], []
    for row in gold_events:
        event_id = row["event_id"]
        matching_runs = event_runs.get(event_id, set())
        if not matching_runs:
            missing.append(event_id)
        elif len(matching_runs) > 1:
            ambiguous.append(event_id)
        else:
            run_name = next(iter(matching_runs))
            run_counts[run_name] += 1
            if encode_pair(row) not in event_pairs[(run_name, event_id)]:
                mismatched.append(event_id)
            if isinstance(row.get("user_id"), str) and row["user_id"].strip():
                run_users[run_name].add(row["user_id"])
            if isinstance(row.get("video_id"), str) and row["video_id"].strip():
                run_videos[run_name].add(row["video_id"])
    identifiers = [row["event_id"] for row in gold_events]
    repeated = len(identifiers) - len(set(identifiers))
    passes = not (missing or ambiguous or mismatched or repeated)
    return {
        "status": "PASS" if passes else "REVIEW_REQUIRED",
        "gold_event_count": len(gold_events),
        "distinct_gold_event_ids": len(set(identifiers)),
        "repeated_gold_event_id_rows": repeated,
        "uniquely_mapped_event_count": sum(run_counts.values()),
        "missing_source_count": len(missing),
        "ambiguous_source_count": len(ambiguous),
        "user_video_mismatch_count": len(mismatched),
        "examples": {
            "missing_source": missing[:5],
            "ambiguous_source": ambiguous[:5],
            "user_video_mismatch": mismatched[:5],
        },
        "runs": {
            name: {"gold_event_count": run_counts[name],
                   "gold_user_id_count": len(run_users[name]),
                   "gold_video_id_count": len(run_videos[name])}
            for name in sorted(run_counts)
        },
        "distinct_run_user_pairs": sum(len(values) for values in run_users.values()),
        "identity_mapping_applied": False,
        "historical_identity_status": "generation_boundaries_unverified_not_resolved_by_this_check",
    }


def save_reports(report, version):
    lines = [
        "# Đối chiếu provenance giữa Gold và raw hiện tại", "",
        f"Kết quả: **{report['status']}**", "",
        f"- Sự kiện Gold đã đọc: {report['gold_event_count']}",
        f"- Tìm được duy nhất một run: {report['uniquely_mapped_event_count']}",
        f"- Không tìm thấy nguồn: {report['missing_source_count']}",
        f"- Event ID thuộc nhiều run: {report['ambiguous_source_count']}",
        f"- User/video của Gold không khớp bất kỳ bản ghi cùng event ID trong run: {report['user_video_mismatch_count']}",
        f"- Dòng lặp event ID trong Gold: {report['repeated_gold_event_id_rows']}", "",
        "| Run nguồn | Sự kiện Gold | User ID trong Gold | Video trong Gold |",
        "|---|---:|---:|---:|",
    ]
    for name, values in report["runs"].items():
        lines.append(f"| {name} | {values['gold_event_count']} | {values['gold_user_id_count']} | {values['gold_video_id_count']} |")
    lines += ["", "## Ý nghĩa và giới hạn", "",
        f"- Có {report['distinct_run_user_pairs']} cặp (run, user_id) khác nhau trong các sự kiện ánh xạ được.",
        "- Đây là các danh tính theo phạm vi lần sinh; không phải số người thật. Chưa sửa user_id hoặc xuất bộ dữ liệu học.",
        "- Đối chiếu event_id và cặp user_id/video_id giúp xác nhận nguồn; chưa kiểm tra sự bằng nhau của mọi trường sự kiện.",
        "- Đọc Gold trong transaction REPEATABLE READ, READ ONLY; chỉ đọc raw bằng Python. Không gọi ETL.",
        "- Code/cấu hình collaborator và file raw được đối chiếu checksum trước/sau. Không ghi dữ liệu Bronze/Silver/Gold.",
        "- Phạm vi kiểm tra là Gold hiện tại và raw của các thư mục run=* hiện tại. Không suy ra dữ liệu lịch sử đã được nhập đủ vào Gold.",
        "- Ranh giới lần sinh user trong dữ liệu lịch sử vẫn chưa xác minh.",
        "- Bước sau: chốt ánh xạ run + user_id trong phần dẫn xuất riêng, trước khi hợp nhất dữ liệu hoặc làm EDA mô hình.", ""]
    directory = RECOMMENDATION_DIR / "reports"
    payloads = {
        f"gold_provenance_{version}.json": json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        f"gold_provenance_{version}.md": "\n".join(lines),
    }
    for name, content in payloads.items():
        path = directory / name
        if path.exists() and path.read_text(encoding="utf-8") != content:
            raise FileExistsError(f"Kết quả đổi; chọn --report-version khác để giữ bản cũ: {path}")
    directory.mkdir(parents=True, exist_ok=True)
    for name, content in payloads.items():
        path = directory / name
        if not path.exists():
            with path.open("x", encoding="utf-8", newline="\n") as file:
                file.write(content)
    return directory / f"gold_provenance_{version}.md"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-version", default="v1")
    arguments = parser.parse_args()
    if not re.fullmatch(r"v[1-9][0-9]*", arguments.report_version):
        parser.error("Phiên bản phải có dạng v1, v2, ...")
    configuration_path = RECOMMENDATION_DIR / "config.json"
    configuration_hash = checksum(configuration_path)
    inspector_hash = checksum(Path(__file__))
    helper_hash = checksum(Path(__file__).with_name("inspect_sources.py"))
    protected_before = protected_file_checksums()
    configuration = read_json(configuration_path)
    root = (PROJECT_ROOT / configuration["sources"]["current_root"]).resolve()
    if not root.is_relative_to(PROJECT_ROOT):
        raise ValueError("Nguồn hiện tại phải nằm trong repo")
    directory = root / "user_interaction_logs"
    event_runs, event_pairs, source_hashes, raw_counts = collect_event_runs(directory)
    gold_events = read_gold_events()
    report = analyse_provenance(gold_events, event_runs, event_pairs)
    sources_after = {
        path.relative_to(PROJECT_ROOT).as_posix(): checksum(path)
        for _, path in list_interaction_files(directory)
    }
    if source_hashes != sources_after or protected_file_checksums() != protected_before:
        raise ValueError("Nguồn hoặc code được bảo vệ thay đổi trong lúc kiểm tra; chưa lưu báo cáo")
    if (checksum(configuration_path) != configuration_hash
            or checksum(Path(__file__)) != inspector_hash
            or checksum(Path(__file__).with_name("inspect_sources.py")) != helper_hash):
        raise ValueError("Cấu hình/code inspector thay đổi trong lúc kiểm tra")
    report.update({
        "stage": "gold_provenance_inspection_not_preprocessing",
        "database": "dwh", "table": "public.fact_interactions",
        "read_only_transaction": True,
        "raw_event_counts": raw_counts,
        "gold_snapshot_sha256": hashlib.sha256(json.dumps(gold_events, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest(),
        "configuration_sha256": configuration_hash,
        "inspector_sha256": inspector_hash,
        "helper_sha256": helper_hash,
        "source_file_sha256": source_hashes,
        "protected_file_sha256": protected_before,
        "source_and_protected_files_unchanged": True,
        "cleaning_applied": False, "training_performed": False,
    })
    report_path = save_reports(report, arguments.report_version)
    print("Kết quả:", report["status"])
    print("Sự kiện Gold:", report["gold_event_count"])
    print("Ánh xạ duy nhất một run:", report["uniquely_mapped_event_count"])
    for name, values in report["runs"].items():
        print(f"  {name}: {values['gold_event_count']} sự kiện; {values['gold_user_id_count']} user ID")
    print("Thiếu nguồn:", report["missing_source_count"])
    print("Nhiều nguồn:", report["ambiguous_source_count"])
    print("Sai cặp user/video:", report["user_video_mismatch_count"])
    print("Cặp (run, user_id):", report["distinct_run_user_pairs"])
    print("Báo cáo:", report_path)
    if report["status"] != "PASS":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
