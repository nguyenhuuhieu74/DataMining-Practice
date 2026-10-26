"""Inventory raw inputs without cleaning, merging identities or training."""

import hashlib
import io
import json
import subprocess
import tarfile
from itertools import combinations
from pathlib import Path, PurePosixPath

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RECOMMENDATION_DIR = Path(__file__).resolve().parents[1]


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))

def run_git(*arguments):
    result = subprocess.run(
        ["git", *arguments],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )

    return result.stdout

def source_checksums(sources):
    root = (PROJECT_ROOT / sources["current_root"]).resolve()
    historical_root = PurePosixPath(sources["historical_root"])
    if not root.is_relative_to(PROJECT_ROOT):
        raise ValueError("Nguồn hiện tại phải nằm trong thư mục dự án")
    if historical_root.is_absolute() or ".." in historical_root.parts:
        raise ValueError("Đường dẫn nguồn lịch sử không hợp lệ")
    files = list((root / "youtube_shorts").glob("*.jsonl"))
    for directory in (root / "user_interaction_logs").glob("run=*"):
        if directory.is_dir():
            files.extend(directory.rglob("*.jsonl"))
    result = {
        "current:" + path.relative_to(PROJECT_ROOT).as_posix():
        hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(files)
    }
    archive = subprocess.run(
        ["git", "archive", sources["historical_commit"], sources["historical_root"]],
        cwd=PROJECT_ROOT, capture_output=True, check=True,
    ).stdout
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as members:
        for member in members:
            if member.isfile() and member.name.endswith(".jsonl"):
                with members.extractfile(member) as file:
                    result["historical:" + member.name] = hashlib.sha256(file.read()).hexdigest()
    return dict(sorted(result.items()))


def protected_file_checksums():
    paths = run_git(
        "ls-files", "-z", "--", "raw", "etl", "dags", "tests",
        "Dockerfile.airflow", "docker-compose.yml", "requirements.txt",
        "requirements-airflow.txt", "README.md", "workflow.md",
        ".gitignore", ".dockerignore",
    ).split("\0")
    return {
        name: hashlib.sha256((PROJECT_ROOT / name).read_bytes()).hexdigest()
        for name in sorted(paths) if name
    }


def parse_jsonl(text, source):
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"JSON không hợp lệ: {source}, dòng {line_number}") from error
        if not isinstance(record, dict):
            raise ValueError(f"Bản ghi không hợp lệ: {source}, dòng {line_number}")
        yield line_number, record


def collect_video_ids(text, source):
    video_ids = set()

    for line_number, record in parse_jsonl(text, source):
        video_id = record.get("video_id")

        if not isinstance(video_id, str) or not video_id.strip():
            raise ValueError(
                f"Thiếu video_id: {source}, dòng {line_number}"
            )

        video_ids.add(video_id)

    return video_ids

def summarise_interactions(text_sources):
    event_count = 0
    missing_user_count = 0
    bot_event_count = 0

    user_ids = set()
    video_ids = set()
    event_ids = set()
    repeated_event_id_count = 0
    missing_event_id_count = 0
    missing_video_id_count = 0

    for source, text in text_sources:
        for _, record in parse_jsonl(text, source):
            event_count += 1

            user_id = record.get("user_id")
            video_id = record.get("video_id")
            event_id = record.get("event_id")

            is_bot = (
                record.get("app_version") == "bot_v1.0"
                or (
                    isinstance(user_id, str)
                    and user_id.startswith("bot_")
                )
            )

            if not isinstance(user_id, str) or not user_id.strip():
                missing_user_count += 1
            elif not is_bot:
                user_ids.add(user_id)

            if is_bot:
                bot_event_count += 1

            if isinstance(video_id, str) and video_id.strip():
                video_ids.add(video_id)
            else:
                missing_video_id_count += 1

            if isinstance(event_id, str) and event_id.strip():
                if event_id in event_ids:
                    repeated_event_id_count += 1

                event_ids.add(event_id)
            else:
                missing_event_id_count += 1

    return {
        "event_count": event_count,
        "user_id_count": len(user_ids),
        "video_id_count": len(video_ids),
        "missing_user_count": missing_user_count,
        "bot_event_count": bot_event_count,
        "repeated_event_id_count": repeated_event_id_count,
        "missing_event_id_count": missing_event_id_count,
        "missing_video_id_count": missing_video_id_count,
    }

def collect_interaction_ids(text_sources):
    user_ids, event_ids, video_ids = set(), set(), set()
    for source, text in text_sources:
        for _, record in parse_jsonl(text, source):
            user = record.get("user_id")
            bot = record.get("app_version") == "bot_v1.0" or (
                isinstance(user, str) and user.startswith("bot_")
            )
            if isinstance(user, str) and user.strip() and not bot:
                user_ids.add(user)
            event, video = record.get("event_id"), record.get("video_id")
            if isinstance(event, str) and event.strip():
                event_ids.add(event)
            if isinstance(video, str) and video.strip():
                video_ids.add(video)
    return {"users": user_ids, "events": event_ids, "videos": video_ids}


def save_reports(inventory, provenance):
    catalogue = inventory["catalogue"]
    lines = [
        "# Kiểm kê nguồn dữ liệu recommendation", "",
        "Bước này chỉ kiểm kê nguồn: chưa làm sạch, gộp user, chia tập hoặc train.", "",
        f"- Video hiện tại: {catalogue['current_video_count']}",
        f"- Video lịch sử: {catalogue['historical_video_count']}",
        f"- Video giao hai nguồn: {catalogue['overlap_count']}",
        f"- Tổng video khác nhau: {catalogue['union_count']}",
        f"- Tổng sự kiện thô: {inventory['total_raw_event_count']}", "",
        "| Nhóm | Sự kiện thô | User ID không phải bot | Video | Thiếu user | Bot | Event ID lặp thêm |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    groups = dict(inventory["current_interaction_runs"])
    groups["historical_unverified"] = inventory["historical_interactions"]
    for name, row in groups.items():
        lines.append(
            f"| {name} | {row['event_count']} | {row['user_id_count']} | "
            f"{row['video_id_count']} | {row['missing_user_count']} | "
            f"{row['bot_event_count']} | {row['repeated_event_id_count']} |"
        )
    lines += ["", "## Đối chiếu giữa các nhóm", "",
              "| Nhóm A | Nhóm B | User ID giống chuỗi | Event ID trùng |",
              "|---|---|---:|---:|"]
    for row in inventory["group_overlaps"]:
        lines.append(f"| {row['group_a']} | {row['group_b']} | {row['user_id_overlap']} | {row['event_id_overlap']} |")
    lines += ["", "## Kết luận và giới hạn", "",
        "- Các số ở trên là số lượng thô, không phải dữ liệu đủ điều kiện huấn luyện.",
        "- Một sự kiện có thể đồng thời thuộc nhiều loại lỗi; không cộng/trừ các số lỗi như các tập tách biệt.",
        "- Các run hiện tại khởi tạo hồ sơ riêng; phải phân biệt user theo run khi hợp nhất. Chưa thay đổi ID nào ở bước này.",
        "- Mã lịch sử khởi tạo hồ sơ trước vòng lặp nhiều ngày nhưng nối thêm vào file. Chưa xác minh ranh giới lần sinh trong dữ liệu đã commit.",
        "- User ID giống chuỗi không chứng minh cùng danh tính. Chưa kết luận tổng số người dùng ổn định.",
        "- Chỉ đọc JSONL local và commit Git; chưa truy vấn MinIO/PostgreSQL hoặc đối chiếu từng bản ghi trong Gold.",
        "- Checksum file nguồn và code/cấu hình được bảo vệ khớp trước/sau khi kiểm kê.",
        "- Bước tiếp theo: chốt ánh xạ danh tính user và phạm vi dùng dữ liệu lịch sử trước khi hợp nhất tương tác.", ""]
    payloads = {
        "source_inventory_v1.json": json.dumps(inventory, ensure_ascii=False, indent=2) + "\n",
        "source_manifest_v1.json": json.dumps(provenance, ensure_ascii=False, indent=2) + "\n",
        "source_inventory_v1.md": "\n".join(lines),
    }
    directory = RECOMMENDATION_DIR / "reports"
    # Validate every existing report before writing any missing report.
    for name, text in payloads.items():
        path = directory / name
        if path.exists() and path.read_text(encoding="utf-8") != text:
            raise FileExistsError(f"Kết quả thay đổi; cần phiên bản báo cáo mới: {path}")
    directory.mkdir(parents=True, exist_ok=True)
    for name, text in payloads.items():
        path = directory / name
        if not path.exists():
            with path.open("x", encoding="utf-8", newline="\n") as file:
                file.write(text)
    return directory

def main():
    configuration = read_json(RECOMMENDATION_DIR / "config.json")
    sources = configuration["sources"]
    initial_source_checksums = source_checksums(sources)
    initial_protected_checksums = protected_file_checksums()
    configuration_checksum = hashlib.sha256((RECOMMENDATION_DIR / "config.json").read_bytes()).hexdigest()
    inspector_checksum = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    resolved_commit = run_git("rev-parse", "--verify", f"{sources['historical_commit']}^{{commit}}").strip()

    current_root = PROJECT_ROOT / sources["current_root"]
    catalogue_directory = current_root / "youtube_shorts"
    interaction_directory = current_root / "user_interaction_logs"

    for directory in [catalogue_directory, interaction_directory]:
        if not directory.is_dir():
            raise FileNotFoundError(
                f"Không tìm thấy thư mục nguồn: {directory}"
            )

    print("Thư mục dự án:", PROJECT_ROOT)
    print("Nguồn video hiện tại:", catalogue_directory)
    print("Nguồn tương tác hiện tại:", interaction_directory)
    print("Commit lịch sử:", sources["historical_commit"])
    print("Đường dẫn trong commit:", sources["historical_root"])
    catalogue_files = sorted(catalogue_directory.glob("*.jsonl"))
    if not catalogue_files:
        raise FileNotFoundError("Không tìm thấy metadata hiện tại")

    interaction_runs = sorted(
        directory
        for directory in interaction_directory.glob("run=*")
        if directory.is_dir()
    )
    if not interaction_runs:
        raise FileNotFoundError("Không tìm thấy thư mục run tương tác hiện tại")

    historical_listing = run_git(
        "ls-tree",
        "-r",
        "--name-only",
        sources["historical_commit"],
        "--",
        sources["historical_root"],
    )

    historical_files = [
        path
        for path in historical_listing.splitlines()
        if path.endswith(".jsonl")
    ]

    print("\n--- Kiểm kê file nguồn ---")
    print("Số file metadata hiện tại:", len(catalogue_files))
    print("Số thư mục chạy tương tác hiện tại:", len(interaction_runs))
    print("Số file JSONL trong lịch sử:", len(historical_files))

    print("\nCác lần chạy tương tác hiện tại:")
    for directory in interaction_runs:
        print(" ", directory.name)

    current_video_ids = set()

    for path in catalogue_files:
        text = path.read_text(encoding="utf-8-sig")
        current_video_ids.update(
            collect_video_ids(text, str(path))
        )

    historical_catalogue_files = [
        path
        for path in historical_files
        if "/youtube_shorts/" in path
    ]

    if not historical_catalogue_files:
        raise FileNotFoundError(
            "Không tìm thấy metadata video trong commit lịch sử"
        )

    historical_video_ids = set()

    for path in historical_catalogue_files:
        text = run_git(
            "show",
            f"{sources['historical_commit']}:{path}",
        )

        historical_video_ids.update(
            collect_video_ids(text, path)
        )

    print("\n--- Kiểm kê video khác nhau ---")
    print("Video hiện tại:", len(current_video_ids))
    print("Video lịch sử:", len(historical_video_ids))
    print(
        "Video có trong cả hai nguồn:",
        len(current_video_ids & historical_video_ids),
    )
    print(
        "Tổng video khác nhau:",
        len(current_video_ids | historical_video_ids),
    )


    print("\n--- Tương tác hiện tại theo từng lần chạy ---")

    current_interaction_summaries = {}
    interaction_ids = {}
    for directory in interaction_runs:
        interaction_files = sorted(directory.rglob("*.jsonl"))
        if not interaction_files:
            raise FileNotFoundError(f"Không có tương tác trong {directory}")

        text_sources = [
            (
                str(path),
                path.read_text(encoding="utf-8-sig"),
            )
            for path in interaction_files
        ]

        summary = summarise_interactions(text_sources)
        summary["file_count"] = len(interaction_files)
        current_interaction_summaries[directory.name] = summary
        interaction_ids[directory.name] = collect_interaction_ids(text_sources)

        print(f"\nLần chạy: {directory.name}")
        print("Số file:", len(interaction_files))
        print("Số sự kiện thô:", summary["event_count"])
        print("Số user ID khác nhau, không phải bot:", summary["user_id_count"])
        print("Số video có tương tác:", summary["video_id_count"])
        print("Sự kiện thiếu user ID:", summary["missing_user_count"])
        print("Sự kiện đánh dấu bot:", summary["bot_event_count"])
        print("Event ID lặp thêm:", summary["repeated_event_id_count"])

    historical_interaction_files = [
        path
        for path in historical_files
        if "/user_interaction_logs/" in path
    ]

    if not historical_interaction_files:
        raise FileNotFoundError(
            "Không tìm thấy tương tác trong commit lịch sử"
        )

    historical_text_sources = [
        (
            path,
            run_git(
                "show",
                f"{sources['historical_commit']}:{path}",
            ),
        )
        for path in historical_interaction_files
    ]

    historical_summary = summarise_interactions(
        historical_text_sources
    )
    historical_summary["file_count"] = len(historical_interaction_files)
    interaction_ids["historical_unverified"] = collect_interaction_ids(historical_text_sources)

    print("\n--- Tương tác lịch sử của collaborator ---")
    print("Số file:", len(historical_interaction_files))
    print("Số sự kiện thô:", historical_summary["event_count"])
    print(
        "Số user ID khác nhau, không phải bot:",
        historical_summary["user_id_count"],
    )
    print(
        "Số video có tương tác:",
        historical_summary["video_id_count"],
    )
    print(
        "Sự kiện thiếu user ID:",
        historical_summary["missing_user_count"],
    )
    print(
        "Sự kiện đánh dấu bot:",
        historical_summary["bot_event_count"],
    )
    print(
        "Event ID lặp thêm:",
        historical_summary["repeated_event_id_count"],
    )
    catalogue_union = current_video_ids | historical_video_ids
    overlaps = []
    for first, second in combinations(sorted(interaction_ids), 2):
        overlaps.append({
            "group_a": first, "group_b": second,
            "user_id_overlap": len(interaction_ids[first]["users"] & interaction_ids[second]["users"]),
            "event_id_overlap": len(interaction_ids[first]["events"] & interaction_ids[second]["events"]),
        })
    inventory = {
        "stage": "raw_source_inventory", "historical_commit": resolved_commit,
        "file_counts": {
            "current_catalogue": len(catalogue_files),
            "current_interactions": sum(row["file_count"] for row in current_interaction_summaries.values()),
            "historical_catalogue": len(historical_catalogue_files),
            "historical_interactions": len(historical_interaction_files),
        },
        "catalogue": {
            "current_video_count": len(current_video_ids),
            "historical_video_count": len(historical_video_ids),
            "overlap_count": len(current_video_ids & historical_video_ids),
            "union_count": len(catalogue_union),
        },
        "current_interaction_runs": current_interaction_summaries,
        "historical_interactions": historical_summary,
        "total_raw_event_count": historical_summary["event_count"] + sum(
            row["event_count"] for row in current_interaction_summaries.values()),
        "group_overlaps": overlaps,
        "unknown_catalogue_video_counts": {
            name: len(ids["videos"] - catalogue_union) for name, ids in interaction_ids.items()
        },
        "identity_policy": {
            "current": "separate_users_by_run",
            "historical": "generation_boundaries_unverified",
            "merge_users_across_sources": False,
            "total_stable_user_count": None,
        },
        "cleaning_applied": False, "training_performed": False,
        "live_minio_or_postgresql_queried": False,
        "source_checksums_match_before_after": True,
        "protected_checksums_match_before_after": True,
    }
    if source_checksums(sources) != initial_source_checksums:
        raise ValueError("File nguồn thay đổi khi kiểm kê; hãy chạy lại khi nguồn ổn định")
    if protected_file_checksums() != initial_protected_checksums:
        raise ValueError("Code/cấu hình được bảo vệ đã thay đổi khi kiểm kê")
    if hashlib.sha256((RECOMMENDATION_DIR / "config.json").read_bytes()).hexdigest() != configuration_checksum:
        raise ValueError("Cấu hình kiểm kê thay đổi trong lúc chạy")
    if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != inspector_checksum:
        raise ValueError("Script kiểm kê thay đổi trong lúc chạy")
    provenance = {
        "historical_commit": resolved_commit,
        "configuration_sha256": configuration_checksum,
        "inspector_sha256": inspector_checksum,
        "source_file_sha256": initial_source_checksums,
        "protected_file_sha256": initial_protected_checksums,
        "scope": "local_and_git_sources_not_live_database",
    }
    reports = save_reports(inventory, provenance)
    print("\nTổng sự kiện thô:", inventory["total_raw_event_count"])
    print("File nguồn và code được bảo vệ: checksum trước/sau khớp.")
    print("Chưa xác nhận tổng user ổn định; chưa làm sạch hoặc train.")
    print("Đã lưu báo cáo kiểm kê:", reports / "source_inventory_v1.md")
    print("Manifest nguồn:", reports / "source_manifest_v1.json")


if __name__ == "__main__":
    main()
