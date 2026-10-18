import glob
import json
import logging
import os
import random
import re
from datetime import datetime, timezone
from typing import Dict, List
import yt_dlp

# Cấu hình Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# 1. MỞ RỘNG TẬP SEARCH TOPICS LÊN 10 DANH MỤC CỦA SOCIAL MEDIA
SEARCH_TOPICS = {
    "tech": [
        "python programming #shorts",
        "data engineering #shorts",
        "ai technology #shorts",
        "coding tips #shorts",
        "web development #shorts",
        "devops #shorts",
        "machine learning #shorts",
        "cybersecurity #shorts",
        "cloud computing #shorts",
        "software engineering #shorts",
    ],
    "gaming": [
        "minecraft tips #shorts",
        "league of legends play #shorts",
        "valorant highlights #shorts",
        "gaming moments #shorts",
        "roblox funny #shorts",
        "cs2 highlights #shorts",
        "mobile legends #shorts",
        "elden ring gameplay #shorts",
        "fortnite clips #shorts",
        "gta 5 stunts #shorts",
    ],
    "food": [
        "quick recipe #shorts",
        "street food #shorts",
        "cooking hacks #shorts",
        "delicious food #shorts",
        "baking tips #shorts",
        "mukbang #shorts",
        "asian cuisine #shorts",
        "fast food review #shorts",
        "coffee art #shorts",
        "dessert recipe #shorts",
    ],
    "education": [
        "science facts #shorts",
        "english tips #shorts",
        "history in 60s #shorts",
        "study hacks #shorts",
        "math tricks #shorts",
        "psychology facts #shorts",
        "space exploration #shorts",
        "geography trivia #shorts",
        "philosophy thoughts #shorts",
        "physics experiments #shorts",
    ],
    "music": [
        "guitar cover #shorts",
        "piano tutorial #shorts",
        "drum solo #shorts",
        "vocal practice #shorts",
        "lofi beats #shorts",
        "music production #shorts",
        "song cover #shorts",
        "hiphop dance #shorts",
        "dj mix #shorts",
        "busking performance #shorts",
    ],
}

# Các phụ tố biến đổi để sinh Dynamic Query
DYNAMIC_MODIFIERS = [
    "2026",
    "viral",
    "best",
    "tutorial",
    "tips",
    "guide",
    "review",
    "hacks",
    "moments",
    "tricks",
    "latest",
    "trending",
]


def generate_dynamic_query(seed_query: str) -> str:
    """Ghép từ khóa ngẫu nhiên vào query gốc để đa dạng hóa kết quả từ YouTube API."""
    if random.random() < 0.6:  # 60% cơ hội biến đổi query
        modifier = random.choice(DYNAMIC_MODIFIERS)
        if random.random() < 0.5:
            return f"{modifier} {seed_query}"
        else:
            return f"{seed_query} {modifier}"
    return seed_query


def load_existing_video_ids(output_dir: str) -> set:
    """Đọc toàn bộ video_id từ các file .jsonl cũ trong Data Lake để loại trừ trùng lặp tuyệt đối."""
    seen_ids = set()
    jsonl_files = glob.glob(os.path.join(output_dir, "*.jsonl"))

    for file_path in jsonl_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        record = json.loads(line)
                        if "video_id" in record:
                            seen_ids.add(record["video_id"])
        except Exception as e:
            logging.warning(f"Unable to read from {file_path}: {e}")

    logging.info(
        f"Loaded {len(seen_ids)} video_id from history"
    )
    return seen_ids


def extract_tags(entry: Dict) -> List[str]:
    """Trích xuất và hợp nhất tags từ Metadata + Hashtag (#) trong Title & Description."""
    text_to_search = f"{entry.get('title') or ''} {entry.get('description') or ''}"
    hashtags = re.findall(r"#([^\s#.,!?:;\"'()\[\]{}]+)", text_to_search)

    tags_list = []
    seen = set()
    raw_candidates = list(entry.get("tags") or []) + hashtags

    for value in raw_candidates:
        if not value:
            continue
        clean_tag = value.strip().lstrip("#")
        tag_key = clean_tag.casefold()

        if clean_tag and tag_key not in seen:
            seen.add(tag_key)
            tags_list.append(clean_tag)

    return tags_list


def crawl_youtube_shorts(
    topics_dict: Dict[str, List[str]],
    max_results_per_query: int = 50,
    output_dir: str = "./data_lake/raw/youtube_shorts",
):
    """Crawl YouTube Shorts Metadata không trùng lặp tuyệt đối giữa các đợt chạy."""

    os.makedirs(output_dir, exist_ok=True)

    # Nạp toàn bộ ID lịch sử để lọc trùng tuyệt đối
    seen_video_ids = load_existing_video_ids(output_dir)

    now_utc = datetime.now(timezone.utc)
    current_time_str = now_utc.strftime("%Y%m%d%H")
    filename = f"{current_time_str}.jsonl"
    output_filepath = os.path.join(output_dir, filename)

    ydl_opts = {
        "extract_flat": True,
        "skip_download": True,
        "quiet": True,
        "no_warnings": True,
    }

    crawled_count = 0

    with open(output_filepath, "a", encoding="utf-8") as f_out, yt_dlp.YoutubeDL(
        ydl_opts
    ) as ydl, yt_dlp.YoutubeDL({**ydl_opts, "extract_flat": False}) as detail_ydl:

        categories = list(topics_dict.keys())
        random.shuffle(categories)

        for category in categories:
            seed_queries = topics_dict[category].copy()
            random.shuffle(seed_queries)

            logging.info(f"Crawling category: {category}")

            for seed_query in seed_queries:
                dynamic_query = generate_dynamic_query(seed_query)
                search_url = f"ytsearch{max_results_per_query}:{dynamic_query}"
                logging.info(f"Querying: '{dynamic_query}'")

                try:
                    search_results = ydl.extract_info(
                        search_url, download=False
                    )

                    if not search_results or "entries" not in search_results:
                        continue

                    for entry in search_results["entries"]:
                        if not entry:
                            continue

                        video_id = entry.get("id")
                        duration = entry.get("duration")

                        # Bỏ qua nếu video ĐÃ TỪNG xuất hiện trong lịch sử hoặc không đúng độ dài Short
                        if (
                            not video_id
                            or video_id in seen_video_ids
                            or not duration
                            or duration > 60
                            or duration < 5
                        ):
                            continue

                        shorts_url = f"https://www.youtube.com/shorts/{video_id}"

                        try:
                            details = detail_ydl.extract_info(
                                shorts_url, download=False
                            )
                            if details:
                                entry = {
                                    **entry,
                                    **{
                                        k: v
                                        for k, v in details.items()
                                        if v is not None
                                    },
                                }
                        except Exception as exc:
                            logging.warning(
                                f"Error crawling video {video_id}: {exc}"
                            )

                        # Đánh dấu ID đã xuất hiện để không cào lại trong các vòng lặp tiếp theo
                        seen_video_ids.add(video_id)

                        raw_record = {
                            "video_id": video_id,
                            "video_url": shorts_url,
                            "title": entry.get("title", ""),
                            "description": entry.get("description", ""),
                            "channel_id": entry.get("channel_id", ""),
                            "channel_title": entry.get("uploader", ""),
                            "duration_seconds": float(duration),
                            "categories": [category],
                            "tags": extract_tags(entry),
                            "view_count_at_crawl": entry.get("view_count", 0),
                            "like_count_at_crawl": entry.get("like_count", 0),
                            "comment_count_at_crawl": entry.get(
                                "comment_count", 0
                            ),
                            "upload_date": entry.get("upload_date", ""),
                            "thumbnail_url": entry.get("thumbnail")
                            or entry.get("url", ""),
                            "crawled_at": datetime.now(timezone.utc).isoformat(),
                        }

                        f_out.write(
                            json.dumps(raw_record, ensure_ascii=False) + "\n"
                        )
                        crawled_count += 1

                except Exception as e:
                    logging.error(f"Error while crawling '{dynamic_query}': {str(e)}")

    logging.info(
        f"Completed crawl for {filename}: total {crawled_count}"
    )


if __name__ == "__main__":
    crawl_youtube_shorts(
        topics_dict=SEARCH_TOPICS,
        max_results_per_query=50,
        output_dir="./data_lake/raw/youtube_shorts",
    )