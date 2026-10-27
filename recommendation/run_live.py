"""Run a new crawl-to-recommendation demo in its own Docker project and directories."""
import argparse
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import uuid


PROJECT = Path(__file__).resolve().parents[1]
RECOMMENDATION = PROJECT / "recommendation"


def save_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def available_port(start):
    for port in range(start, start + 100):
        with socket.socket() as connection:
            try:
                connection.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"Không có cổng trống từ {start}")


def execute(arguments, environment, capture=False):
    result = subprocess.run([str(value) for value in arguments], cwd=PROJECT,
                            env=environment, check=True, text=True,
                            encoding="utf-8", errors="replace", capture_output=capture)
    return result.stdout if capture else None


def mount(source, target, read_only=False):
    return dict(type="bind", source=str(source.resolve()), target=target, read_only=read_only)


def prepare(users, max_results):
    identifier = "live_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    directory = RECOMMENDATION / "artifacts" / "live_runs" / identifier
    root = directory / "recommendation"
    root.mkdir(parents=True, exist_ok=False)
    for name in ["src", "web"]:
        shutil.copytree(RECOMMENDATION / name, root / name, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    for name in ["requirements.txt", "README.md", "DEMO.md"]:
        shutil.copy2(RECOMMENDATION / name, root / name)
    for name in ["reports", "notebooks"]:
        (root / name).mkdir()
    data = directory / "data_lake"
    for name in ["youtube_shorts", "user_interaction_logs"]:
        (data / "raw" / name).mkdir(parents=True)
    configuration = json.loads((RECOMMENDATION / "config.json").read_text(encoding="utf-8"))
    configuration["sources"] = {"current_root": (data / "raw").relative_to(PROJECT).as_posix(), "include_historical": False}
    save_json(root / "config.json", configuration)
    protocol = json.loads((RECOMMENDATION / "protocol.json").read_text(encoding="utf-8"))
    protocol.pop("source_snapshot", None)
    protocol.update(version=identifier, live_run=True, old_test_previously_inspected=False,
                    purpose="New crawl and synthetic interactions; train and evaluate this run only",
                    retrospective_status="New run; validation selects configuration before first test evaluation")
    save_json(root / "protocol.json", protocol)
    ports = {"airflow": available_port(8081), "minio_api": available_port(9002),
             "minio": available_port(9101), "web": available_port(8502)}
    project_name = identifier.replace("_", "-")
    environment = {
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
        "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "false",
        "AIRFLOW__WEBSERVER__SECRET_KEY": uuid.uuid4().hex,
        "AIRFLOW__CORE__DEFAULT_TIMEZONE": "UTC",
        "MINIO_ENDPOINT": "minio:9000", "MINIO_KEY": "minioadmin", "MINIO_SECRET": "minioadmin123",
        "PG_HOST": "postgres", "PG_PORT": "5432", "PG_DB": "dwh", "PG_USER": "dataeng", "PG_PASSWORD": "dataeng123",
        "BRONZE_YOUTUBE_DIR": "/opt/airflow/data_lake/raw/youtube_shorts",
        "BRONZE_INTERACTIONS_DIR": "/opt/airflow/data_lake/raw/user_interaction_logs",
        "LIVE_USERS": str(users), "LIVE_MAX_RESULTS": str(max_results), "LIVE_ID": identifier,
    }
    dag_directory = directory / "dags"
    dag_directory.mkdir()
    (dag_directory / "live_pipeline.py").write_text(DAG_SOURCE, encoding="utf-8")
    common = dict(image="datamining-airflow:2.10.0",
                  build=dict(context=str(PROJECT), dockerfile="Dockerfile.airflow"),
                  environment=environment,
                  volumes=[mount(PROJECT / "raw", "/opt/airflow/raw", True),
                           mount(PROJECT / "etl", "/opt/airflow/etl", True),
                           mount(dag_directory, "/opt/airflow/dags", True),
                           mount(data, "/opt/airflow/data_lake"), "airflow_logs:/opt/airflow/logs"])
    dependencies = {"airflow-init": {"condition": "service_completed_successfully"},
                    "postgres": {"condition": "service_healthy"}, "minio": {"condition": "service_started"}}
    database_health = dict(test=["CMD-SHELL", "pg_isready -U airflow -d airflow"], interval="5s", timeout="5s", retries=30)
    services = {
        "airflow-postgres": dict(image="postgres:16-alpine", environment=dict(POSTGRES_USER="airflow", POSTGRES_PASSWORD="airflow", POSTGRES_DB="airflow"), volumes=["airflow_postgres_data:/var/lib/postgresql/data"], healthcheck=database_health),
        "postgres": dict(image="postgres:18-alpine", environment=dict(POSTGRES_USER="dataeng", POSTGRES_PASSWORD="dataeng123", POSTGRES_DB="dwh"), volumes=["postgres_data:/var/lib/postgresql"], healthcheck={**database_health, "test": ["CMD-SHELL", "pg_isready -U dataeng -d dwh"]}),
        "minio": dict(image="cgr.dev/chainguard/minio:latest", environment=dict(MINIO_ROOT_USER="minioadmin", MINIO_ROOT_PASSWORD="minioadmin123"), command=["server", "/data", "--console-address", ":9001"], ports=[f"127.0.0.1:{ports['minio_api']}:9000", f"127.0.0.1:{ports['minio']}:9001"], volumes=["minio_data:/data"]),
        "airflow-init": {**common, "environment": {**environment, "_AIRFLOW_DB_MIGRATE": "true", "_AIRFLOW_WWW_USER_CREATE": "true", "_AIRFLOW_WWW_USER_USERNAME": "admin", "_AIRFLOW_WWW_USER_PASSWORD": "admin"}, "command": ["version"], "depends_on": {"airflow-postgres": {"condition": "service_healthy"}}},
        "airflow-webserver": {**common, "command": ["webserver"], "ports": [f"127.0.0.1:{ports['airflow']}:8080"], "depends_on": dependencies},
        "airflow-scheduler": {**common, "command": ["scheduler"], "depends_on": dependencies},
    }
    save_json(directory / "compose.json", dict(services=services, volumes={key: {} for key in ["airflow_postgres_data", "postgres_data", "minio_data", "airflow_logs"]}))
    details = dict(id=identifier, project=project_name, ports=ports, users=users, max_results=max_results, stage="prepared")
    save_json(directory / "run.json", details)
    return directory, details


def run_environment(directory, details):
    environment = os.environ.copy()
    environment.update(RECOMMENDATION_PROJECT_ROOT=str(PROJECT), RECOMMENDATION_RUN_DIR=str(directory / "recommendation"),
                       COMPOSE_FILE=str(directory / "compose.json"), COMPOSE_PROJECT_NAME=details["project"],
                       PYTHONUTF8="1", PYTHONIOENCODING="utf-8")
    return environment


def wait_for_ingestion(docker, environment, details, dag_id):
    command = [docker, "compose", "exec", "-T", "airflow-scheduler", "airflow", "dags"]
    deadline = time.monotonic() + 300
    while True:
        try:
            dags = json.loads(execute(command + ["list", "-o", "json"], environment, True))
            if any(row["dag_id"] == dag_id for row in dags):
                break
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            if time.monotonic() > deadline:
                raise RuntimeError("Airflow chưa nạp DAG. Xem: docker compose logs airflow-scheduler")
            time.sleep(5)
        if time.monotonic() > deadline:
            raise RuntimeError(f"Airflow chưa nạp DAG {dag_id}; xem log scheduler")
        time.sleep(5)
    def read_runs():
        output = execute(command + ["list-runs", "-d", dag_id, "-o", "json"], environment, True).strip()
        return [] if output == "No data found" else json.loads(output)
    runs = read_runs()
    if not any(row["run_id"] == details["id"] for row in runs):
        execute(command + ["trigger", dag_id, "--run-id", details["id"]], environment)
    print(f"Airflow đang chạy {dag_id}. Xem task/log trong giao diện.", flush=True)
    deadline = time.monotonic() + 8 * 3600
    previous = None
    while time.monotonic() < deadline:
        runs = read_runs()
        state = next((row["state"] for row in runs if row["run_id"] == details["id"]), "queued")
        if state != previous:
            print("DAG:", state, flush=True)
            previous = state
        if state == "success":
            return
        if state == "failed":
            raise RuntimeError("DAG thất bại. Mở task màu đỏ → Logs trong Airflow; chưa train model.")
        time.sleep(10)
    raise TimeoutError("DAG chưa xong sau 8 giờ; không tự chuyển sang train")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--users", type=int, default=100)
    parser.add_argument("--max-results", type=int, default=10)
    parser.add_argument("--resume", type=Path, help="Tiếp tục hoặc mở lại đúng lượt chạy này")
    parser.add_argument("--serve-only", action="store_true")
    parser.add_argument("--stop", action="store_true")
    parser.add_argument("--prepare-only", action="store_true", help="Chỉ tạo cấu hình để kiểm tra, chưa chạy Docker/crawl")
    parser.add_argument("--inspect-only", action="store_true", help="Sau ETL: kiểm kê/đối chiếu Gold mới, chưa preprocessing/train")
    arguments = parser.parse_args()
    if arguments.users < 20 or arguments.max_results < 2:
        parser.error("Dùng ít nhất 20 user và max-results >=2 để có dữ liệu đánh giá")
    if (arguments.serve_only or arguments.stop or arguments.inspect_only) and not arguments.resume:
        parser.error("--serve-only/--stop/--inspect-only cần --resume <thư mục lượt chạy>")
    if arguments.resume:
        directory = arguments.resume.resolve()
        if not directory.is_relative_to((RECOMMENDATION / "artifacts/live_runs").resolve()):
            parser.error("Thư mục resume phải ở recommendation/artifacts/live_runs")
        details = json.loads((directory / "run.json").read_text(encoding="utf-8"))
    else:
        directory, details = prepare(arguments.users, arguments.max_results)
    environment = run_environment(directory, details)
    print("LƯỢT CHẠY:", directory, flush=True)
    print("Airflow:", f"http://127.0.0.1:{details['ports']['airflow']}  (admin / admin)", flush=True)
    print("MinIO:", f"http://127.0.0.1:{details['ports']['minio']}  (minioadmin / minioadmin123)", flush=True)
    print("Demo:", f"http://127.0.0.1:{details['ports']['web']}", flush=True)
    print(f'Mở lại: python -X utf8 recommendation/run_live.py --resume "{directory}" --serve-only', flush=True)
    if arguments.prepare_only:
        return
    docker = shutil.which("docker") or shutil.which("docker.exe")
    if not docker and not arguments.serve_only:
        raise FileNotFoundError("Chưa tìm thấy Docker CLI; mở Docker Desktop rồi terminal mới")
    if arguments.stop:
        execute([docker, "compose", "stop"], environment)
        return
    root = directory / "recommendation"
    if arguments.inspect_only:
        inspect_new_sources(root, environment)
        details['stage'] = 'ingested'
        save_json(directory / 'run.json', details)
        return
    if not arguments.serve_only:
        for package in ["numpy", "pandas", "scipy", "sklearn"]:
            if importlib.util.find_spec(package) is None:
                raise RuntimeError("Thiếu thư viện. Chạy: python -m pip install -r recommendation/requirements.txt")
        execute([docker, "info", "--format", "{{.ServerVersion}}"], environment)
        if details["stage"] == "prepared":
            print("[1/4] Khởi động kho dữ liệu và Airflow riêng", flush=True)
            execute([docker, "compose", "up", "-d", "--build", "airflow-webserver", "airflow-scheduler"], environment)
            wait_for_ingestion(docker, environment, details, 'live_crawl')
            wait_for_ingestion(docker, environment, details, 'live_etl')
            details["stage"] = "ingested"
            save_json(directory / "run.json", details)
        if details["stage"] == "ingested":
            print("[2/4] Kiểm tra nguồn mới và preprocessing", flush=True)
            inspect_new_sources(root, environment)
            execute([sys.executable, "-X", "utf8", root / "src/preprocess.py"], environment)
            print("[3/4] Train bốn phương pháp; chọn cấu hình bằng validation", flush=True)
            execute([sys.executable, "-X", "utf8", root / "src/train.py"], environment)
            print("[4/4] Đánh giá test và mở demo của lượt chạy mới", flush=True)
            execute([sys.executable, "-X", "utf8", root / "src/evaluate.py"], environment)
            details["stage"] = "evaluated"
            save_json(directory / "run.json", details)
    execute([sys.executable, "-X", "utf8", root / "src/web.py", "--port", str(details["ports"]["web"])], environment)


def inspect_new_sources(root, environment):
    manifest_command = "from inspect_sources import *; c=read_json(RECOMMENDATION_DIR/'config.json'); p=RECOMMENDATION_DIR/'reports/source_manifest_v1.json'; p.write_text(json.dumps({'source_file_sha256':source_checksums(c['sources']),'protected_file_sha256':protected_file_checksums()},indent=2),encoding='utf-8')"
    execute([sys.executable, "-X", "utf8", "-c", "import sys; sys.path.insert(0," + repr(str(root / "src")) + ");" + manifest_command], environment)
    execute([sys.executable, "-X", "utf8", root / "src/inspect_gold_provenance.py"], environment)


DAG_SOURCE = '''"""Fresh manual crawl and ETL DAGs; original collaborator functions are unchanged."""
import os
import runpy
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pendulum
from airflow.decorators import dag, task
sys.path.insert(0, "/opt/airflow")

@dag(dag_id="live_crawl", schedule=None, start_date=pendulum.datetime(2026,1,1,tz="UTC"),
     catchup=False, max_active_runs=1, default_args={"retries":0}, tags=["live", "fresh"])
def live_crawl():
    @task(execution_timeout=timedelta(hours=6))
    def crawl_shorts():
        code = runpy.run_path("/opt/airflow/raw/crawl-short.py")
        result = code["crawl_youtube_shorts"](topics_dict=code["SEARCH_TOPICS"],
            max_results_per_query=int(os.environ["LIVE_MAX_RESULTS"]),
            output_dir=os.environ["BRONZE_YOUTUBE_DIR"], batch_name=os.environ["LIVE_ID"])
        if not Path(result).is_file() or not Path(result).stat().st_size:
            raise ValueError("Crawl không có bản ghi; chưa chuyển sang sinh log")
        return result

    @task(execution_timeout=timedelta(hours=2))
    def synthesize_logs(metadata_path):
        code = runpy.run_path("/opt/airflow/raw/user-logs.py")
        destination = Path(os.environ["BRONZE_INTERACTIONS_DIR"]) / ("run=" + os.environ["LIVE_ID"])
        destination.mkdir(parents=True, exist_ok=False)
        count = code["run_user_simulation_pipeline"](items_dir=os.environ["BRONZE_YOUTUBE_DIR"],
            output_base_dir=str(destination), start_date=datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0),
            simulation_days=1, num_users=int(os.environ["LIVE_USERS"]))
        if not count:
            raise ValueError("Không có log tương tác")
        return count

    synthesize_logs(crawl_shorts())

@dag(dag_id="live_etl", schedule=None, start_date=pendulum.datetime(2026,1,1,tz="UTC"),
     catchup=False, max_active_runs=1, default_args={"retries":0}, tags=["live", "fresh"])
def live_etl():
    @task
    def upload_raw():
        if not any(Path(os.environ["BRONZE_YOUTUBE_DIR"]).glob("*.jsonl")) or not any(Path(os.environ["BRONZE_INTERACTIONS_DIR"]).rglob("*.jsonl")):
            raise ValueError("Chạy live_crawl thành công trước khi trigger live_etl")
        from etl.upload_to_bronze_minio import sync_local_raw_to_bronze
        return sync_local_raw_to_bronze(youtube_dir=os.environ["BRONZE_YOUTUBE_DIR"], logs_dir=os.environ["BRONZE_INTERACTIONS_DIR"])

    @task
    def silver_videos():
        from etl.bronze_to_silver import etl_youtube_bronze_to_silver
        return etl_youtube_bronze_to_silver(youtube_dir=os.environ["BRONZE_YOUTUBE_DIR"])

    @task
    def silver_interactions():
        from etl.bronze_to_silver import etl_interactions_bronze_to_silver
        return etl_interactions_bronze_to_silver(logs_dir=os.environ["BRONZE_INTERACTIONS_DIR"])

    @task
    def gold(video_count, interaction_count):
        if not video_count or not interaction_count:
            raise ValueError("Silver rỗng; chưa nạp Gold")
        from etl.silver_to_gold import load_silver_to_gold
        return load_silver_to_gold()

    uploaded = upload_raw()
    videos, interactions = silver_videos(), silver_interactions()
    uploaded >> [videos, interactions]
    gold(videos, interactions)

live_crawl()
live_etl()
'''


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nĐã dừng lệnh. Dịch vụ và dữ liệu lượt chạy được giữ lại; dùng --resume để tiếp tục.")
    except (subprocess.CalledProcessError, RuntimeError, OSError, ValueError) as error:
        print("\nDừng tại lỗi:", error, file=sys.stderr)
        raise SystemExit(1)
