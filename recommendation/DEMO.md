# Demo live: crawl → ETL → recommendation

Chạy từng bước bằng tay trong **cùng một terminal PowerShell của VS Code**, tại
thư mục gốc repository. Chỉ sang bước tiếp theo khi bước hiện tại thành công.

Lượt demo tạo raw, MinIO, PostgreSQL và model riêng. Không nạp lịch sử hoặc model
cũ. Metadata được crawl từ YouTube; log tương tác user vẫn là dữ liệu mô phỏng.

## Chuẩn bị

Bật Docker Desktop. Máy cần Python 3.14, Git và Internet.
Nếu chưa có môi trường Python, chạy một lần tại thư mục repo:

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r .\recommendation\requirements.txt
```

Máy đã có `.venv` và thư viện thì bỏ qua hai lệnh này.

## 1. Tạo lượt demo mới

Lệnh này chỉ tạo cấu hình và thư mục, chưa crawl hoặc train:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\recommendation\run_live.py --prepare-only --users 100 --max-results 10
```

Chọn lượt vừa tạo và gán các biến cho terminal:

```powershell
$liveRun = (Get-ChildItem .\recommendation\artifacts\live_runs -Directory | Sort-Object Name -Descending | Select-Object -First 1).FullName
$liveInfo = Get-Content (Join-Path $liveRun 'run.json') -Raw | ConvertFrom-Json
$liveId = $liveInfo.id
$env:COMPOSE_FILE = Join-Path $liveRun 'compose.json'
$env:COMPOSE_PROJECT_NAME = $liveInfo.project
$env:RECOMMENDATION_PROJECT_ROOT = (Get-Location).Path
$env:RECOMMENDATION_RUN_DIR = Join-Path $liveRun 'recommendation'
```

`--users 100` sinh 100 hồ sơ mô phỏng. `--max-results 10` giới hạn số kết quả mỗi
truy vấn crawl, không bảo đảm tổng số video cuối cùng. Mỗi lần chạy prepare tạo
một lượt mới; không chạy lại prepare để tiếp tục lượt đang làm.

## 2. Bật Airflow, MinIO và PostgreSQL

```powershell
docker compose up -d --build airflow-webserver airflow-scheduler
```

Kiểm tra:

```powershell
docker compose exec -T airflow-scheduler airflow dags list
```

Chờ thấy **live_crawl** và **live_etl**. Nếu chưa thấy, đợi rồi chạy lại lệnh kiểm tra.
Hai DAG không chạy theo lịch tự động; cần trigger từng DAG.

Xem địa chỉ giao diện của lượt này:

```powershell
"Airflow: http://127.0.0.1:$($liveInfo.ports.airflow)"
"MinIO:   http://127.0.0.1:$($liveInfo.ports.minio)"
"Demo:    http://127.0.0.1:$($liveInfo.ports.web)"
```

| Giao diện | Cổng ưu tiên | Tài khoản |
| --- | --- | --- |
| Airflow | 8081 | admin / admin |
| MinIO | 9101 | minioadmin / minioadmin123 |
| Recommendation | 8502 | Không đăng nhập |

Nếu cổng bận, chương trình chọn cổng khác; dùng địa chỉ thực tế ở trên.

## 3. Crawl metadata và sinh log mới

```powershell
docker compose exec -T airflow-scheduler airflow dags trigger live_crawl --run-id "$liveId"
```

Kiểm tra trạng thái, có thể gọi nhiều lần:

```powershell
docker compose exec -T airflow-scheduler airflow dags list-runs -d live_crawl
```

Trong Airflow, mở **live_crawl → Graph** để xem hai task `crawl_shorts` và
`synthesize_logs`. Chọn task → **Logs** để xem tiến độ.

**Chờ run có trạng thái success rồi mới chạy bước 4.** Thời gian crawl phụ thuộc
YouTube và mạng; không bảo đảm xong trong vài phút. Nếu task failed, xem Logs và
dừng ở bước đó. Không trigger lại cùng run ID khi run vẫn đang chạy.

## 4. ETL: Bronze → Silver → Gold

```powershell
docker compose exec -T airflow-scheduler airflow dags trigger live_etl --run-id "$liveId"
```

Kiểm tra:

```powershell
docker compose exec -T airflow-scheduler airflow dags list-runs -d live_etl
```

Chờ **success**. Trong Airflow, DAG **live_etl** gồm upload raw lên Bronze,
chuyển video và tương tác sang Silver bằng Polars, rồi nạp Gold PostgreSQL.
Mở MinIO để xem bucket **bronze** và **silver**.

## 5. Xem dữ liệu Gold

```powershell
docker compose exec -T postgres psql -U dataeng -d dwh -c "SELECT 'dim_video' AS table_name, COUNT(*) FROM dim_video UNION ALL SELECT 'dim_user', COUNT(*) FROM dim_user UNION ALL SELECT 'fact_interactions', COUNT(*) FROM fact_interactions UNION ALL SELECT 'agg_video_daily', COUNT(*) FROM agg_video_daily;"
```

Đây là database của lượt demo mới. Số dòng không nhất thiết giống báo cáo cũ.

## 6. Kiểm tra nguồn mới

Đối chiếu event Gold với raw của lượt mới, tạo manifest nguồn:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\recommendation\run_live.py --resume "$liveRun" --inspect-only
```

Lệnh chỉ kiểm tra nguồn; chưa train. Kết quả đối chiếu phải PASS.

## 7. Preprocessing

```powershell
.\.venv\Scripts\python.exe -X utf8 "$env:RECOMMENDATION_RUN_DIR\src\preprocess.py"
```

Tạo dữ liệu dẫn xuất, nhãn và các tập train/validation/test theo thời gian 70/15/15.
Dữ liệu nguồn và các bảng Gold không bị sửa bởi bước này.

## 8. Train bốn phương pháp

```powershell
.\.venv\Scripts\python.exe -X utf8 "$env:RECOMMENDATION_RUN_DIR\src\train.py"
```

Huấn luyện **Popularity, Content-based, Collaborative Filtering và Hybrid**.
Cấu hình và chính sách xếp hạng được chọn bằng validation, model được lưu trước
khi Evaluate đọc nhãn test. Không chỉnh theo test để ép Hybrid thắng.

## 9. Đánh giá và hiện bảng kết quả

```powershell
.\.venv\Scripts\python.exe -X utf8 "$env:RECOMMENDATION_RUN_DIR\src\evaluate.py"
```

Terminal hiện Precision@10, Recall@10, F1@10, NDCG@10 và Coverage của bốn phương pháp.
Kết quả mới có thể khác bảng trong báo cáo. Dữ liệu mô phỏng không chứng minh
hiệu quả trên người dùng thật.

## 10. Xem đề xuất trong terminal

Liệt kê một số ID user có trong model:

```powershell
.\.venv\Scripts\python.exe -X utf8 "$env:RECOMMENDATION_RUN_DIR\src\recommend.py" --list-users
```

Đề xuất Top-10 bằng Hybrid:

```powershell
.\.venv\Scripts\python.exe -X utf8 "$env:RECOMMENDATION_RUN_DIR\src\recommend.py" --user-id "run=${liveId}:usr_00000" --model Hybrid --top-k 10
```

Nếu user ví dụ không có trong model, dùng một ID đầy đủ từ `--list-users`.
Đổi `--model` thành `Popularity`, `ContentBased` hoặc `CollaborativeFiltering`
để xem phương pháp khác. Điểm xếp hạng không phải xác suất thích video.

## 11. Mở web demo

```powershell
.\.venv\Scripts\python.exe -X utf8 .\recommendation\run_live.py --resume "$liveRun" --serve-only
```

Mở URL in trong terminal, thường là **http://127.0.0.1:8502**. Giữ terminal chạy.

Khi trình bày:

1. Chọn user, xem tóm tắt quan tâm và lịch sử train.
2. Xem Top-10 và chuyển giữa bốn phương pháp.
3. Mở **Đánh giá mô hình** để xem chỉ số và độ phủ.
4. Mở **Dữ liệu & quy trình** để xem quy mô của chính lượt chạy mới.

Web đọc model vừa train của lượt này. Các lượt bấm trên web chưa cập nhật model.

## 12. Kết thúc demo

Nhấn **Ctrl+C** để dừng web, rồi:

```powershell
docker compose stop
```

Dữ liệu, model và Docker volume của lượt mới vẫn được giữ. Đóng terminal để bỏ
các biến COMPOSE/RECOMMENDATION tạm thời. Code và dữ liệu cũ của collaborator
không bị ghi đè.

## Mở lại một lượt đã hoàn thành

Mở terminal tại thư mục repo. Chọn lượt gần nhất, không chạy lại prepare:

```powershell
$liveRun = (Get-ChildItem .\recommendation\artifacts\live_runs -Directory | Sort-Object Name -Descending | Select-Object -First 1).FullName
.\.venv\Scripts\python.exe -X utf8 .\recommendation\run_live.py --resume "$liveRun" --serve-only
```

Lệnh chỉ mở web từ model đã lưu. Nếu muốn một lượt khác, gán `$liveRun` bằng
đường dẫn thư mục lượt đó. Muốn tiếp tục các bước thủ công sau khi đóng terminal,
gán lại các biến ở bước 1, bỏ qua lệnh prepare.

## Chia sẻ code qua GitHub

Có thể push code và tài liệu để collaborator làm cùng quy trình trên máy họ.
Sau khi clone/pull repo, họ cài môi trường theo mục Chuẩn bị rồi chạy từ bước 1.
Không cần nhận dữ liệu hoặc model cũ để chạy demo live mới.

`recommendation/artifacts/` đã được gitignore: raw mới, model, báo cáo live và
cấu hình Docker sinh riêng trên máy không được push. Docker volume cũng nằm
ngoài repository. Các máy có thể thu được video và kết quả khác nhau.
