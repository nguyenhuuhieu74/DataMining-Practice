# Recommendation: bốn phương pháp

Luồng hiện tại: **Popularity, ContentBased, CollaborativeFiltering, Hybrid**.
Đọc `reports/four_methods_v1/REPORT.md` và mở bốn notebook theo thứ tự
`EDA.ipynb`, `Preprocessing.ipynb`, `Train.ipynb`, `Evaluate.ipynb`.
Bảng và biểu đồ nằm trực tiếp trong notebook. API/web demo đã có; đọc `DEMO.md`
để xem kịch bản trình bày, API và cách đóng gói sang máy khác.

## Mở demo web

Trên Windows, từ thư mục gốc của repo, tạo môi trường Python 3.14 và cài thư viện:

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r .\recommendation\requirements.txt
```

Phiên bản Python và các thư viện trên là môi trường đã dùng để xây snapshot.
Máy đã có `.venv` và các thư viện thì không cần tạo lại. Để chạy demo trên máy
khác, cần snapshot trong `recommendation/artifacts/` hoặc gói demo riêng; các
file này không được tự tải khi clone repo.

```powershell
.\.venv\Scripts\python.exe -X utf8 .\recommendation\src\web.py
```

Mở **http://127.0.0.1:8501**. Dùng snapshot hiện có nên không cần Docker hay train lại.
Chọn run, user và phương pháp; xem tóm tắt quan tâm từ train, lịch sử, danh sách
đề xuất, so sánh phương pháp, tải CSV và đọc bảng đánh giá.

## Cấu trúc

```text
recommendation/
  src/
    inspect_sources.py          # Kiểm kê raw/Git và checksum nguồn
    inspect_gold_provenance.py  # Đối chiếu Gold với run nguồn, chỉ đọc
    pipeline_common.py         # Đường dẫn, snapshot, đọc/ghi dùng chung
    preprocess.py              # Quy tắc làm sạch, nhãn, chia theo thời gian
    recommenders.py            # Thuật toán content/CF và feature
    ranking_metrics.py         # Ranking, metric và chính sách coverage
    train.py                   # Chọn cấu hình bốn nhóm trên validation
    evaluate.py                # Evaluate, audit, bootstrap, các đối chứng
    report.py                  # Báo cáo và notebook cho ba giai đoạn
    recommend.py               # Snapshot service dùng chung cho CLI và web
    web.py                     # HTTP API cục bộ và đóng gói demo
  web/index.html               # Giao diện tiếng Việt, không cần npm/build
  notebooks/                   # EDA / Preprocessing / Train / Evaluate
  reports/four_methods_v1/     # Kết quả hiện hành
  artifacts/                  # Dữ liệu/model lớn, gitignore
```

## Chạy từ thư mục gốc repository

```powershell
python -X utf8 .\recommendation\src\preprocess.py
python -X utf8 .\recommendation\src\train.py
python -X utf8 .\recommendation\src\evaluate.py --all
python -X utf8 .\recommendation\src\recommend.py --list-users
python -X utf8 .\recommendation\src\recommend.py --user-id "run=e1d06d3cf6587c44:usr_00000" --model Hybrid --top-k 10
```

`evaluate.py --all` chạy thêm kiểm tra danh tính lịch sử theo ngày, nhãn like/share và
tráo nhãn train; giữ cấu hình đã chọn. Không có flag `--all` thì chạy đánh giá chính,
audit và bootstrap. Báo cáo được cập nhật tự động. Snapshot tồn tại được xác minh rồi
dùng lại, không ghi đè model để dò điểm test.

Các cấu hình nội bộ (ItemCF/MF; TF-IDF/SVD logistic) chọn trên validation, mỗi nhóm chỉ
xuất một phương pháp. Coverage là policy từng nhóm; báo cáo chính đúng bốn dòng,
bảng xếp hạng gốc cũng đúng bốn dòng, không thêm phương pháp Selected/Balanced/Random.
Học từng cohort riêng vì mô phỏng user/video được khởi tạo lại mỗi run.

## Nguồn và giới hạn

Lần chạy này dùng lại snapshot nguồn `artifacts/official_v1/sources/` đã kiểm tra,
chạy lại preprocessing vào `artifacts/four_methods_v1/`; không cần Docker để đọc
snapshot có sẵn. EDA cũng đọc bản Gold này, không sửa hay truy vấn lại DB.
Không chạy ETL gốc để tránh thay bảng collaborator. Raw và commit lịch sử vẫn cần
để đối chiếu checksum; máy mới cần được cấp dữ liệu/snapshot đúng manifest.

Test đã được xem ở các lần trước. Đây là lần chạy lại cùng dữ liệu với giao thức
tinh gọn, không phải bằng chứng xác nhận độc lập mới. Chất lượng đánh giá là positive
ghi nhận trong log, không phải xác suất thích thật trên mọi ứng viên. Không hứa
Hybrid vượt Popularity. Danh tính lịch sử chưa xác minh, metadata mang tính hồi cứu.

Demo pipeline chỉ hỗ trợ user có sẵn, loại toàn bộ cặp đã gặp ở cả ba tập. Có xếp hạng
gốc và áp policy đã chốt cho lô user hiện tại; pool demo khác Evaluate nên coverage có
thể khác bảng test. Tóm tắt quan tâm chỉ dùng phản hồi tích cực trong train;
không tham gia tính điểm hoặc thay đổi mô hình. Các thí nghiệm cũ đã được gỡ bỏ.

Code, protocol, notebook, requirements, manifest nguồn và kết quả chính có thể push;
`artifacts/` cần sao lưu/phân phối riêng. Báo cáo kiểm tra phụ và bản lưu cũ không
được push theo cấu hình hiện tại. Requirements chỉ thuộc phần recommendation.
