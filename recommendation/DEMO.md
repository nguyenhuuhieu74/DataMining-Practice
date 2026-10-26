# Demo đề xuất YouTube Shorts

## Chạy

Từ terminal VS Code tại thư mục gốc repository:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\recommendation\src\web.py
```

Mở http://127.0.0.1:8501. Dừng bằng Ctrl+C. Nếu cổng bận, thêm --port 8502.
Snapshot đã có nên không cần Docker hay train lại.

## Trình bày

1. Chọn nhóm/run và user trong Demo kết quả.
2. Xem User này quan tâm gì?: từ khóa và ba video train có phản hồi tích cực.
   Số cạnh từ khóa là số video tích cực chứa từ đó. Đây là dấu hiệu suy ra,
   không phải sở thích tự khai hoặc xác suất. Không dùng nhãn validation/test.
3. Đối chiếu danh sách đề xuất; đổi phương pháp hoặc dùng So sánh phương pháp.
4. Video đã tương tác được loại; các video còn lại là ứng viên. Điểm dùng xếp hạng.
   Cân bằng hỗ trợ Top-10; xếp hạng gốc hỗ trợ Top-5/10/20 trên GUI.
5. Xem lịch sử train để kiểm tra bằng chứng; Tải CSV để xuất danh sách.
6. Đánh giá mô hình hiển thị precision, recall, F1, NDCG và coverage đã đo trên test.
   Ba run hiện tại là kết quả chính; lịch sử collaborator được báo cáo riêng.

Độ phủ trên trang đề xuất gộp danh sách của toàn bộ user đủ điều kiện trong nhóm,
chia số video khác nhau được đề xuất cho kích thước catalogue của nhóm.
Pool demo loại lịch sử cả ba tập nên có thể khác pool đánh giá test.

## API và tích hợp app

API chỉ đọc, nạp mô hình đã lưu. Instagram clone có thể gọi API này khi nối app sau.
Trang hiện tại dùng trình diễn; chưa tích hợp tài khoản, lượt xem mới hoặc cập nhật mô hình.
Chỉ phục vụ user/video trong snapshot. Tương tác nguồn là mô phỏng, metadata crawl từ YouTube.

| Endpoint GET | Nội dung |
| --- | --- |
| /api/health | Trạng thái |
| /api/status | Phương pháp, nhóm và quy mô |
| /api/users?cohort=run%3De1d06d3cf6587c44 | User trong run |
| /api/recommendations?user_id=run%3De1d06d3cf6587c44%3Ausr_00000&method=Hybrid | Top-10 |
| /api/history?user_id=run%3De1d06d3cf6587c44%3Ausr_00000 | Lịch sử và tóm tắt train |
| /api/compare?user_id=run%3De1d06d3cf6587c44%3Ausr_00000 | Bốn phương pháp |
| /api/metrics?scope=primary&mode=balanced | Đánh giá đã lưu |
| /api/export?user_id=run%3De1d06d3cf6587c44%3Ausr_00000 | CSV |

method: Popularity, ContentBased, CollaborativeFiltering, Hybrid.
mode đề xuất: quality hoặc balanced (mặc định). Balanced chỉ Top-10;
quality hỗ trợ top_k từ 1 đến 50. scope metric: primary hoặc historical;
mode metric: raw hoặc balanced. Chỉ có nguồn pipeline.

## Đóng gói

```powershell
.\.venv\Scripts\python.exe -X utf8 .\recommendation\src\web.py --export-bundle .\recommendation\artifacts\demo_bundle.zip
```

Bundle không ghi đè file đã có. ZIP chứa code, giao diện, mô hình, dữ liệu tiền xử lý
và đánh giá; không chứa raw, .git, .venv hoặc bí mật đăng nhập. Chuyển ZIP riêng,
giải nén vào một thư mục trống trên máy khác rồi chạy các lệnh sau tại thư mục đó:

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r .\recommendation\requirements.txt
.\.venv\Scripts\python.exe -X utf8 .\recommendation\src\web.py
```

Mở http://127.0.0.1:8501. Máy khác cần có Python 3.14 và tải được các thư viện.
Không giải nén chồng vào repo đang có nếu chưa kiểm tra file trùng. Gói phục vụ demo;
chạy lại toàn bộ EDA/train cần thêm nguồn raw và lịch sử Git đúng manifest.
artifacts/ bị gitignore nên không tự có khi clone repo.

Compose riêng tùy chọn: docker compose -f recommendation/compose.yaml up --build.
Compose chỉ mount artifacts/reports để đọc, không thay Docker gốc. Python/API đã được
kiểm thử cục bộ; kiểm tra cú pháp Compose không xác nhận image đã chạy.
