# Demo đề xuất YouTube Shorts

## Chạy

Từ terminal VS Code tại thư mục gốc repository:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\recommendation\src\web.py
```

Mở http://127.0.0.1:8501. 
Snapshot đã có sẵn rồi nên không cần Docker hay train lại.

## API và tích hợp app (chưa xong)

API đọc, nạp mô hình đã lưu. Instagram clone có thể gọi API này khi nối app sau.
Trang hiện tại tạm chỉ dùng demo, chưa tích hợp tài khoản, lượt xem mới hoặc cập nhật mô hình.
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
Compose chỉ mount artifacts/reports để đọc, không thay Docker gốc. 
