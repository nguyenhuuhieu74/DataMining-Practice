# Chạy EDA KuaiRec

Notebook: 01_KuaiRec_EDA.ipynb.

## VS Code (khuyến nghị)
1. Mở C:/Users/Legion/Downloads/ThucHanh bằng VS Code.
2. Mở study/EDA/01_KuaiRec_EDA.ipynb.
3. Chọn Select Kernel → Python Environments → .venv.
   Interpreter: C:/Users/Legion/Downloads/ThucHanh/.venv/Scripts/python.exe.
4. Chạy từng ô bằng Shift + Enter. Kiểm tra sys.executable ở ô đầu.
5. Nếu thiếu hỗ trợ notebook trong VS Code, dùng extension Microsoft Python/Jupyter.
   Không cần cài JupyterLab vào Python, không cần Docker.

## Cần gì ngoài head/tail/describe?
shape, info, dtypes, isna, duplicated, nunique, value_counts, quantile,
groupby/agg, query/loc, sort_values, merge, explode, to_datetime, cut và biểu đồ.
Pandas xử lý bảng, NumPy xử lý vector và histogram, matplotlib vẽ biểu đồ.

## RAM và thời gian
Preview chỉ đọc 100.000 dòng đầu, không đại diện toàn tập.
RUN_FULL_SCAN=True: quét từng khối 250.000 dòng để thống kê đầy đủ hai ma trận.
RUN_PAIR_AUDIT=False: mặc định bỏ qua kiểm tra unique pairs/density/overlap cần thêm RAM.
Muốn audit: đặt True ở ô cấu hình rồi chạy lại từ đầu.
Không sửa CSV gốc. Không có ô pip install. Không ghi hay cài gì vào global.

## Khi gặp lỗi
ModuleNotFoundError: kiểm tra kernel có đúng .venv không trước khi cài gì.
FileNotFoundError: kiểm tra DATA_DIR ở ô cấu hình.
Kernel chạy lâu: đợi phần chunks, hoặc giảm CHUNK_SIZE; có thể tắt full scan để học phần đầu.
Join báo lỗi many-to-one: kiểm tra ID trùng trong metadata, không bỏ validate để che lỗi.
File caption cần lineterminator='\n' vì chứa dấu CR trong văn bản; notebook đã cấu hình.

Notebook bản đã chạy: 01_KuaiRec_EDA_da_chay.ipynb (nếu đã có).
Kết quả kiểm tra và phát hiện: KET_QUA_EDA.md (nếu đã có).
