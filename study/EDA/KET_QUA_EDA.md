# Kết quả EDA KuaiRec trên CSV hiện có

Đã chạy 18 ô code bằng Python trong ThucHanh/.venv, không cài thêm thư viện.
Thời gian xử lý: 46.9 giây. Biểu đồ: 5.

## Toàn bộ hai ma trận

| Matrix | Dòng | Users | Videos | Quan tâm mạnh (>2) | Max ratio | Lệch công thức |
|---|---:|---:|---:|---:|---:|---:|
| small_matrix | 4,676,570 | 1,411 | 3,327 | 4.64% | 571.52 | 1,209 |
| big_matrix | 12,530,806 | 7,176 | 10,728 | 7.47% | 573.46 | 81 |

Tỷ lệ quan tâm mạnh tính trên ratio hữu hạn và không âm; đây là proxy, không phải lượt bấm like.

## Chất lượng dữ liệu và exposure

- small_matrix: thiếu ở core columns = 181,992; duration <= 0 = 0; ratio âm = 0; ratio không hữu hạn = 0. Top 10% video chiếm 10.05% bản ghi.
- big_matrix: thiếu ở core columns = 0; duration <= 0 = 0; ratio âm = 0; ratio không hữu hạn = 0. Top 10% video chiếm 40.72% bản ghi.

## Audit cặp user-video

- small_matrix: 4,676,570 cặp unique; 0 dòng lặp cặp; density = 99.620%.
- big_matrix: 10,300,969 cặp unique; 2,229,837 dòng lặp cặp; density = 13.381%.

Cặp overlap giữa hai ma trận: 0.

## Cách đọc kết quả

- Ratio cực lớn cần xem cùng play_duration và video_duration; không tự động xóa vì có thể phản ánh xem lại.
- Bảng preview chỉ dùng 100.000 dòng đầu, không dùng để suy ra phân bố toàn tập.
- Bảng theo category dùng toàn bộ big_matrix, trọng số là số bản ghi hợp lệ.
- Thống kê EDA trên toàn bộ dữ liệu không được đem làm feature train nếu chứa nhãn của validation/test.
- Quét chất lượng toàn tập áp dụng cho core columns; thời gian chuỗi time được parse ở preview.
- Đây là EDA, chưa huấn luyện hay so sánh recommendation model.

## Phát hiện đã kiểm chứng trên dữ liệu của bạn

- Small: 4.676.570 dòng và cặp unique; density 99,620%; nhãn `watch_ratio > 2` chiếm 4,64%.
- Big: 12.530.806 dòng, 10.300.969 cặp unique. Có 2.229.837 dòng ngoài lần xuất hiện đầu của cặp.
  Density theo unique pairs là 13,381%. Con số 16,3% công bố tương ứng mật độ theo số bản ghi;
  cần nêu rõ định nghĩa khi so sánh. Không tự xóa các lượt lặp: có thể là tương tác ở thời điểm khác.
- Hai ma trận không có cặp user–video giao nhau (audit toàn bộ).
- Small có 181.992 dòng thiếu `date` (3,89%). Các ví dụ kiểm tra cũng thiếu `time` và `timestamp`.
  Biểu đồ ngày loại những dòng thiếu ngày, nên không bao phủ toàn bộ tương tác.
  Không tự gán ngày để làm temporal split. Muốn biết số thiếu `time`/`timestamp` toàn tập, quét thêm hai cột đó.
- Có 1.209 dòng ở small và 81 dòng ở big lệch công thức ratio tại `rtol=1e-5`, `atol=1e-6`.
  Kiểm tra độ lệch tuyệt đối/tương đối và tài liệu trước khi quyết định dùng nhãn có sẵn hay tính lại.
- Top 10% video theo exposure chiếm 40,72% bản ghi big, so với 10,05% ở small.
- Caption cần `lineterminator='\n'` để giữ đúng 10.728 video. Đổi sang Python parser đơn thuần
  tạo 4 dòng giả; kiểm tra ID giúp nhận ra vấn đề.

Các số này là kết quả EDA, chưa phải kết quả chất lượng mô hình.
