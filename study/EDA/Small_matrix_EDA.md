# Tổng hợp đánh giá buổi 1 — KuaiRec small_matrix

Tài liệu này tổng hợp kết quả đã lưu trong `Small_matrix_EDA.ipynb`, giải thích ý nghĩa và giới hạn của từng kiểm tra. Đây là cơ sở để viết phần tổng kết, chưa phải kết luận về hành vi hoặc sở thích người dùng.

## 1. Mục tiêu và phạm vi

Buổi 1 nhằm xác nhận quy mô, cấu trúc và chất lượng cơ bản của toàn bộ `small_matrix.csv` trước khi phân tích. Những câu hỏi cần trả lời là: đã đọc toàn bộ file chưa, các cột có thiếu hoặc trùng không, thời lượng có hợp lệ không, và `watch_ratio` có nhất quán với hai cột thời lượng không.

Notebook hiện có 9 ô code và 1 ô Markdown. Các ô code đã có output. Dữ liệu gốc chưa bị xóa dòng, điền thiếu hoặc thay đổi ratio.

## 2. Đọc dữ liệu và kiểm tra cấu trúc — block 1 đến 3

### Kết quả

- Đã thiết lập thư viện và đường dẫn tới file trong thư mục `KuaiRec 2.0`.
- Đọc file bằng `pd.read_csv()` không giới hạn số dòng.
- DataFrame có **4.676.570 dòng và 8 cột**.
- Đã xem đầu file, cuối file và 5 dòng lấy mẫu với seed cố định.

| Cột | Kiểu dữ liệu hiện tại | Vai trò |
|---|---|---|
| `user_id` | `int64` | Mã người dùng |
| `video_id` | `int64` | Mã video |
| `play_duration` | `int64` | Thời gian xem, đơn vị mili giây |
| `video_duration` | `int64` | Thời lượng video, đơn vị mili giây |
| `time` | `str` | Thời gian tương tác dạng chuỗi |
| `date` | `float64` | Ngày được biểu diễn theo dạng YYYYMMDD |
| `timestamp` | `float64` | Unix timestamp |
| `watch_ratio` | `float64` | Tỷ lệ thời gian xem trên thời lượng video |

### Đánh giá

File đã được nạp toàn bộ. Việc xem đầu, cuối và mẫu giúp nhận diện cấu trúc, nhưng không thay thế kiểm tra trên toàn tập.

`time` chưa được chuyển sang datetime. `date` đang là số thực và có giá trị thiếu; chưa kiểm tra toàn bộ các giá trị ngày có hợp lệ hay không. Chưa cần đổi kiểu trước khi hiểu dữ liệu.

ID là mã định danh: giá trị ID lớn nhất không phải số lượng user hoặc video. Số lượng phải đếm bằng số giá trị duy nhất.

## 3. Giá trị thiếu — block 4 và 5

### Kết quả

| Cột hoặc nhóm cột | Số giá trị thiếu mỗi cột | Tỷ lệ thiếu |
|---|---:|---:|
| `user_id`, `video_id` | 0 | 0% |
| `play_duration`, `video_duration`, `watch_ratio` | 0 | 0% |
| `time`, `date`, `timestamp` | 181.992 | 3,89157% |

Kiểm tra tổ hợp thiếu cho thấy:

- **4.494.578 dòng** có đủ ba trường thời gian.
- **181.992 dòng** thiếu đồng thời cả ba trường.
- Không có dòng chỉ thiếu một hoặc hai trường thời gian.

### Đánh giá và hướng xử lý

Các trường cần cho phân tích mức độ xem không có giá trị thiếu theo kiểm tra `isna()`. Những dòng thiếu thời gian vẫn chứa ID, thời lượng và ratio nên có thể giữ lại cho các phân tích không phụ thuộc thời gian.

Khi phân tích theo ngày hoặc giờ, cần dùng các dòng có thời gian hợp lệ sau khi kiểm tra định dạng. Có đủ trường thời gian chưa đồng nghĩa với thời gian hợp lệ.

Chưa biết nguyên nhân thiếu hoặc sự thiếu có tập trung ở một nhóm user/video nào hay không. Không nên mặc định phần thiếu là ngẫu nhiên, cũng chưa có cơ sở để tự điền thời gian.

## 4. Trùng lặp và số thực thể — block 6

### Kết quả

| Kiểm tra | Kết quả |
|---|---:|
| Dòng trùng hoàn toàn, ngoài dòng đầu tiên | 0 |
| Cặp user–video lặp, ngoài lần đầu tiên | 0 |
| Số user duy nhất | 1.411 |
| Số video duy nhất | 3.327 |

### Đánh giá

Mỗi cặp user–video xuất hiện tối đa một lần trong file. Vì vậy, 4.676.570 dòng cũng là 4.676.570 cặp user–video quan sát được. Không cần loại bỏ trùng lặp theo hai tiêu chí đã kiểm tra.

Kết quả này mô tả cấu trúc file; không chứng minh rằng một người chỉ xem một video một lần trong thực tế.

Notebook chưa tính mật độ hoặc phân tích các cặp thiếu theo user/video. Đây là phần độ bao phủ của buổi tiếp theo.

## 5. Giá trị không hợp lệ — block 7

### Kết quả

| Điều kiện | Số dòng |
|---|---:|
| `play_duration < 0` | 0 |
| `video_duration < 0` | 0 |
| `video_duration = 0` | 0 |
| `watch_ratio < 0` | 0 |
| `watch_ratio` vô hạn | 0 |

### Đánh giá

Không phát hiện giá trị vi phạm các điều kiện đã kiểm tra. Mọi thời lượng video đều dương, nên có thể tính `play_duration / video_duration` cho toàn bộ các dòng.

Kết quả này chưa chứng minh mọi giá trị đều hợp lý về mặt hành vi. Chưa khảo sát phân phối, giá trị cực lớn hoặc outlier. Thời gian xem bằng 0 và ratio lớn hơn 1 không tự động là lỗi; cần phân tích ngữ cảnh trước khi xử lý.

## 6. Tính nhất quán của watch_ratio — block 8 và 9

### Cách kiểm tra

So sánh ratio lưu trong file với ratio tính lại từ `play_duration / video_duration`, dùng `np.isclose()` với `rtol=1e-6` và `atol=1e-8`. Kết quả khớp hay không phụ thuộc vào ngưỡng sai số này.

### Kết quả

| Chỉ số | Giá trị |
|---|---:|
| Số dòng khớp trong ngưỡng | 4.674.957 |
| Số dòng không khớp trong ngưỡng | 1.613 |
| Tỷ lệ không khớp | Khoảng 0,0345% |
| Sai lệch tuyệt đối lớn nhất | 0,002921739089867259 |

Đã xem 10 dòng không khớp có sai lệch tuyệt đối lớn nhất. Ví dụ dòng lệch nhiều nhất:

- `user_id`: 3885; `video_id`: 7358.
- `play_duration`: 183.003 ms; `video_duration`: 6.610 ms.
- Ratio lưu trong file: khoảng 27,682857.
- Ratio tính lại: khoảng 27,685779.
- Sai lệch tuyệt đối: khoảng 0,002922; sai lệch tương đối so với ratio tính lại khoảng 0,0106%.

### Đánh giá và hướng xử lý

Phần lớn ratio nhất quán với công thức trong ngưỡng đã chọn. Có một nhóm nhỏ vượt ngưỡng, nhưng vượt ngưỡng không tự động đồng nghĩa với dữ liệu sai.

Chưa xác định được nguyên nhân sai lệch. Chưa đủ bằng chứng để khẳng định là do làm tròn. Sai lệch tương đối của ví dụ trên cũng không đại diện cho toàn bộ 1.613 dòng.

Tạm giữ ratio gốc và ghi nhận vấn đề. Nếu phân tích sau nhạy với ngưỡng ratio, cần kiểm tra kết luận có thay đổi khi dùng ratio tính lại hay không.

## 7. Notebook đã có gì và còn thiếu gì?

### Đã có

- Nạp toàn bộ file và kiểm tra cấu trúc.
- Kiểm tra thiếu trên từng cột và thiếu đồng thời giữa ba trường thời gian.
- Kiểm tra trùng lặp, số user và số video.
- Kiểm tra một số điều kiện không hợp lệ của thời lượng và ratio.
- Đối chiếu ratio với công thức và xem các trường hợp lệch nhiều nhất.
- Một nhận xét Markdown về dữ liệu thiếu.

### Cần bổ sung để trình bày rõ buổi 1

- Mục tiêu ở đầu notebook và mô tả ngắn về các cột, đơn vị đo.
- Nhận xét sau từng nhóm kiểm tra: kết quả, ý nghĩa, giới hạn và quyết định xử lý.
- Một phần tổng kết chất lượng dữ liệu, nêu rõ điều đã xác nhận và điều chưa kiểm tra.

### Cần làm trước phân tích thời gian

- Kiểm tra các chuỗi thời gian có đọc được không, phân biệt lỗi định dạng với giá trị thiếu.
- Xác định ngày sớm nhất và muộn nhất trên các dòng hợp lệ.
- Đối chiếu `date`, `time` và `timestamp`; xác định cách diễn giải múi giờ trước khi so sánh timestamp.

### Thuộc các buổi EDA tiếp theo

- Độ bao phủ theo user/video và các cặp chưa quan sát.
- Phân phối thời lượng và `watch_ratio`, các phân vị và giá trị cực lớn.
- Tỷ lệ xem ngắn, xem đủ thời lượng hoặc xem lâu hơn thời lượng video.
- So sánh phản hồi theo user, video và nhóm thời lượng.
- Phân tích theo thời gian sau khi kiểm tra thời gian.

Chưa có cơ sở từ các kiểm tra hiện tại để kết luận người dùng thích loại video nào hoặc thường xem bao lâu.

## 8. Đoạn tổng kết tham khảo

Toàn bộ `small_matrix.csv` đã được nạp với 4.676.570 dòng, 8 cột, 1.411 user và 3.327 video. Không phát hiện dòng trùng hoàn toàn hoặc cặp user–video lặp. Các trường ID, thời lượng và watch_ratio không thiếu; 181.992 dòng, tương đương khoảng 3,89%, thiếu đồng thời time, date và timestamp. Không phát hiện thời lượng âm, thời lượng video bằng 0, ratio âm hoặc vô hạn. Khi đối chiếu watch_ratio với play_duration/video_duration bằng ngưỡng rtol=1e-6 và atol=1e-8, có 1.613 dòng không khớp, chiếm khoảng 0,0345%, với sai lệch tuyệt đối tối đa khoảng 0,002922. Dữ liệu được giữ nguyên để tiếp tục khám phá hành vi xem; cần kiểm tra thời gian trước phân tích theo ngày/giờ và ghi nhận sai lệch ratio khi diễn giải kết quả.
