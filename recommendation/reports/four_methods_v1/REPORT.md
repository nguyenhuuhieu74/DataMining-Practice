# Recommendation: bốn phương pháp

Phiên bản: **four_methods_v1**. Mặc định chọn trên validation: **Popularity**.
Chỉ xuất đúng bốn nhóm: Popularity, ContentBased, CollaborativeFiltering và Hybrid.
Các cấu hình nội bộ không phải phương pháp thứ năm. Coverage là chính sách xếp hạng
áp dụng cho từng phương pháp, được chọn bằng validation.

## Cấu hình đã chốt

| method | configuration | policy |
| --- | --- | --- |
| Popularity | Popularity | coverage90 |
| ContentBased | ContentFull | quality_only |
| CollaborativeFiltering | LogisticMF_d4_r5 | coverage90 |
| Hybrid | Hybrid_alpha_0.5 | coverage90 |

Content-based thử TF-IDF full/title-tags và SVD logistic C=0,1/1. Collaborative filtering
thử ItemCF và LogisticMF 4/8 chiều với regularisation=5. Chọn một cấu hình mỗi nhóm bằng
NDCG validation của ba run hiện tại, rồi chọn alpha trong 0,25/0,5/0,75 cho hybrid của
hai cấu hình đã chọn. Không chọn lại cấu hình theo test và không ép Hybrid phải thắng.

## Dữ liệu và đánh giá

Nguồn snapshot Gold có 28762 sự kiện; lịch sử sau xử lý có
28255 sự kiện. Tổng 57017,
catalogue 560 video, 40734 cặp lần đầu.
Preprocessing được chạy lại từ snapshot đã xác minh. Dữ liệu thô, mã collaborator và
các bảng Bronze/Silver/Gold không bị sửa. Raw hiện tại chỉ truy provenance, không cộng lại với Gold.

Danh tính current có namespace run; lịch sử có giả định một lần khởi tạo chưa xác minh,
không trộn người cùng chuỗi ID. Chia theo server time trong mỗi cohort 70/15/15.
Nhãn positive: không block/skip và completion >=0,8. Dữ liệu chưa quan sát không thành
negative để huấn luyện. Chỉ dùng title/tags/description làm feature, không dùng crawl query
hoặc popularity tại lúc crawl. Metadata tĩnh mang tính hồi cứu.

## Test chính: đúng bốn phương pháp, với chính sách đã chốt

| model | precision_at_10 | recall_at_10 | f1_at_10 | ndcg_at_10 | coverage | observed_auc |
| --- | --- | --- | --- | --- | --- | --- |
| CollaborativeFiltering | 0.16513 | 0.39476 | 0.21487 | 0.27263 | 0.91604 | 0.49835 |
| ContentBased | 0.15822 | 0.38384 | 0.20667 | 0.27291 | 0.94775 | 0.51486 |
| Hybrid | 0.16819 | 0.39663 | 0.21772 | 0.27643 | 0.93021 | 0.49813 |
| Popularity | 0.16645 | 0.39836 | 0.21700 | 0.28874 | 0.92227 | 0.47835 |

Mỗi run trọng số bằng nhau. P/R/F/NDCG lấy macro trên user đủ điều kiện có positive test;
coverage gồm mọi user đủ điều kiện trong catalogue của từng run. Candidate là toàn catalogue
cohort trừ cặp train/validation đã gặp. Positive test chỉ là nhãn ghi nhận trong log;
P@10 không phải xác suất người thật thích video. Ứng viên chưa có phản hồi được coi là chưa biết.

## Đối chiếu xếp hạng gốc, cùng bốn phương pháp

| model | precision_at_10 | recall_at_10 | f1_at_10 | ndcg_at_10 | coverage | observed_auc |
| --- | --- | --- | --- | --- | --- | --- |
| CollaborativeFiltering | 0.16404 | 0.38971 | 0.21310 | 0.27075 | 0.74433 | 0.49835 |
| ContentBased | 0.15822 | 0.38384 | 0.20667 | 0.27291 | 0.94775 | 0.51486 |
| Hybrid | 0.16783 | 0.39603 | 0.21727 | 0.27611 | 0.88278 | 0.49813 |
| Popularity | 0.16681 | 0.39796 | 0.21737 | 0.28866 | 0.68572 | 0.47835 |

Ngân sách cân bằng: tối đa hai thay đổi/user, giảm trung bình điểm chuẩn hoá không quá 0,02.
Chỉ nhận policy nếu từng P/R/F/NDCG validation giảm không quá 0,01 và coverage tăng.
Ngân sách điểm không bảo đảm mọi user có chất lượng tốt hơn.

## Lịch sử, chỉ là nhánh phụ theo giả định danh tính

| model | precision_at_10 | recall_at_10 | f1_at_10 | ndcg_at_10 | coverage | observed_auc |
| --- | --- | --- | --- | --- | --- | --- |
| CollaborativeFiltering | 0.03394 | 0.06095 | 0.03935 | 0.06057 | 0.55601 | 0.54178 |
| ContentBased | 0.01384 | 0.02383 | 0.01580 | 0.01751 | 0.93279 | 0.45340 |
| Hybrid | 0.02715 | 0.04643 | 0.03146 | 0.04313 | 0.90020 | 0.52123 |
| Popularity | 0.05901 | 0.11540 | 0.07058 | 0.10657 | 0.90020 | 0.52402 |

## Giới hạn

Test đã được xem ở các lần chạy trước. Đây là lần chạy lại và tinh gọn cùng dữ liệu,
không phải tập xác nhận độc lập mới. Chất lượng cao hơn ở một dòng test không được dùng đổi winner.
Simulator collaborator có vector sở thích ngẫu nhiên độc lập với văn bản; chưa có cơ sở
đòi hỏi hybrid luôn thắng. Việc giảm số phương pháp không tự làm chất lượng tăng.
