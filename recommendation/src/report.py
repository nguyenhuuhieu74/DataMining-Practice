"""Compact report and three stage notebooks; algorithms stay in their own modules."""
import json
import uuid
import pandas as pd
from pipeline_common import ROOT, ARTIFACTS, REPORTS, PROTOCOL, read_json, load_csv
from train import METHODS, QUALITY


def markdown_table(frame):
    rows = [[f"{value:.5f}" if isinstance(value, float) else str(value) for value in row]
            for row in frame.itertuples(index=False, name=None)]
    return '\n'.join(['| ' + ' | '.join(frame.columns) + ' |',
                      '| ' + ' | '.join(['---'] * len(frame.columns)) + ' |'] +
                     ['| ' + ' | '.join(row) + ' |' for row in rows])


def write_notebook(name, entries):
    path = ROOT / 'notebooks' / name
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    cells = []
    for kind, source in entries:
        cell = dict(cell_type=kind, id=uuid.uuid4().hex[:8], metadata={}, source=source.splitlines(keepends=True))
        if kind == 'code':
            cell.update(execution_count=None, outputs=[])
        cells.append(cell)
    notebook = dict(cells=cells, metadata={'kernelspec': {'display_name': 'Python (.venv)', 'language': 'python', 'name': 'python3'}}, nbformat=4, nbformat_minor=5)
    path.write_text(json.dumps(notebook, ensure_ascii=False, indent=1) + '\n', encoding='utf-8')


SETUP = '''import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

project_directory = next(path for path in [Path.cwd().resolve(), *Path.cwd().resolve().parents]
                         if (path / "recommendation/config.json").is_file())
sys.path.insert(0, str(project_directory / "recommendation/src"))
from pipeline_common import ROOT, ARTIFACTS, REPORTS, read_json, load_csv
'''


def make_report():
    selection = read_json(ARTIFACTS / 'models/primary/selection.json')
    preprocessing = read_json(ARTIFACTS / 'preprocessing/manifest.json')
    balanced = load_csv(REPORTS / 'test_primary_primary.csv')
    raw = load_csv(REPORTS / 'test_primary_primary_raw.csv')
    columns = ['model', *QUALITY, 'coverage', 'observed_auc']
    history_path = REPORTS / 'test_historical_primary.csv'
    historical = load_csv(history_path) if history_path.exists() else pd.DataFrame(columns=columns)
    configurations = pd.DataFrame([dict(method=method, configuration=selection['configurations'][method], policy=selection['policies'][method]) for method in METHODS])
    text = f'''# Recommendation: bốn phương pháp

Phiên bản: **four_methods_v1**. Mặc định chọn trên validation: **{selection['winner']}**.
Chỉ xuất đúng bốn nhóm: Popularity, ContentBased, CollaborativeFiltering và Hybrid.
Các cấu hình nội bộ không phải phương pháp thứ năm. Coverage là chính sách xếp hạng
áp dụng cho từng phương pháp, được chọn bằng validation.

## Cấu hình đã chốt

{markdown_table(configurations)}

Content-based thử TF-IDF full/title-tags và SVD logistic C=0,1/1. Collaborative filtering
thử ItemCF và LogisticMF 4/8 chiều với regularisation=5. Chọn một cấu hình mỗi nhóm bằng
NDCG validation của ba run hiện tại, rồi chọn alpha trong 0,25/0,5/0,75 cho hybrid của
hai cấu hình đã chọn. Không chọn lại cấu hình theo test và không ép Hybrid phải thắng.

## Dữ liệu và đánh giá

Nguồn snapshot Gold có {preprocessing['gold_events']} sự kiện; lịch sử sau xử lý có
{preprocessing['historical_clean_events']} sự kiện. Tổng {preprocessing['all_clean_events']},
catalogue {preprocessing['catalogue_videos']} video, {preprocessing['scenarios']['primary']['pairs']} cặp lần đầu.
Preprocessing được chạy lại từ snapshot đã xác minh. Dữ liệu thô, mã collaborator và
các bảng Bronze/Silver/Gold không bị sửa. Raw hiện tại chỉ truy provenance, không cộng lại với Gold.

Danh tính current có namespace run; lịch sử có giả định một lần khởi tạo chưa xác minh,
không trộn người cùng chuỗi ID. Chia theo server time trong mỗi cohort 70/15/15.
Nhãn positive: không block/skip và completion >=0,8. Dữ liệu chưa quan sát không thành
negative để huấn luyện. Chỉ dùng title/tags/description làm feature, không dùng crawl query
hoặc popularity tại lúc crawl. Metadata tĩnh mang tính hồi cứu.

## Test chính: đúng bốn phương pháp, với chính sách đã chốt

{markdown_table(balanced[columns])}

Mỗi run trọng số bằng nhau. P/R/F/NDCG lấy macro trên user đủ điều kiện có positive test;
coverage gồm mọi user đủ điều kiện trong catalogue của từng run. Candidate là toàn catalogue
cohort trừ cặp train/validation đã gặp. Positive test chỉ là nhãn ghi nhận trong log;
P@10 không phải xác suất người thật thích video. Ứng viên chưa có phản hồi được coi là chưa biết.

## Đối chiếu xếp hạng gốc, cùng bốn phương pháp

{markdown_table(raw[columns])}

Ngân sách cân bằng: tối đa hai thay đổi/user, giảm trung bình điểm chuẩn hoá không quá 0,02.
Chỉ nhận policy nếu từng P/R/F/NDCG validation giảm không quá 0,01 và coverage tăng.
Ngân sách điểm không bảo đảm mọi user có chất lượng tốt hơn.

## Lịch sử, chỉ là nhánh phụ theo giả định danh tính

{markdown_table(historical[columns])}

## Giới hạn

Test đã được xem ở các lần chạy trước. Đây là lần chạy lại và tinh gọn cùng dữ liệu,
không phải tập xác nhận độc lập mới. Chất lượng cao hơn ở một dòng test không được dùng đổi winner.
Simulator collaborator có vector sở thích ngẫu nhiên độc lập với văn bản; chưa có cơ sở
đòi hỏi hybrid luôn thắng. Việc giảm số phương pháp không tự làm chất lượng tăng.
'''
    (REPORTS / 'REPORT.md').write_text(text, encoding='utf-8')
    if PROTOCOL.get('live_run'):
        text = text.replace('**four_methods_v1**', f"**{PROTOCOL['version']}**")
        text = text.replace('NDCG validation của ba run hiện tại', 'NDCG validation của run mới')
        text = text.replace('Mỗi run trọng số bằng nhau.', 'Đánh giá trên lượt sinh dữ liệu mới.')
        start = text.index('## Lịch sử, chỉ là nhánh phụ theo giả định danh tính')
        text = text[:start] + '''## Giới hạn của demo live

Chỉ dùng dữ liệu mới trong lượt chạy này, không nạp lịch sử hoặc mô hình cũ.
Metadata crawl từ YouTube; tương tác vẫn được sinh mô phỏng bởi code của dự án.
Chọn cấu hình trên validation, lưu mô hình trước khi đọc phản hồi test.
Điểm đo trên dữ liệu mô phỏng không chứng minh hiệu quả trên người dùng thật.
'''
        (REPORTS / 'REPORT.md').write_text(text, encoding='utf-8')
        print('Đã lưu báo cáo lượt chạy mới:', REPORTS / 'REPORT.md', flush=True)
        return
    write_notebook('Preprocessing.ipynb', [
        ('markdown', '# Preprocessing\n\nGiai đoạn tiền xử lý sau EDA. Chạy lại trên bản snapshot dẫn xuất; giữ nguyên nguồn và pipeline collaborator.'),
        ('code', SETUP),
        ('code', 'from preprocess import build_preprocessing\nmanifest = build_preprocessing()\ndisplay(pd.Series({key:value for key,value in manifest.items() if key not in ["files", "scenarios"]}))\n'),
        ('markdown', '## Quy tắc và nhãn\n\nLoại thiếu ID, bot, watch âm/quá 3.600 giây và event ID lặp; giữ null watch đi cùng block. Positive yêu cầu không block/skip và completion ≥0,8. Không cắt completion >1. Cặp lần đầu dùng cho video mới; lượt xem lặp vẫn lưu riêng.'),
        ('code', 'display(load_csv(ARTIFACTS / "preprocessing/cleaning_audit.csv"))\nfor scenario, details in manifest["scenarios"].items():\n    print(scenario)\n    display(pd.DataFrame(details["counts"]).T)\n'),
        ('markdown', '## Kết luận\n\nDữ liệu có provenance và checksum. Train/validation/test chia theo thời gian toàn cohort. Lịch sử vẫn có giả định danh tính; chưa huấn luyện trong notebook này.')])
    write_notebook('Train.ipynb', [
        ('markdown', '# Train\n\nChỉ có bốn phương pháp. Chọn cấu hình bằng validation, ghi snapshot trước khi Evaluate đọc nhãn test.'),
        ('code', SETUP),
        ('code', 'from train import fit_and_select\nselection = fit_and_select()\ndisplay(pd.DataFrame({"configuration":selection["configurations"], "policy":selection["policies"]}))\nprint("Mặc định:",selection["winner"],"| alpha:",selection["alpha"])\n'),
        ('markdown', '## Validation và hội tụ\n\nCF và content mỗi nhóm xuất một cấu hình tốt nhất trên validation; hybrid kết hợp hai cấu hình đó. Bảng tìm cấu hình chi tiết nằm trong artifacts, không thêm hàng phương pháp vào báo cáo chính.'),
        ('code', 'display(load_csv(REPORTS / "validation_primary.csv"))\ndisplay(pd.DataFrame(read_json(ARTIFACTS / "models/primary/fit_audit.json")))\n'),
        ('markdown', '## Kết luận\n\nĐã chốt bốn phương pháp và policy từng nhóm. Không đổi cấu hình theo kết quả test. Giữ trạng thái test đã được xem trong nghiên cứu trước để tránh gọi lần chạy này là xác nhận độc lập.')])
    write_notebook('Evaluate.ipynb', [
        ('markdown', '# Evaluate\n\nSo sánh đúng bốn phương pháp. Cân bằng coverage là policy, không phải một phương pháp bổ sung.'),
        ('code', SETUP),
        ('code', 'from evaluate import evaluate_snapshot, audit_results, paired_uncertainty\ncohort_metrics = evaluate_snapshot()\nprimary_metrics = load_csv(REPORTS / "test_primary_primary.csv")\ndisplay(primary_metrics)\n'),
        ('markdown', '## Trực quan và đánh đổi\n\nSo với xếp hạng gốc để thấy policy ảnh hưởng chất lượng và coverage ra sao. Các phương pháp dùng cùng nguồn và ứng viên.'),
        ('code', 'figure, axes = plt.subplots(1,2,figsize=(13,4))\nprimary_metrics.set_index("model")[["precision_at_10","recall_at_10","f1_at_10","ndcg_at_10"]].plot.bar(ax=axes[0],rot=20)\naxes[0].set_ylabel("Chỉ số trên nhãn log")\nraw_metrics = load_csv(REPORTS / "test_primary_primary_raw.csv").set_index("model")\npd.DataFrame({"Gốc":raw_metrics.coverage,"Policy đã chọn":primary_metrics.set_index("model").coverage}).plot.bar(ax=axes[1],rot=20)\naxes[1].set_ylim(0,1)\naxes[1].set_ylabel("Độ phủ catalogue theo run")\nfigure.tight_layout()\nplt.show()\n'),
        ('code', 'display(load_csv(REPORTS / "test_primary_primary_raw.csv"))\ndisplay(load_csv(REPORTS / "test_historical_primary.csv"))\ndisplay(cohort_metrics.loc[cohort_metrics["mode"].eq("raw") & cohort_metrics.model.eq("Popularity"),["cohort","eligible_users","evaluated_users","users_without_positives","candidate_count","test_positive_count","oracle_precision_at_10","oracle_recall_at_10"]])\n'),
        ('markdown', '## Kiểm chứng và đối chứng\n\nAudit tính lại metric từ ID và positive test. Khoảng tin cậy có điều kiện trên ba run và model đã fit; không bảo đảm tổng quát ra người thật.'),
        ('code', 'display(pd.Series(audit_results()))\ndisplay(paired_uncertainty())\nfor scenario in ["historical_day","engagement","permuted"]:\n    path = REPORTS / f"test_primary_{scenario}.csv"\n    if path.exists():\n        print(scenario)\n        display(load_csv(path))\n'),
        ('markdown', '## Kết luận\n\nĐọc REPORT.md để xem cấu hình, kết quả và giới hạn. Không gọi lần chạy lại trên test đã xem là tập xác nhận mới. Demo terminal dùng score snapshot; API và demo web dùng snapshot đã lưu; xem DEMO.md.')])
    print('Đã cập nhật báo cáo bốn phương pháp:', REPORTS / 'REPORT.md', flush=True)


if __name__ == '__main__':
    make_report()
