"""Read-only snapshot service shared by the terminal and local web demo."""
import argparse
import json
from pathlib import Path
from urllib.parse import quote
import numpy as np
import pandas as pd
from pipeline_common import ROOT, ARTIFACTS, PROTOCOL, read_json, load_csv, checksum

METHODS = tuple(PROTOCOL['methods'])
DISPLAY_NAMES = {'Popularity': 'Popularity', 'ContentBased': 'Content-based', 'CollaborativeFiltering': 'Collaborative Filtering', 'Hybrid': 'Hybrid'}


def verify_files(directory, files):
    for name, digest in files.items():
        path = (directory / name).resolve()
        if not path.is_relative_to(directory.resolve()) or not path.is_file() or checksum(path) != digest:
            raise ValueError('Snapshot thiếu hoặc sai checksum: ' + name)


def as_records(frame):
    return json.loads(frame.to_json(orient='records', force_ascii=False, double_precision=15))


class SnapshotService:
    """Load trusted local artefacts, never fit or write training/evaluation data."""
    def __init__(self, directory=ARTIFACTS):
        self.directory = Path(directory)
        self.model_directory = self.directory / 'models/primary'
        self.selection = read_json(self.model_directory / 'selection.json')
        if self.selection['protocol_sha256'] != checksum(ROOT / 'protocol.json'):
            raise ValueError('Protocol không khớp snapshot model')
        verify_files(self.model_directory, self.selection['files'])
        if tuple(self.selection['methods']) != METHODS:
            raise ValueError('Snapshot không có đúng bốn phương pháp')
        self.preprocessing = self.directory / 'preprocessing'
        self.preprocessing_manifest = read_json(self.preprocessing / 'manifest.json')
        if self.selection['preprocessing_manifest_sha256'] != checksum(self.preprocessing / 'manifest.json'):
            raise ValueError('Preprocessing không khớp snapshot model')
        verify_files(self.preprocessing, self.preprocessing_manifest['files'])
        self.indices = read_json(self.model_directory / 'indices.json')
        self.catalogue = {row['video_id']: row for row in read_json(self.preprocessing / 'catalogue.json')}
        self.responses = pd.concat([load_csv(self.preprocessing / 'primary' / f'{part}_responses.csv').assign(split=part)
                                    for part in ['train', 'validation', 'test']], ignore_index=True)
        self.seen = {user: set(group.video_id) for user, group in self.responses.groupby('user_id')}
        events = pd.read_csv(self.preprocessing / 'cleaned_events.csv', low_memory=False)
        fields = ['event_id', 'watch_duration_seconds', 'completion_rate', 'action_like', 'action_share', 'action_skip', 'action_block_creator']
        self.history_rows = self.responses.merge(events[fields], on='event_id', validate='one_to_one')
        self.scores, self.rankings = {}, {}
        evaluation_directory = self.directory / 'evaluation/primary'
        evaluation = read_json(evaluation_directory / 'manifest.json')
        if evaluation['selection_sha256'] != checksum(self.model_directory / 'selection.json'):
            raise ValueError('Kết quả đánh giá không khớp model')
        verify_files(evaluation_directory, evaluation['files'])
        self.metrics = load_csv(evaluation_directory / 'cohort_metrics.csv')
        self.interest_features = None

    def status(self):
        cohorts = [dict(id=cohort, label=('Lịch sử collaborator · danh tính giả định' if cohort.startswith('historical') else 'Run ' + cohort.removeprefix('run=')), users=len(index['users']), videos=len(index['videos']))
                   for cohort, index in self.indices.items()]
        return dict(version=PROTOCOL['version'], live_run=PROTOCOL.get('live_run', False), methods=[dict(id=method, label=DISPLAY_NAMES[method]) for method in METHODS],
                    default_method=self.selection['winner'], default_cohort=next(cohort for cohort in reversed(self.indices) if cohort.startswith('run=')),
                    cohorts=cohorts, sources=[dict(id='pipeline', label='Dữ liệu gộp', available=True)],
                    counts=dict(videos=len(self.catalogue), events=self.preprocessing_manifest['all_clean_events'],
                    first_pairs=self.preprocessing_manifest['scenarios']['primary']['pairs'], profiles=sum(len(index['users']) for index in self.indices.values()), gold_events=self.preprocessing_manifest['gold_events']),
                    configurations=self.selection['configurations'], policies=self.selection['policies'],
                    limitations=['Tương tác là dữ liệu mô phỏng.', 'Điểm xếp hạng không phải xác suất thích.', 'Danh tính lịch sử chưa được xác minh.', 'Chỉ hỗ trợ user và video trong snapshot.'])

    def _context(self, source, user_id):
        if source != 'pipeline':
            raise ValueError('Nguồn không hợp lệ')
        matches = [(cohort, index) for cohort, index in self.indices.items() if user_id in index['users']]
        if len(matches) != 1:
            raise ValueError('User không có trong snapshot; cần đầy đủ namespace')
        cohort, index = matches[0]
        return cohort, index['users'], index['videos'], self.catalogue, self.seen.get(user_id, set())

    def users(self, source='pipeline', cohort=None):
        if source != 'pipeline' or cohort not in self.indices:
            raise ValueError('Nguồn hoặc run không hợp lệ')
        return self.indices[cohort]['users']

    @staticmethod
    def video(record):
        video_id = record['video_id']
        return dict(video_id=video_id, title=record.get('title') or video_id,
                    description=record.get('description') or '', tags=record.get('tags') or [],
                    duration_seconds=record.get('duration_seconds'), channel_id=record.get('channel_id', ''),
                    url='https://www.youtube.com/watch?v=' + quote(video_id, safe=''),
                    thumbnail_url='https://i.ytimg.com/vi/' + quote(video_id, safe='') + '/hqdefault.jpg')

    def _scores(self, cohort):
        if cohort not in self.scores:
            index = self.indices[cohort]
            with np.load(self.model_directory / index['file'], allow_pickle=False) as saved:
                if set(saved.files) != set(METHODS):
                    raise ValueError('Model không khớp bốn phương pháp')
                arrays = {method: saved[method].copy() for method in METHODS}
            if not all(array.shape == (len(index['users']), len(index['videos'])) and np.isfinite(array).all() for array in arrays.values()):
                raise ValueError('Kích thước hoặc điểm model không hợp lệ')
            self.scores[cohort] = arrays
        return self.scores[cohort]

    def recommend(self, user_id, method=None, top_k=10, source='pipeline', mode='balanced'):
        if type(top_k) is not int or not 1 <= top_k <= 50:
            raise ValueError('Top-K phải là số nguyên từ 1 đến 50')
        if mode not in ['quality', 'balanced'] or (mode == 'balanced' and top_k != 10):
            raise ValueError('Chính sách cân bằng chỉ hỗ trợ Top-10; chọn xếp hạng gốc cho K khác')
        cohort, users, videos, catalogue, seen = self._context(source, user_id)
        row = users.index(user_id)
        candidates = np.array([index for index, video in enumerate(videos) if video not in seen], dtype=int)
        method = method or self.selection['winner']
        if method not in METHODS:
            raise ValueError('Phương pháp không có trong nguồn đã chọn')
        batch_coverage, policy = None, 'quality_only'
        score = self._scores(cohort)[method][row]
        configuration = self.selection['configurations'][method]
        ordered = candidates[np.lexsort((candidates, -score[candidates]))][:top_k]
        requested_policy = self.selection['policies'][method] if mode == 'balanced' else 'quality_only'
        if requested_policy != 'quality_only':
            key = (cohort, method)
            if key not in self.rankings:
                from ranking_metrics import balance_rankings
                allowed = np.array([[video not in self.seen.get(user, set()) for video in videos] for user in users])
                counts = self.indices[cohort]['train_counts']
                eligible = np.array([counts.get(user, 0) >= 5 for user in users]) & (allowed.sum(axis=1) >= 10)
                ranking, allocation = balance_rankings(self._scores(cohort)[method], allowed, eligible, float(requested_policy.removeprefix('coverage')) / 100)
                covered = set(int(item) for index in np.flatnonzero(eligible) for item in ranking[index])
                self.rankings[key] = (ranking, eligible, len(covered) / len(videos), allocation)
            ranking, eligible, coverage, _ = self.rankings[key]
            if eligible[row]:
                ordered, batch_coverage, policy = ranking[row], coverage, requested_policy
        ids = [videos[int(item)] for item in ordered]
        if len(ids) != len(set(ids)) or set(ids) & seen:
            raise ValueError('Danh sách có video trùng hoặc đã gặp')
        items = [{**self.video(catalogue[videos[int(item)]]), 'rank': rank + 1, 'score': float(score[int(item)])} for rank, item in enumerate(ordered)]
        return dict(source=source, cohort=cohort, user_id=user_id, method=method, configuration=configuration,
                    requested_mode=mode, policy=policy, top_k=top_k, returned=len(items), seen_count=len(seen),
                    candidate_count=len(candidates), batch_coverage=batch_coverage, items=items,
                    history_scope='Cả train, validation và test',
                    note='Danh sách demo khác giao thức test. Điểm xếp hạng không phải xác suất thích.')

    def history(self, user_id, source='pipeline'):
        _, _, _, catalogue, _ = self._context(source, user_id)
        rows = self.history_rows.loc[self.history_rows.user_id.eq(user_id)].sort_values('server_received_timestamp_ms', ascending=False).copy()
        records = []
        fields = ['split', 'positive_label', 'watch_duration_seconds', 'completion_rate', 'action_like', 'action_share', 'action_skip', 'action_block_creator']
        for record in as_records(rows):
            records.append({**self.video(catalogue[record['video_id']]), **{field: record.get(field) for field in fields}})
        return dict(user_id=user_id, source=source, count=len(records), positives=sum(row['positive_label'] for row in records),
                    items=records, summary=self.summarise_interests(user_id), note='Phản hồi quan sát trong dữ liệu đã lưu.')

    def summarise_interests(self, user_id):
        """Describe observed positive train items; never read held-out outcomes."""
        train = self.history_rows.loc[
            self.history_rows.user_id.eq(user_id) & self.history_rows.split.eq('train')
        ]
        positive = train.loc[train.positive_label.eq(1)].drop_duplicates('video_id')
        summary = dict(train_count=len(train), positive_count=len(positive), keywords=[], examples=[],
                       note='Suy ra từ video có phản hồi tích cực trong train; không phải sở thích tự khai.')
        if positive.empty:
            summary['note'] = 'Chưa có phản hồi tích cực trong train để tóm tắt sở thích.'
            return summary
        if self.interest_features is None:
            from sklearn.feature_extraction.text import TfidfVectorizer, ENGLISH_STOP_WORDS
            stop_words = sorted(set(ENGLISH_STOP_WORDS) | {
                'short', 'shorts', 'youtube', 'yt', 'viral', 'fyp', 'trending', 'video', 'videos',
                'subscribe', 'channel', 'like', 'follow', 'share', 'official', 'new', 'https', 'www', 'com',
                'easy', 'best', 'amazing', 'live', 'hat', 'đem', 'dem', 'cua', 'la', 'va', 'nhung', 'cho',
            })
            video_ids = sorted(self.catalogue)
            texts = [self.catalogue[key]['title'] + ' ' + ' '.join(self.catalogue[key].get('tags') or [])
                     for key in video_ids]
            vectoriser = TfidfVectorizer(stop_words=stop_words, ngram_range=(1, 1),
                                        strip_accents='unicode', min_df=2, max_df=0.6,
                                        token_pattern=r'(?u)\b[^\W\d_][\w]+\b')
            matrix = vectoriser.fit_transform(texts)
            self.interest_features = (dict(zip(video_ids, range(len(video_ids)))), matrix,
                                      vectoriser.get_feature_names_out())
        lookup, matrix, names = self.interest_features
        positive_matrix = matrix[[lookup[key] for key in positive.video_id]]
        weights = np.asarray(positive_matrix.mean(axis=0)).ravel()
        support = np.asarray((positive_matrix > 0).sum(axis=0)).ravel()
        # Prefer repeated evidence when enough positive items exist.
        minimum_support = 2 if len(positive) >= 4 else 1
        for index in np.argsort(-weights, kind='stable'):
            term = str(names[index])
            if weights[index] <= 0 or support[index] < minimum_support:
                continue
            if any(set(term.split()) & set(row['term'].split()) for row in summary['keywords']):
                continue
            summary['keywords'].append(dict(term=term, video_count=int(support[index])))
            if len(summary['keywords']) == 5:
                break
        examples = positive.sort_values(
            ['action_like', 'action_share', 'completion_rate', 'video_id'],
            ascending=[False, False, False, True], na_position='last',
        ).head(3)
        for record in as_records(examples):
            summary['examples'].append({**self.video(self.catalogue[record['video_id']]),
                                        **{key: record.get(key) for key in ['action_like', 'action_share', 'completion_rate']}})
        return summary

    def evaluation(self, source='pipeline', scope='primary', mode='balanced'):
        if source != 'pipeline' or scope not in ['primary', 'historical'] or mode not in ['raw', 'balanced']:
            raise ValueError('Tham số đánh giá không hợp lệ')
        mask = self.metrics.cohort.str.startswith('run=')
        frame = self.metrics.loc[(mask if scope == 'primary' else ~mask) & self.metrics['mode'].eq(mode)]
        fields = ['precision_at_10', 'recall_at_10', 'f1_at_10', 'ndcg_at_10', 'coverage', 'observed_auc']
        table = frame.groupby('model', sort=False)[fields].mean().reindex(METHODS).reset_index()
        return dict(source=source, scope=scope, mode=mode, rows=as_records(table),
                    note=f'Trung bình ngang nhau giữa {frame.cohort.nunique()} run.' if scope == 'primary' else 'Lịch sử dưới giả định danh tính chưa xác minh; báo cáo riêng.',
                    caveat='Positive ghi nhận trong log; ứng viên chưa có phản hồi là chưa biết. ' + ('Cấu hình đã được chốt bằng validation trước khi đánh giá test của lượt chạy mới.' if PROTOCOL.get('live_run') else 'Test đã được xem trong các lần nghiên cứu trước.'))


def main():
    parser = argparse.ArgumentParser(description='Đề xuất từ snapshot đã chốt, không huấn luyện lại')
    parser.add_argument('--user-id')
    parser.add_argument('--top-k', type=int, default=10)
    parser.add_argument('--list-users', action='store_true')
    parser.add_argument('--model', choices=METHODS)
    parser.add_argument('--source', choices=['pipeline'], default='pipeline')
    parser.add_argument('--mode', choices=['quality', 'balanced'], default='quality')
    args = parser.parse_args()
    service = SnapshotService()
    if args.list_users:
        for cohort, index in service.indices.items():
            print(cohort, '\n ', '\n  '.join(index['users'][:3]))
        return
    if not args.user_id:
        parser.error('Cần --user-id; dùng --list-users để xem ID')
    try:
        result = service.recommend(args.user_id, args.model, args.top_k, args.source, args.mode)
    except ValueError as error:
        parser.error(str(error))
    print('User:', result['user_id'])
    print('Phương pháp:', result['method'], '| Cấu hình:', result['configuration'], '| Policy:', result['policy'])
    print('Video đã gặp:', result['seen_count'], '| Ứng viên:', result['candidate_count'])
    for row in result['items']:
        print(f"{row['rank']:2}. {row['score']:.4f} | {row['video_id']} | {row['title']}")
    print(result['note'])


if __name__ == '__main__':
    main()
