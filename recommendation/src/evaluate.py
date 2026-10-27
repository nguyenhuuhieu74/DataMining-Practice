"""Evaluate four frozen methods, audit arithmetic and report uncertainty."""
import argparse
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from pipeline_common import ROOT, ARTIFACTS, REPORTS, checksum, read_json, write_json, load_csv, save_frame, verify_sources
from ranking_metrics import candidate_mask, evaluate, balance_rankings, macro_summary
from recommenders import factor_objective
from train import METHODS, QUALITY, fit_and_select


def evaluate_snapshot(output_name='primary'):
    model_path = ARTIFACTS / 'models' / output_name
    selection = read_json(model_path / 'selection.json')
    if selection['protocol_sha256'] != checksum(ROOT / 'protocol.json'):
        raise ValueError('Giao thức không khớp model')
    for name, digest in selection['files'].items():
        if checksum(model_path / name) != digest:
            raise ValueError('Model đã thay đổi: ' + name)
    destination = ARTIFACTS / 'evaluation' / output_name
    if (destination / 'manifest.json').exists():
        manifest = read_json(destination / 'manifest.json')
        if manifest['selection_sha256'] != checksum(model_path / 'selection.json'):
            raise ValueError('Selection không khớp evaluation')
        if manifest['evaluator_sha256'] != checksum(__file__):
            raise ValueError('Code evaluate đã đổi; cần phiên bản mới')
        for name, digest in manifest['files'].items():
            if checksum(destination / name) != digest:
                raise ValueError('Evaluation đã thay đổi: ' + name)
        return load_csv(destination / 'cohort_metrics.csv')
    sources = ARTIFACTS / 'preprocessing' / selection['scenario']
    prior = pd.concat([load_csv(sources / f'{part}_pairs.csv') for part in ['train', 'validation']])
    test = load_csv(sources / 'test_responses.csv')
    test['positive_label'] = test[selection['label']]
    summaries, users, recommendations = [], [], []
    for cohort, index in read_json(model_path / 'indices.json').items():
        user_ids, video_ids = index['users'], index['videos']
        allowed = candidate_mask(user_ids, video_ids, prior.loc[prior.cohort.eq(cohort)])
        heldout = test.loc[test.cohort.eq(cohort)]
        eligible = np.array([index['train_counts'].get(user, 0) >= 5 for user in user_ids]) & (allowed.sum(axis=1) >= 10)
        with np.load(model_path / index['file']) as saved:
            assert set(saved.files) == set(METHODS)
            for method in METHODS:
                score = saved[method]
                for mode in ['raw', 'balanced']:
                    policy = selection['policies'][method] if mode == 'balanced' else 'quality_only'
                    ranking, allocation = None, dict(mean_swaps=0, maximum_score_loss=0, maximum_swaps=0)
                    if policy != 'quality_only':
                        ranking, allocation = balance_rankings(score, allowed, eligible, float(policy.removeprefix('coverage')) / 100)
                    summary, rows, recs = evaluate(score, user_ids, video_ids, allowed, index['train_counts'], heldout, cohort, method, ranking)
                    summaries.append({**summary, **allocation, 'mode': mode, 'policy': policy})
                    users.append(rows.assign(mode=mode))
                    recommendations.append(recs.assign(mode=mode))
        print('Đã đánh giá:', cohort, flush=True)
    metrics = pd.DataFrame(summaries)
    save_frame(metrics, destination / 'cohort_metrics.csv')
    save_frame(pd.concat(users, ignore_index=True), destination / 'user_metrics.csv')
    save_frame(pd.concat(recommendations, ignore_index=True), destination / 'recommendations.csv')
    for scope, mask in [('primary', metrics.cohort.str.startswith('run=')), ('historical', ~metrics.cohort.str.startswith('run='))]:
        if not mask.any():
            continue
        for mode in ['raw', 'balanced']:
            table = macro_summary(metrics.loc[mask & metrics['mode'].eq(mode)])
            assert set(table.model) == set(METHODS) and len(table) == 4
            suffix = '_raw' if mode == 'raw' else ''
            save_frame(table, REPORTS / f'test_{scope}_{output_name}{suffix}.csv')
    write_json(destination / 'manifest.json', dict(evaluated_at=datetime.now(timezone.utc).isoformat(), selection_sha256=checksum(model_path / 'selection.json'), selection_created_at=selection['created_at'], test_responses_sha256=checksum(sources / 'test_responses.csv'), files={path.name: checksum(path) for path in destination.iterdir() if path.is_file()}, evaluator_sha256=checksum(__file__), relevance='Heldout logged positives; unobserved candidates are unjudged'))
    return metrics


def audit_results():
    manifest = read_json(ARTIFACTS / 'preprocessing/manifest.json')
    split_rows = []
    for name, digest in manifest['files'].items():
        assert checksum(ARTIFACTS / 'preprocessing' / name) == digest
    for scenario, info in manifest['scenarios'].items():
        frames = {part: load_csv(ARTIFACTS / 'preprocessing' / scenario / f'{part}_responses.csv') for part in ['train', 'validation', 'test']}
        combined = pd.concat(list(frames.values()))
        assert not combined.duplicated(['cohort', 'user_id', 'video_id']).any()
        assert not combined.event_id.duplicated().any()
        for cohort, cutoffs in info['cutoffs'].items():
            train, validation, test = [frames[part].loc[frames[part].cohort.eq(cohort)] for part in ['train', 'validation', 'test']]
            assert train.server_received_timestamp_ms.le(cutoffs['train_end_ms']).all()
            assert validation.server_received_timestamp_ms.gt(cutoffs['train_end_ms']).all()
            assert validation.server_received_timestamp_ms.le(cutoffs['validation_end_ms']).all()
            assert test.server_received_timestamp_ms.gt(cutoffs['validation_end_ms']).all()
            split_rows.append(dict(scenario=scenario, cohort=cohort, train=len(train), validation=len(validation), test=len(test), disjoint=True, chronological=True))
    selection = read_json(ARTIFACTS / 'models/primary/selection.json')
    evaluation = read_json(ARTIFACTS / 'evaluation/primary/manifest.json')
    assert selection['created_at'] < evaluation['evaluated_at'] and selection['test_responses_read'] is False
    assert evaluation['selection_sha256'] == checksum(ARTIFACTS / 'models/primary/selection.json')
    for name, digest in selection['code_sha256'].items():
        assert checksum(ROOT / 'src' / name) == digest
    for directory, snapshot in [(ARTIFACTS / 'models/primary', selection), (ARTIFACTS / 'evaluation/primary', evaluation)]:
        for name, digest in snapshot['files'].items():
            assert checksum(directory / name) == digest
    events = load_csv(ARTIFACTS / 'preprocessing/cleaned_events.csv')
    assert events.loc[events.action_block_creator.eq(1) | events.action_skip.eq(1), 'positive_label'].eq(0).all()
    assert not events.user_id.str.contains('bot_').any()
    test = load_csv(ARTIFACTS / 'preprocessing/primary/test_responses.csv')
    positives = {user: set(group.loc[group.positive_label.eq(1), 'video_id']) for user, group in test.groupby('user_id')}
    metrics = load_csv(ARTIFACTS / 'evaluation/primary/user_metrics.csv').set_index(['mode', 'model', 'user_id'])
    recs = load_csv(ARTIFACTS / 'evaluation/primary/recommendations.csv')
    assert set(recs.model) == set(METHODS)
    prior = pd.concat([load_csv(ARTIFACTS / 'preprocessing/primary' / f'{part}_pairs.csv') for part in ['train', 'validation']])
    prior_pairs = set(zip(prior.user_id, prior.video_id))
    maximum_error, checked_lists = 0.0, 0
    for (mode, method, user), group in recs.groupby(['mode', 'model', 'user_id']):
        group = group.sort_values('rank')
        assert len(group) == group.video_id.nunique() == 10
        assert all((user, video) not in prior_pairs for video in group.video_id)
        positive = positives.get(user, set())
        relevance = [int(video in positive) for video in group.video_id]
        precision = sum(relevance) / 10
        recall = sum(relevance) / len(positive) if positive else 0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0
        dcg = sum(value / np.log2(rank + 2) for rank, value in enumerate(relevance))
        ideal = sum(1 / np.log2(rank + 2) for rank in range(min(10, len(positive))))
        actual = np.array([precision, recall, f1, dcg / ideal if ideal else 0])
        maximum_error = max(maximum_error, float(np.max(np.abs(actual - metrics.loc[(mode, method, user), list(QUALITY)].to_numpy(float)))))
        checked_lists += 1
    assert maximum_error < 1e-10
    generator = np.random.default_rng(903)
    labels, observed = generator.integers(0, 2, (2, 3)).astype(float), generator.integers(0, 2, (2, 3)).astype(float)
    parameters = generator.normal(0, 0.1, (2+3)*2+2+3+1)
    loss, gradient = factor_objective(parameters, labels, observed, 2, 1.0)
    differences = []
    for index in range(len(parameters)):
        right, left = parameters.copy(), parameters.copy()
        right[index] += 1e-5
        left[index] -= 1e-5
        differences.append((factor_objective(right, labels, observed, 2, 1.0)[0] - factor_objective(left, labels, observed, 2, 1.0)[0]) / 2e-5)
    gradient_error = float(np.max(np.abs(gradient - differences)))
    assert gradient_error < 1e-6
    poisoned = labels.copy()
    poisoned[observed == 0] = 999
    poison_loss, poison_gradient = factor_objective(parameters, poisoned, observed, 2, 1.0)
    assert abs(loss - poison_loss) < 1e-10 and np.max(np.abs(gradient - poison_gradient)) < 1e-10
    verify_sources()
    result = dict(status='PASS', methods=list(METHODS), checked_lists=checked_lists, metric_maximum_absolute_error=maximum_error, factor_gradient_error=gradient_error, sources_and_collaborator_unchanged=True, proves_absence_of_all_leakage_or_overfit=False, old_test_previously_inspected=selection.get('old_test_previously_inspected', True))
    write_json(REPORTS / 'audit.json', result)
    save_frame(pd.DataFrame(split_rows), REPORTS / 'split_audit.csv')
    return result


def paired_uncertainty():
    frame = load_csv(ARTIFACTS / 'evaluation/primary/user_metrics.csv')
    frame = frame.loc[frame.cohort.str.startswith('run=') & frame.test_positive_count.gt(0)]
    rows, generator = [], np.random.default_rng(20261026)
    for mode in ['raw', 'balanced']:
        baseline = frame.loc[frame['mode'].eq(mode) & frame.model.eq('Popularity')].set_index(['cohort', 'user_id']).sort_index()
        for method in METHODS[1:]:
            candidate = frame.loc[frame['mode'].eq(mode) & frame.model.eq(method)].set_index(['cohort', 'user_id']).sort_index()
            assert candidate.index.equals(baseline.index)
            for metric in QUALITY:
                difference = candidate[metric] - baseline[metric]
                draws, means = [], []
                for _, group in difference.groupby(level='cohort'):
                    values = group.to_numpy(float)
                    draws.append(values[generator.integers(0, len(values), (5000, len(values)))].mean(axis=1))
                    means.append(values.mean())
                distribution = np.mean(draws, axis=0)
                low, high = np.quantile(distribution, [0.025 / 24, 1 - 0.025 / 24])
                rows.append(dict(mode=mode, model=method, baseline='Popularity', metric=metric, mean_difference=float(np.mean(means)), simultaneous_ci_lower=float(low), simultaneous_ci_upper=float(high)))
    table = pd.DataFrame(rows)
    save_frame(table, REPORTS / 'paired_uncertainty.csv')
    return table


def run_diagnostics():
    reference = read_json(ARTIFACTS / 'models/primary/selection.json')
    for scenario, label, name, permuted in [('historical_day', 'positive_label', 'historical_day', False), ('primary', 'engagement_label', 'engagement', False), ('primary', 'positive_label', 'permuted', True)]:
        fit_and_select(scenario, label, name, permuted, reference_selection=reference)
        evaluate_snapshot(name)


def main():
    parser = argparse.ArgumentParser(description='Đánh giá và kiểm tra đúng bốn phương pháp')
    parser.add_argument('--all', action='store_true', help='Chạy thêm ba đối chứng đã định trước')
    args = parser.parse_args()
    fit_and_select()
    evaluate_snapshot()
    print(audit_results(), flush=True)
    paired_uncertainty()
    if args.all:
        run_diagnostics()
    from report import make_report
    make_report()
    print(load_csv(REPORTS / 'test_primary_primary.csv').to_string(index=False))


if __name__ == '__main__':
    main()
