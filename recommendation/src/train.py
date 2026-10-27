"""Four method families, selected on validation and sealed before evaluation."""
from datetime import datetime, timezone
import pickle
import numpy as np
import pandas as pd
from scipy import sparse
from pipeline_common import ROOT, ARTIFACTS, REPORTS, PROTOCOL, checksum, read_json, write_json, load_csv, save_frame, verify_sources
from preprocess import build_preprocessing
from recommenders import build_features, fit_models, normalise_scores
from ranking_metrics import candidate_mask, evaluate, balance_rankings, macro_summary

METHODS = ('Popularity', 'ContentBased', 'CollaborativeFiltering', 'Hybrid')
QUALITY = ('precision_at_10', 'recall_at_10', 'f1_at_10', 'ndcg_at_10')


def fit_and_select(scenario='primary', label='positive_label', output_name='primary', permutation=False, reference_selection=None):
    destination = ARTIFACTS / 'models' / output_name
    if (destination / 'selection.json').exists():
        selection = read_json(destination / 'selection.json')
        if selection['protocol_sha256'] != checksum(ROOT / 'protocol.json'):
            raise ValueError('Giao thức đổi; cần phiên bản mới')
        for name, digest in selection['files'].items():
            if checksum(destination / name) != digest:
                raise ValueError('Model đã thay đổi: ' + name)
        for name, digest in selection['code_sha256'].items():
            if checksum(ROOT / 'src' / name) != digest:
                raise ValueError('Code đã đổi sau khi chốt: ' + name)
        return selection
    build_preprocessing()
    preprocessing = ARTIFACTS / 'preprocessing'
    manifest = read_json(preprocessing / 'manifest.json')
    for name, digest in manifest['files'].items():
        if checksum(preprocessing / name) != digest:
            raise ValueError('Preprocessing đã thay đổi: ' + name)
    catalogue = read_json(preprocessing / 'catalogue.json')
    features, feature_state = build_features(catalogue)
    video_index = {row['video_id']: i for i, row in enumerate(catalogue)}
    training = load_csv(preprocessing / scenario / 'train_responses.csv')
    validation = load_csv(preprocessing / scenario / 'validation_responses.csv')
    # Heldout identities define a retrospective catalogue; no heldout outcomes here.
    roster = pd.concat([load_csv(preprocessing / scenario / f'{part}_pairs.csv') for part in ['train', 'validation', 'test']], ignore_index=True)
    training['positive_label'], validation['positive_label'] = training[label], validation[label]
    if permutation:
        generator = np.random.default_rng(551)
        training['positive_label'] = training.groupby('user_id')['positive_label'].transform(lambda values: generator.permutation(values.to_numpy()))
    cohorts, searches, audits = {}, [], []
    for cohort, group in roster.groupby('cohort'):
        users, videos = sorted(group.user_id.unique()), sorted(group.video_id.unique())
        train, valid = training.loc[training.cohort.eq(cohort)], validation.loc[validation.cohort.eq(cohort)]
        required = None if reference_selection is None else [reference_selection['configurations'][method] for method in ['ContentBased', 'CollaborativeFiltering']]
        scores, state, fit_audit = fit_models(train[['user_id', 'video_id', 'positive_label']], users, videos, features, [video_index[video] for video in videos], required_models=required)
        counts, allowed = train.user_id.value_counts().to_dict(), candidate_mask(users, videos, train)
        for name, score in scores.items():
            summary, _, _ = evaluate(score, users, videos, allowed, counts, valid, cohort, name)
            searches.append(summary)
        cohorts[cohort] = dict(users=users, videos=videos, scores=scores, state=state, train_counts=counts, allowed=allowed, validation=valid)
        audits.extend({'cohort': cohort, **row} for row in fit_audit)
        print('Đã fit:', cohort, flush=True)
    common = set.intersection(*(set(data['scores']) for data in cohorts.values()))
    search = pd.DataFrame(searches)
    search = search.loc[search.model.isin(common)]
    ranked = macro_summary(search.loc[search.cohort.str.startswith('run=')]).sort_values(['ndcg_at_10', 'model'], ascending=[False, True], kind='stable')
    if reference_selection is None:
        content = ranked.loc[ranked.model.str.startswith(('Content', 'SVDContent'))].iloc[0].model
        collaborative = ranked.loc[ranked.model.eq('ItemCF') | ranked.model.str.startswith('LogisticMF')].iloc[0].model
    else:
        content = reference_selection['configurations']['ContentBased']
        collaborative = reference_selection['configurations']['CollaborativeFiltering']
    hybrid_rows = []
    for cohort, data in cohorts.items():
        scores = data['scores']
        for alpha in PROTOCOL['search']['hybrid_cf_weights']:
            name = f'Hybrid_alpha_{alpha:g}'
            scores[name] = alpha * normalise_scores(scores[collaborative]) + (1-alpha) * normalise_scores(scores[content])
            summary, _, _ = evaluate(scores[name], data['users'], data['videos'], data['allowed'], data['train_counts'], data['validation'], cohort, name)
            hybrid_rows.append(summary)
    hybrids = pd.DataFrame(hybrid_rows)
    hybrid_ranking = macro_summary(hybrids.loc[hybrids.cohort.str.startswith('run=')]).sort_values(['ndcg_at_10', 'model'], ascending=[False, True], kind='stable')
    alpha = float(hybrid_ranking.iloc[0].model.removeprefix('Hybrid_alpha_')) if reference_selection is None else reference_selection['alpha']
    configurations = {'Popularity': 'Popularity', 'ContentBased': content, 'CollaborativeFiltering': collaborative, 'Hybrid': f'Hybrid_alpha_{alpha:g}'}
    summaries, policies = [], []
    for cohort, data in cohorts.items():
        data['scores'] = {method: data['scores'][name] for method, name in configurations.items()}
        eligible = np.array([data['train_counts'].get(user, 0) >= 5 for user in data['users']]) & (data['allowed'].sum(axis=1) >= 10)
        for method, score in data['scores'].items():
            summary, _, _ = evaluate(score, data['users'], data['videos'], data['allowed'], data['train_counts'], data['validation'], cohort, method)
            summaries.append(summary)
            for target in PROTOCOL['search']['coverage_targets']:
                rankings, allocation = balance_rankings(score, data['allowed'], eligible, target)
                summary, _, _ = evaluate(score, data['users'], data['videos'], data['allowed'], data['train_counts'], data['validation'], cohort, method, rankings)
                policies.append({**summary, **allocation, 'policy': f'coverage{int(target*100)}'})
    validation_metrics, policy_table = pd.DataFrame(summaries), pd.DataFrame(policies)
    ranking = macro_summary(validation_metrics.loc[validation_metrics.cohort.str.startswith('run=')])
    winner = ranking.sort_values(['ndcg_at_10', 'model'], ascending=[False, True], kind='stable').iloc[0].model
    selected_policies = {}
    for method in METHODS:
        baseline = ranking.loc[ranking.model.eq(method)].iloc[0]
        accepted = [(float(baseline.coverage), 'quality_only')]
        for policy, group in policy_table.loc[policy_table.model.eq(method) & policy_table.cohort.str.startswith('run=')].groupby('policy'):
            row = macro_summary(group).iloc[0]
            if all(row[metric] >= baseline[metric] - 0.01 for metric in QUALITY) and row.coverage > baseline.coverage:
                accepted.append((float(row.coverage), policy))
        selected_policies[method] = max(accepted)[1]
    if reference_selection is not None:
        selected_policies, winner = reference_selection['policies'], reference_selection['winner']
    destination.mkdir(parents=True, exist_ok=True)
    with (destination / 'feature_state.pkl').open('wb') as file:
        pickle.dump(feature_state, file)
    for name in ['full', 'title_tags']:
        sparse.save_npz(destination / f'content_{name}.npz', features[name])
    np.save(destination / 'content_embedding.npy', features['embedding'])
    indices = {}
    for cohort, data in cohorts.items():
        safe = cohort.replace('=', '_')
        filename = f'scores_{safe}.npz'
        np.savez_compressed(destination / filename, **data['scores'])
        np.savez_compressed(destination / f'state_{safe}.npz', **data['state'])
        indices[cohort] = dict(users=data['users'], videos=data['videos'], train_counts=data['train_counts'], file=filename)
    write_json(destination / 'indices.json', indices)
    write_json(destination / 'fit_audit.json', audits)
    save_frame(pd.concat([search, hybrids]), destination / 'configuration_search.csv')
    save_frame(validation_metrics, destination / 'validation_cohorts.csv')
    save_frame(policy_table, destination / 'validation_policies.csv')
    selection = dict(created_at=datetime.now(timezone.utc).isoformat(), scenario=scenario, label=label, output_name=output_name, winner=winner, methods=list(METHODS), configurations=configurations, alpha=alpha, policies=selected_policies, test_responses_read=False, old_test_previously_inspected=PROTOCOL.get('old_test_previously_inspected', True), permuted_train_labels=permutation, fixed_from_primary=reference_selection is not None, protocol_sha256=checksum(ROOT / 'protocol.json'), preprocessing_manifest_sha256=checksum(preprocessing / 'manifest.json'), code_sha256={name: checksum(ROOT / 'src' / name) for name in ['train.py', 'recommenders.py', 'ranking_metrics.py', 'preprocess.py', 'pipeline_common.py']}, files={path.name: checksum(path) for path in destination.iterdir() if path.is_file()})
    verify_sources()
    write_json(destination / 'selection.json', selection)
    REPORTS.mkdir(parents=True, exist_ok=True)
    write_json(REPORTS / f'selection_{output_name}.json', selection)
    save_frame(ranking, REPORTS / f'validation_{output_name}.csv')
    print('Đã chốt bốn phương pháp; mặc định:', winner, '| alpha:', alpha, flush=True)
    return selection


if __name__ == '__main__':
    fit_and_select()
