"""Logged-positive ranking metrics, explicit candidate pools, paired comparisons."""
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score
from recommenders import normalise_scores


def candidate_mask(users, videos, prior):
    ui, vi = {u:i for i,u in enumerate(users)}, {v:i for i,v in enumerate(videos)}
    mask = np.ones((len(users), len(videos)), dtype=bool)
    for row in prior.itertuples():
        if row.user_id in ui and row.video_id in vi:
            mask[ui[row.user_id], vi[row.video_id]] = False
    return mask


def rank_scores(scores, allowed, k=10):
    ranked = []
    for row in range(len(scores)):
        items = np.flatnonzero(allowed[row])
        ranked.append(items[np.lexsort((items, -scores[row, items]))][:k])
    return ranked


def balance_rankings(scores, allowed, eligible, target=0.9, k=10, swaps_limit=2, budget=0.02):
    scaled = normalise_scores(scores)
    ranking = rank_scores(scores, allowed, k)
    selected = np.zeros_like(allowed)
    for row in np.flatnonzero(eligible):
        selected[row, ranking[row]] = True
    counts = selected.sum(axis=0)
    initial = selected.copy()
    initial_sum = (scaled*selected).sum(axis=1)
    current_sum = initial_sum.copy()
    changes = np.zeros(len(scores), dtype=int)
    target_count = int(np.ceil(target*len(counts)))
    while (counts>0).sum() < target_count:
        removal_scores = np.where(selected & (counts[None, :]>1), scaled, np.inf)
        removal = removal_scores.argmin(axis=1)
        removal_score = removal_scores[np.arange(len(scores)), removal]
        cost = removal_score[:, None]-scaled
        used = initial_sum[:, None]-current_sum[:, None]+cost
        feasible = allowed & (counts[None, :]==0) & eligible[:, None] & (changes[:, None]<swaps_limit)
        feasible &= np.isfinite(removal_score[:, None]) & (used<=k*budget+1e-12)
        costs = np.where(feasible, cost, np.inf)
        user, item = np.unravel_index(np.argmin(costs), costs.shape)
        if not np.isfinite(costs[user, item]):
            break
        previous = removal[user]
        selected[user, previous] = False
        selected[user, item] = True
        counts[previous] -= 1
        counts[item] += 1
        current_sum[user] += scaled[user, item]-scaled[user, previous]
        changes[user] += 1
    losses = (initial_sum-current_sum)/k
    if np.any(changes>swaps_limit) or np.any(losses>budget+1e-10) or np.any(selected & ~allowed):
        raise ValueError("Phân bổ vượt ngân sách")
    for row in np.flatnonzero(eligible):
        items = np.flatnonzero(selected[row])
        if len(items) != k:
            raise ValueError("Top-K không đủ")
        ranking[row] = items[np.lexsort((items, -scores[row, items]))]
    return ranking, {"mean_swaps": float(changes[eligible].mean()) if eligible.any() else 0,
                     "maximum_score_loss": float(losses.max()), "maximum_swaps": int(changes.max())}


def evaluate(scores, users, videos, allowed, train_counts, responses, cohort, model,
             rankings=None, label="positive_label", k=10):
    vi = {v:i for i,v in enumerate(videos)}
    groups = {u:g for u,g in responses.groupby("user_id")}
    eligible = np.array([train_counts.get(u, 0)>=5 for u in users]) & (allowed.sum(axis=1)>=k)
    rankings = rank_scores(scores, allowed, k) if rankings is None else rankings
    discounts = 1/np.log2(np.arange(2, k+2))
    rows, recommendations = [], []
    for index, user in enumerate(users):
        if not eligible[index]:
            continue
        candidates = allowed[index].sum()
        observed = groups.get(user)
        positive = set()
        auc = float("nan")
        if observed is not None:
            positive = {vi[v] for v in observed.loc[observed[label].eq(1), "video_id"]}
            observed_indices = observed.video_id.map(vi).to_numpy(int)
            if not allowed[index, observed_indices].all():
                raise ValueError("Heldout overlap prior pairs")
            if observed[label].nunique()==2:
                auc = roc_auc_score(observed[label], scores[index, observed_indices])
        ranking = rankings[index]
        if len(ranking)!=k or len(set(ranking))!=k or not allowed[index, ranking].all():
            raise ValueError("Ranking sai điều kiện ứng viên")
        relevance = np.array([int(item in positive) for item in ranking])
        hits = int(relevance.sum())
        precision = hits/k
        recall = hits/len(positive) if positive else 0
        ideal = discounts[:min(k,len(positive))].sum()
        ndcg = float(relevance @ discounts / ideal) if ideal else 0
        rows.append({"cohort": cohort, "model": model, "user_id": user, "candidate_count": int(candidates),
                     "test_positive_count": len(positive), "observed_response_count": 0 if observed is None else len(observed),
                     "precision_at_10": precision, "recall_at_10": recall,
                     "f1_at_10": 2*precision*recall/(precision+recall) if precision+recall else 0,
                     "ndcg_at_10": ndcg, "observed_auc": auc,
                     "random_expected_precision": len(positive)/candidates,
                     "random_expected_recall": k/candidates if positive else 0,
                     "random_expected_ndcg": float(len(positive)/candidates*discounts.sum()/ideal) if ideal else 0,
                     "oracle_precision_at_10": min(k,len(positive))/k,
                     "oracle_recall_at_10": min(k,len(positive))/len(positive) if positive else 0})
        recommendations.extend({"cohort":cohort, "model":model, "user_id":user,
                                "video_id":videos[item], "rank":rank+1, "score":float(scores[index,item]),
                                "logged_positive":int(item in positive)} for rank,item in enumerate(ranking))
    metrics = pd.DataFrame(rows)
    recs = pd.DataFrame(recommendations)
    positive_metrics = metrics.loc[metrics.test_positive_count.gt(0)] if len(metrics) else metrics
    summary = {"cohort":cohort, "model":model, "eligible_users":len(metrics),
               "evaluated_users":len(positive_metrics), "users_without_positives":len(metrics)-len(positive_metrics),
               "excluded_cold_or_small_pool":int((~eligible).sum()),
               "catalogue_size":len(videos), "coverage":recs.video_id.nunique()/len(videos) if len(recs) else 0,
               "concentration":float(recs.video_id.value_counts().head(10).sum()/len(recs)) if len(recs) else 0}
    for name in ["precision_at_10","recall_at_10","f1_at_10","ndcg_at_10","observed_auc",
                 "candidate_count","test_positive_count","random_expected_precision","random_expected_recall",
                 "random_expected_ndcg","oracle_precision_at_10","oracle_recall_at_10"]:
        summary[name] = float(positive_metrics[name].mean()) if len(positive_metrics) else float("nan")
    return summary, metrics, recs


def macro_summary(frame):
    metrics = ["precision_at_10","recall_at_10","f1_at_10","ndcg_at_10","coverage","concentration", "observed_auc"]
    return frame.groupby("model")[metrics].mean().reset_index()
