"""Finite model search using only observed train responses and static text metadata."""
import re
import warnings

import numpy as np
from scipy import sparse
from scipy.optimize import minimize
from scipy.special import expit
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import normalize
from sklearn.exceptions import ConvergenceWarning
from threadpoolctl import threadpool_limits


def clean_text(value):
    value = re.sub(r"https?://\S+|www\.\S+", " ", str(value))
    return re.sub(r"\b(shorts|viral|subscribe|fyp)\b", " ", value, flags=re.IGNORECASE)


def build_features(catalogue):
    matrices, vectorisers = {}, {}
    for field in ["title", "tags", "description"]:
        texts = [clean_text(" ".join(r[field]) if isinstance(r[field], list) else r[field]) for r in catalogue]
        vectoriser = TfidfVectorizer(ngram_range=(1, 2), min_df=2, max_df=0.95,
                                    sublinear_tf=True, strip_accents="unicode", stop_words="english")
        matrices[field] = vectoriser.fit_transform(texts)
        vectorisers[field] = vectoriser
    title_tags = normalize(sparse.hstack([2 * matrices["title"], matrices["tags"]]).tocsr())
    full = normalize(sparse.hstack([2 * matrices["title"], matrices["tags"], 0.25 * matrices["description"]]).tocsr())
    svd = TruncatedSVD(n_components=min(16, full.shape[0]-1, full.shape[1]-1), random_state=42)
    embedding = normalize(svd.fit_transform(full))
    return {"title_tags": title_tags, "full": full, "embedding": embedding}, {"vectorisers": vectorisers, "svd": svd}


def normalise_scores(scores):
    low = scores.min(axis=1, keepdims=True)
    span = scores.max(axis=1, keepdims=True) - low
    return np.divide(scores-low, span, out=np.zeros_like(scores), where=span > 1e-12)


def factor_objective(parameters, labels, observed, factors, regularisation):
    user_count, item_count = labels.shape
    first, second = user_count*factors, (user_count+item_count)*factors
    users = parameters[:first].reshape(user_count, factors)
    items = parameters[first:second].reshape(item_count, factors)
    user_bias = parameters[second:second+user_count]
    item_bias = parameters[second+user_count:-1]
    logits = users @ items.T + user_bias[:, None] + item_bias[None, :] + parameters[-1]
    residuals = (expit(logits)-labels)*observed
    loss = ((np.logaddexp(0, logits)-labels*logits)*observed).sum()
    loss += 0.5*regularisation*((users**2).sum()+(items**2).sum())
    loss += 0.5*((user_bias**2).sum()+(item_bias**2).sum())
    gradient = np.r_[(residuals@items+regularisation*users).ravel(),
                     (residuals.T@users+regularisation*items).ravel(),
                     residuals.sum(axis=1)+user_bias, residuals.sum(axis=0)+item_bias, residuals.sum()]
    return float(loss), gradient


def fit_models(train, users, videos, features, global_indices, seed=42, required_models=None):
    needed = None if required_models is None else set(required_models)
    user_index = {u:i for i,u in enumerate(users)}
    video_index = {v:i for i,v in enumerate(videos)}
    row = train.user_id.map(user_index).to_numpy(int)
    column = train.video_id.map(video_index).to_numpy(int)
    values = train.positive_label.to_numpy(float)
    labels = np.zeros((len(users), len(videos)))
    observed = np.zeros_like(labels)
    labels[row, column] = values
    observed[row, column] = 1
    if train.duplicated(["user_id", "video_id"]).any():
        raise ValueError("Train có cặp trùng")
    mean = float(values.mean())
    item_rate = (labels.sum(axis=0)+10*mean)/(observed.sum(axis=0)+10)
    scores = {"Popularity": np.broadcast_to(labels.sum(axis=0), labels.shape).copy()}
    positive_count = labels.sum(axis=1, keepdims=True)
    negative = observed-labels
    negative_count = negative.sum(axis=1, keepdims=True)
    profile_weights = np.divide(labels, positive_count, out=np.zeros_like(labels), where=positive_count>0)
    profile_weights -= np.divide(negative, negative_count, out=np.zeros_like(labels), where=negative_count>0)
    for name, key in [("ContentFull", "full"), ("ContentTitleTags", "title_tags")]:
        matrix = features[key][global_indices]
        profiles = normalize(profile_weights @ matrix)
        scores[name] = np.asarray(profiles @ matrix.T)
    norms = np.sqrt((labels**2).sum(axis=0))
    similarity = labels.T @ labels / np.maximum(norms[:, None]*norms[None, :], 1e-12)
    np.fill_diagonal(similarity, 0)
    for target in range(len(videos)):
        order = np.argsort(-similarity[:, target], kind="stable")
        similarity[order[20:], target] = 0
    scores["ItemCF"] = np.divide(labels @ similarity, observed @ similarity,
                                  out=np.broadcast_to(item_rate, labels.shape).copy(), where=(observed @ similarity)>0)
    embedding = features["embedding"][global_indices]
    state = {"labels_train": labels, "observed_train": observed, "item_similarity": similarity}
    audits = []
    for c_value in [0.1, 1.0]:
        name = f"SVDContent_C{c_value:g}"
        if needed is not None and name not in needed:
            continue
        coefficient = np.zeros((len(users), embedding.shape[1]))
        intercept = np.zeros(len(users))
        for i in range(len(users)):
            columns = np.flatnonzero(observed[i])
            response = labels[i, columns]
            if len(columns)==0 or len(np.unique(response))<2:
                local_rate = (response.sum()+mean*2)/(len(response)+2)
                intercept[i] = np.log(np.clip(local_rate, 1e-6, 1-1e-6)/(1-np.clip(local_rate, 1e-6, 1-1e-6)))
                continue
            learner = LogisticRegression(C=c_value, solver="lbfgs", max_iter=1000, tol=1e-6)
            with warnings.catch_warnings(), threadpool_limits(limits=1):
                warnings.simplefilter("error", ConvergenceWarning)
                learner.fit(embedding[columns], response)
            coefficient[i], intercept[i] = learner.coef_[0], learner.intercept_[0]
        name = f"SVDContent_C{c_value:g}"
        scores[name] = coefficient @ embedding.T + intercept[:, None]
        state[name+"_coefficient"], state[name+"_intercept"] = coefficient, intercept
    for factors in [4, 8]:
        for regularisation in [5.0]:
            name = f"LogisticMF_d{factors}_r{regularisation:g}"
            if needed is not None and name not in needed:
                continue
            generator = np.random.default_rng(seed)
            initial = np.r_[generator.normal(0, 0.1, (len(users)+len(videos))*factors),
                            np.zeros(len(users)+len(videos)), np.log(mean/(1-mean))]
            with threadpool_limits(limits=1):
                result = minimize(factor_objective, initial, args=(labels, observed, factors, regularisation),
                                  jac=True, method="L-BFGS-B", options={"maxiter": 350, "ftol": 1e-6, "gtol": 1e-5})
            name = f"LogisticMF_d{factors}_r{regularisation:g}"
            audits.append({"model": name, "converged": bool(result.success), "iterations": int(result.nit),
                           "objective": float(result.fun), "termination":str(result.message),
                           "maximum_iterations":350, "relative_function_tolerance":1e-6})
            print("  MF", name, "| hội tụ:", result.success, "| vòng:", result.nit, flush=True)
            if not result.success or not np.isfinite(result.fun):
                continue
            first, second = len(users)*factors, (len(users)+len(videos))*factors
            u = result.x[:first].reshape(len(users), factors)
            v = result.x[first:second].reshape(len(videos), factors)
            bu = result.x[second:second+len(users)]
            bv = result.x[second+len(users):-1]
            scores[name] = u@v.T+bu[:, None]+bv[None, :]+result.x[-1]
            state[name+"_parameters"] = result.x
    if not all(np.isfinite(score).all() for score in scores.values()):
        raise ValueError("Điểm mô hình không hữu hạn")
    return scores, state, audits
