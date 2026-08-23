from __future__ import annotations

from collections import defaultdict

import numpy as np

from .models import AlignmentOutcome


def _normalize_rows(values: np.ndarray) -> np.ndarray:
    values = values.astype(np.float32, copy=False)
    norms = np.linalg.norm(values, axis=1, keepdims=True)
    return values / np.maximum(norms, 1e-10)


def temporal_vector_alignment(
    query_embeddings: np.ndarray,
    query_timestamps: np.ndarray,
    database_embeddings: np.ndarray,
    database_timestamps: np.ndarray,
    *,
    top_k: int,
    min_votes: int,
    min_vote_ratio: float,
    offset_bin_seconds: float,
) -> AlignmentOutcome:
    if query_embeddings.size == 0 or database_embeddings.size == 0:
        return AlignmentOutcome(False, None, None, "No embeddings", None, None, {})
    if len(query_embeddings) != len(query_timestamps):
        raise ValueError("Query embeddings and timestamps are misaligned")
    if len(database_embeddings) != len(database_timestamps):
        raise ValueError("Database embeddings and timestamps are misaligned")

    q = _normalize_rows(query_embeddings)
    d = _normalize_rows(database_embeddings)
    k = min(max(1, int(top_k)), len(d))
    similarities = np.dot(q, d.T)
    candidate_indices = np.argpartition(similarities, -k, axis=1)[:, -k:]
    candidate_scores = np.take_along_axis(similarities, candidate_indices, axis=1)

    votes: dict[float, int] = defaultdict(int)
    score_sums: dict[float, float] = defaultdict(float)
    for query_index in range(len(q)):
        for candidate_index, score in zip(candidate_indices[query_index], candidate_scores[query_index]):
            raw_offset = float(database_timestamps[candidate_index] - query_timestamps[query_index])
            bucket = round(raw_offset / offset_bin_seconds) * offset_bin_seconds
            votes[bucket] += 1
            score_sums[bucket] += float(score)

    ranked = sorted(votes, key=lambda key: (votes[key], score_sums[key]), reverse=True)
    if not ranked:
        return AlignmentOutcome(False, None, None, "No temporal candidates", None, None, {})
    best_offset = float(ranked[0])
    best_votes = votes[best_offset]
    vote_ratio = best_votes / float(len(q))
    mean_score = score_sums[best_offset] / best_votes
    accepted = best_votes >= min_votes and vote_ratio >= min_vote_ratio
    reason = (
        f"Accepted with {best_votes} votes ({vote_ratio:.3f} ratio)"
        if accepted
        else f"Rejected with {best_votes} votes ({vote_ratio:.3f} ratio)"
    )
    top_offsets = [
        {
            "start_seconds": float(offset),
            "votes": votes[offset],
            "vote_ratio": votes[offset] / float(len(q)),
            "mean_similarity": score_sums[offset] / votes[offset],
        }
        for offset in ranked[:5]
    ]
    return AlignmentOutcome(
        found=accepted,
        start_seconds=best_offset if accepted else None,
        confidence=vote_ratio,
        reason=reason,
        raw_start_seconds=best_offset,
        raw_score=mean_score,
        diagnostics={"best_votes": best_votes, "query_fingerprints": len(q), "top_offsets": top_offsets},
    )
