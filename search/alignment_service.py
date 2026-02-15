from collections import Counter
from dataclasses import dataclass
import numpy as np

from search.models import AlignmentResult
from storage.vector_store import VectorStore


@dataclass(frozen=True)
class AlignmentConfig:
    min_vote_count: int = 3
    min_vote_ratio: float = 0.08


class AlignmentService:
    def __init__(
        self,
        store: VectorStore,
        config: AlignmentConfig | None = None,
    ):
        self.store = store
        self.config = config or AlignmentConfig()

    def align(
        self,
        neighbor_ids: np.ndarray,
        query_timestamps: np.ndarray,
    ) -> AlignmentResult:
        if neighbor_ids.size == 0:
            return AlignmentResult(found=False, reason="No nearest neighbors found")
        if query_timestamps.size == 0:
            return AlignmentResult(found=False, reason="Query had no timestamps")
        if neighbor_ids.shape[0] != len(query_timestamps):
            return AlignmentResult(found=False, reason="Neighbor/timestamp length mismatch")

        flat_ids = [int(v) for v in neighbor_ids.reshape(-1).tolist()]
        rows = self.store.get_fingerprint_rows(flat_ids)
        if not rows:
            return AlignmentResult(found=False, reason="No fingerprint rows resolved")

        id_to_row = {row_id: (video_id, ts) for row_id, video_id, ts in rows}
        votes: Counter[tuple[int, int]] = Counter()

        for i, row_neighbors in enumerate(neighbor_ids):
            q_time = float(query_timestamps[i])
            for fp_id in row_neighbors:
                resolved = id_to_row.get(int(fp_id))
                if resolved is None:
                    continue
                video_id, db_time = resolved
                offset_seconds = int(round(db_time - q_time))
                votes[(video_id, offset_seconds)] += 1

        if not votes:
            return AlignmentResult(found=False, reason="No alignment candidates")

        (best_video_id, best_offset), best_votes = votes.most_common(1)[0]
        vote_ratio = best_votes / float(len(query_timestamps))

        if best_votes < self.config.min_vote_count:
            return AlignmentResult(
                found=False,
                reason=(
                    f"Best candidate vote count {best_votes} is below min_vote_count "
                    f"{self.config.min_vote_count}"
                ),
            )

        if vote_ratio < self.config.min_vote_ratio:
            return AlignmentResult(
                found=False,
                reason=(
                    f"Best candidate vote ratio {vote_ratio:.3f} is below min_vote_ratio "
                    f"{self.config.min_vote_ratio:.3f}"
                ),
            )

        return AlignmentResult(
            found=True,
            video_id=best_video_id,
            timestamp_seconds=best_offset,
            score=vote_ratio,
            reason=f"Accepted with {best_votes} votes ({vote_ratio:.3f} ratio)",
        )
