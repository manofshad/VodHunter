import { useState } from "react";
import { ThumbsDown, ThumbsUp } from "lucide-react";

import { saveSearchFeedback } from "../../api/client";
import { CompletedSearchFeedback, MatchRatingValue, SearchFeedbackResponse } from "../../api/types";

type RatingState =
  | { kind: "ready"; saved: SearchFeedbackResponse | null }
  | { kind: "saving"; saved: SearchFeedbackResponse | null; attempted: MatchRatingValue }
  | { kind: "error"; saved: SearchFeedbackResponse | null; attempted: MatchRatingValue };

export function MatchRating({ search }: { search: CompletedSearchFeedback }) {
  const [state, setState] = useState<RatingState>({ kind: "ready", saved: search.feedback });

  const save = async (rating: MatchRatingValue) => {
    setState({ kind: "saving", saved: state.saved, attempted: rating });
    try {
      const saved = await saveSearchFeedback(search.searchToken, rating);
      setState({ kind: "ready", saved });
    } catch {
      setState({ kind: "error", saved: state.saved, attempted: rating });
    }
  };

  return (
    <div className="mt-2 flex flex-wrap items-center justify-between gap-2 border-t border-gray-700 pt-1">
      <div role="status" aria-live="polite" className="min-w-0 text-xs text-gray-400">
        {state.kind === "saving" ? "Saving feedback..." : state.kind === "error" ? (
          <span>
            Could not save feedback. {state.saved ? "Your previous rating is still saved. " : ""}
            <button type="button" className="underline underline-offset-4 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#fb2844]" onClick={() => void save(state.attempted)}>Retry</button>
          </span>
        ) : state.saved ? "Feedback saved." : "Was this match correct?"}
      </div>
      <div className="flex shrink-0 items-center gap-1.5" role="group" aria-label="Rate this match">
        {(["up", "down"] satisfies MatchRatingValue[]).map((rating) => {
          const Icon = rating === "up" ? ThumbsUp : ThumbsDown;
          const selected = state.saved?.rating === rating;
          return (
            <button
              key={rating}
              type="button"
              aria-label={rating === "up" ? "Thumbs up, correct match" : "Thumbs down, incorrect match"}
              aria-pressed={selected}
              disabled={state.kind === "saving"}
              onClick={() => void save(rating)}
              className={`inline-flex size-6 items-center justify-center transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#fb2844] disabled:cursor-wait disabled:opacity-60 ${
                selected ? "text-[#fb2844]" : "text-gray-500 hover:text-gray-200"
              }`}
            >
              <Icon aria-hidden="true" className={`size-3.5 ${selected ? "fill-current" : ""}`} />
            </button>
          );
        })}
      </div>
    </div>
  );
}
