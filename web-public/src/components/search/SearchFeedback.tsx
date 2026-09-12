import { AlertCircle, LoaderCircle, Search } from "lucide-react";

import { SearchResponse } from "../../api/types";
import { SearchResultCard } from "./SearchResults";
import { getStageMessage } from "./searchUtils";

interface SearchFeedbackProps {
  requestError: string | null;
  submitting: boolean;
  activeSearchStage: string | null;
  result: SearchResponse | null;
  lastSubmittedUrl: string;
}

export function SearchFeedback({
  requestError,
  submitting,
  activeSearchStage,
  result,
  lastSubmittedUrl,
}: SearchFeedbackProps) {
  if (!requestError && !submitting && !result) {
    return null;
  }

  return (
    <div className="mt-0 space-y-6">
      {requestError ? (
        <div className="mx-auto max-w-4xl rounded-xl border border-red-400/20 bg-gray-900 p-5 text-left shadow-lg">
          <div className="flex items-start gap-3">
            <AlertCircle className="mt-0.5 size-5 shrink-0 text-red-100" />
            <div>
              <p className="text-sm font-semibold uppercase tracking-[0.18em] text-red-100">Search error</p>
              <p className="mt-2 text-sm leading-6 text-white/90">{requestError}</p>
            </div>
          </div>
        </div>
      ) : null}

      {submitting ? (
        <div className="mx-auto max-w-4xl rounded-xl border border-gray-700 bg-gray-900 p-6 text-center shadow-lg">
          <LoaderCircle className="mx-auto size-7 animate-spin text-[#fb2844]" />
          <p className="mt-3 text-base font-semibold text-white">Searching Twitch VODs...</p>
          <p className="mt-2 text-sm text-gray-400">{getStageMessage(activeSearchStage)}</p>
        </div>
      ) : null}

      {!submitting && result ? <SearchResultCard result={result} lastSubmittedUrl={lastSubmittedUrl} /> : null}

      {!submitting && result && !result.found ? (
        <div className="mx-auto max-w-4xl rounded-xl border border-gray-700 bg-gray-900 p-6 text-center shadow-lg">
          <Search className="mx-auto size-7 text-gray-400" />
          <h2 className="mt-4 text-2xl font-bold text-white">No exact match yet</h2>
          <p className="mx-auto mt-3 max-w-2xl text-base leading-7 text-gray-300">
            Try another TikTok URL or confirm that you selected the right streamer before searching again.
          </p>
        </div>
      ) : null}
    </div>
  );
}
