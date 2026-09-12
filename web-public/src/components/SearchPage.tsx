import { RefObject, useRef, useState } from "react";
import { History as HistoryIcon } from "lucide-react";

import { HistoryDrawer } from "./search/HistoryDrawer";
import { SearchFeedback } from "./search/SearchFeedback";
import { SearchForm } from "./search/SearchForm";
import { useSearchPage } from "./search/useSearchPage";

export { DateRangePicker } from "./search/DateRangePicker";
export { SearchResultCard } from "./search/SearchResults";
export { formatTimelineTime, isSupportedTikTokUrl } from "./search/searchUtils";

interface HeaderProps {
  historyOpen: boolean;
  historyButtonRef: RefObject<HTMLButtonElement>;
  onOpenHistory: () => void;
}

function Header({ historyOpen, historyButtonRef, onOpenHistory }: HeaderProps) {
  return (
    <header className="border-b border-gray-700 bg-gray-900 px-6 py-4">
      <div className="mx-auto flex max-w-7xl items-center justify-between">
        <div className="flex items-center gap-2" aria-label="VodHunter">
          <span className="text-xl font-bold text-white">
            <span className="font-bold">Vod</span>
            <span className="font-bold text-[#fb2844]">Hunter</span>
          </span>
        </div>
        <nav>
          <button
            ref={historyButtonRef}
            type="button"
            aria-label="History"
            aria-expanded={historyOpen}
            aria-controls="vodhunter-history-drawer"
            onClick={onOpenHistory}
            className="inline-flex items-center gap-2 rounded-lg px-3 py-2 text-sm font-semibold text-gray-300 transition hover:bg-gray-800 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#fb2844]"
          >
            <HistoryIcon aria-hidden="true" className="size-5" />
            <span className="hidden sm:inline">History</span>
          </button>
        </nav>
      </div>
    </header>
  );
}

function FeatureGrid() {
  const features = [
    {
      title: "Find The Exact VOD Moment",
      description: "Paste a TikTok clip link and match it to the exact timestamp inside a Twitch VOD.",
    },
    {
      title: "Audio-Based Matching",
      description:
        "VodHunter uses neural audio fingerprints and timeline alignment, so it can recognize supported moments across edited clips.",
    },
    {
      title: "Search Hours In Seconds",
      description: "Skip manual scrubbing through long streams and go straight to the source moment.",
    },
  ];

  return (
    <div className="bg-gray-900 px-6 py-20">
      <div className="mx-auto max-w-6xl">
        <div className="grid grid-cols-1 gap-8 md:grid-cols-3">
          {features.map((feature) => (
            <div key={feature.title} className="text-center">
              <h3 className="mb-4 text-2xl font-bold text-white">{feature.title}</h3>
              <p className="text-gray-300 leading-relaxed">{feature.description}</p>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export default function SearchPage() {
  const page = useSearchPage();
  const [historyOpen, setHistoryOpen] = useState(false);
  const historyButtonRef = useRef<HTMLButtonElement>(null);

  return (
    <div className="min-h-screen bg-gray-900">
      <Header
        historyOpen={historyOpen}
        historyButtonRef={historyButtonRef}
        onOpenHistory={() => setHistoryOpen(true)}
      />

      <HistoryDrawer
        open={historyOpen}
        entries={page.historyEntries}
        returnFocusRef={historyButtonRef}
        onClose={() => setHistoryOpen(false)}
        onClear={page.onClearHistory}
      />

      <main>
        <div
          className="relative px-6 py-24 md:py-28"
          style={{ background: "linear-gradient(160deg, #fb2844 0%, #f55b70 100%)" }}
        >
          <div className="mx-auto max-w-6xl text-center">
            <div className="mx-auto max-w-4xl">
              <h1 className="mb-12 text-3xl font-bold text-white">Find That Exact Moment</h1>

              <SearchForm
                tiktokUrl={page.tiktokUrl}
                streamer={page.streamer}
                streamers={page.streamers}
                loadingStreamers={page.loadingStreamers}
                streamerLoadError={page.streamerLoadError}
                streamerError={page.streamerError}
                submitting={page.submitting}
                hasUrl={page.hasUrl}
                searchButtonLabel={page.searchButtonLabel}
                streamedFrom={page.streamedFrom}
                streamedTo={page.streamedTo}
                streamerTriggerRef={page.streamerTriggerRef}
                onSubmit={page.onSubmit}
                onUrlChange={page.onUrlChange}
                onPaste={page.onPaste}
                onSelectStreamer={page.onSelectStreamer}
                onDateRangeChange={page.onDateRangeChange}
              >
                <SearchFeedback
                  requestError={page.requestError}
                  submitting={page.submitting}
                  activeSearchStage={page.activeSearchStage}
                  result={page.result}
                  lastSubmittedUrl={page.lastSubmittedUrl}
                />
              </SearchForm>
            </div>
          </div>
        </div>

        <FeatureGrid />
      </main>
    </div>
  );
}
