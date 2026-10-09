import { RefObject, useRef, useState } from "react";
import { History as HistoryIcon, Smartphone } from "lucide-react";

import { HistoryDrawer } from "./search/HistoryDrawer";
import { SearchFeedback } from "./search/SearchFeedback";
import { SearchForm } from "./search/SearchForm";
import { ShortcutDialog } from "./search/ShortcutDialog";
import { useSearchPage } from "./search/useSearchPage";

export { DateRangePicker } from "./search/DateRangePicker";
export { SearchResultCard } from "./search/SearchResults";
export { formatTimelineTime, isSupportedTikTokUrl } from "./search/searchUtils";

type OpenPanel = "history" | "shortcut" | null;

interface HeaderProps {
  openPanel: OpenPanel;
  historyButtonRef: RefObject<HTMLButtonElement>;
  shortcutButtonRef: RefObject<HTMLButtonElement>;
  onOpenHistory: () => void;
  onOpenShortcut: () => void;
}

function Header({ openPanel, historyButtonRef, shortcutButtonRef, onOpenHistory, onOpenShortcut }: HeaderProps) {
  return (
    <header className="border-b border-gray-700 bg-gray-900 px-3 py-4 sm:px-6">
      <div className="mx-auto flex max-w-7xl items-center justify-between gap-2">
        <div className="flex items-center gap-2" aria-label="VodHunter">
          <span className="text-lg font-bold text-white sm:text-xl">
            <span className="font-bold">Vod</span>
            <span className="font-bold text-[#fb2844]">Hunter</span>
          </span>
        </div>
        <nav aria-label="Search tools" className="flex shrink-0 items-center gap-1 sm:gap-2">
          <button
            ref={shortcutButtonRef}
            type="button"
            aria-haspopup="dialog"
            aria-expanded={openPanel === "shortcut"}
            aria-controls="vodhunter-shortcut-dialog"
            onClick={onOpenShortcut}
            className="inline-flex min-h-11 items-center gap-1.5 rounded-lg border border-gray-700 bg-gray-800 px-2 text-xs font-semibold text-gray-200 transition hover:border-[#fb2844] hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#fb2844] sm:gap-2 sm:px-3 sm:text-sm"
          >
            <Smartphone aria-hidden="true" className="size-4 shrink-0 sm:size-5" />
            <span>iPhone shortcut</span>
          </button>
          <button
            ref={historyButtonRef}
            type="button"
            aria-label="History"
            aria-expanded={openPanel === "history"}
            aria-controls="vodhunter-history-drawer"
            onClick={onOpenHistory}
            className="inline-flex min-h-11 min-w-11 items-center justify-center gap-2 rounded-lg px-2 py-2 text-sm font-semibold text-gray-300 transition hover:bg-gray-800 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#fb2844] sm:px-3"
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
      title: "Paste the Clip",
      description: "Add the TikTok link containing the moment you want to find.",
    },
    {
      title: "Match the Audio",
      description:
        "VodHunter compares the clip’s audio against the streamer’s VODs to locate the matching moment.",
    },
    {
      title: "Jump to the Source",
      description: "Go straight to the original Twitch VOD at the exact timestamp.",
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
  const [openPanel, setOpenPanel] = useState<OpenPanel>(null);
  const historyButtonRef = useRef<HTMLButtonElement>(null);
  const shortcutButtonRef = useRef<HTMLButtonElement>(null);

  return (
    <div className="min-h-screen bg-gray-900">
      <Header
        openPanel={openPanel}
        historyButtonRef={historyButtonRef}
        shortcutButtonRef={shortcutButtonRef}
        onOpenHistory={() => setOpenPanel("history")}
        onOpenShortcut={() => setOpenPanel("shortcut")}
      />

      <HistoryDrawer
        open={openPanel === "history"}
        entries={page.historyEntries}
        returnFocusRef={historyButtonRef}
        onClose={() => setOpenPanel(null)}
        onClear={page.onClearHistory}
      />

      <ShortcutDialog
        open={openPanel === "shortcut"}
        returnFocusRef={shortcutButtonRef}
        onClose={() => setOpenPanel(null)}
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
