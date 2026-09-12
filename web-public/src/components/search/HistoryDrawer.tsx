import { RefObject, useEffect, useRef, useState } from "react";
import { Check, Copy, ExternalLink, History as HistoryIcon, X } from "lucide-react";

import { AvatarImage } from "./AvatarImage";
import { formatTimelineTime } from "./searchUtils";
import {
  displayTikTokUrl,
  formatHistoryTime,
  groupSearchHistory,
  SearchHistoryEntry,
} from "./searchHistory";

interface HistoryDrawerProps {
  open: boolean;
  entries: SearchHistoryEntry[];
  returnFocusRef: RefObject<HTMLButtonElement>;
  onClose: () => void;
  onClear: () => void;
}

interface HistoryEntryRowProps {
  entry: SearchHistoryEntry;
  tabIndex: number;
}

function HistoryEntryRow({ entry, tabIndex }: HistoryEntryRowProps) {
  const timestamp = formatTimelineTime(entry.timestampSeconds);
  const [copied, setCopied] = useState(false);

  const handleCopyTikTokUrl = async () => {
    await navigator.clipboard.writeText(entry.tiktokUrl);
    setCopied(true);
  };

  useEffect(() => {
    if (!copied) {
      return;
    }

    const timeout = window.setTimeout(() => setCopied(false), 2000);
    return () => window.clearTimeout(timeout);
  }, [copied]);

  return (
    <li className="rounded-xl border border-gray-700 bg-gray-800/70 p-4">
      <div className="flex min-w-0 items-start gap-3">
        <AvatarImage
          src={entry.profileImageUrl}
          alt={`${entry.streamer} avatar`}
          className="size-10 shrink-0 rounded-full border border-gray-700 object-cover"
        />

        <div className="min-w-0 flex-1">
          <p className="truncate text-xs font-semibold uppercase tracking-[0.16em] text-[#fb2844]">
            {entry.streamer}
          </p>
          <h3 className="mt-1 min-w-0 break-words text-base font-bold leading-tight text-white">
            <a
              href={entry.twitchUrlAtTimestamp}
              target="_blank"
              rel="noreferrer"
              tabIndex={tabIndex}
              title={`Open on Twitch at ${timestamp}`}
              className="group rounded-sm transition hover:text-gray-200 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#fb2844]"
            >
              {entry.streamTitle}
              <ExternalLink
                aria-hidden="true"
                className="ml-1 inline size-4 align-[-0.125em] text-gray-400 transition group-hover:text-[#fb2844]"
              />
            </a>
          </h3>
          <div className="mt-1 flex min-w-0 items-center gap-1">
            <p className="min-w-0 truncate text-sm text-gray-400" title={entry.tiktokUrl}>
              {displayTikTokUrl(entry.tiktokUrl)}
            </p>
            <button
              type="button"
              tabIndex={tabIndex}
              onClick={handleCopyTikTokUrl}
              aria-label={copied ? "TikTok URL copied" : "Copy TikTok URL"}
              className="flex size-7 shrink-0 items-center justify-center rounded-md text-gray-400 transition hover:bg-gray-700 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#fb2844]"
            >
              {copied ? (
                <Check aria-hidden="true" className="size-3.5 text-emerald-400" />
              ) : (
                <Copy aria-hidden="true" className="size-3.5" />
              )}
            </button>
          </div>
        </div>
      </div>

      <div className="mt-4 flex flex-wrap items-baseline gap-x-2 gap-y-1 border-t border-gray-700/80 pt-3">
        <span className="font-mono text-sm font-semibold text-white">{timestamp}</span>
        {entry.additionalMatchCount > 0 ? (
          <span className="text-sm text-gray-400">+{entry.additionalMatchCount} more</span>
        ) : null}
        <span className="ml-auto text-xs text-gray-500">{formatHistoryTime(entry.searchedAt)}</span>
      </div>
    </li>
  );
}

export function HistoryDrawer({ open, entries, returnFocusRef, onClose, onClear }: HistoryDrawerProps) {
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const wasOpenRef = useRef(false);
  const groups = groupSearchHistory(entries);

  useEffect(() => {
    if (!open) {
      if (wasOpenRef.current) {
        wasOpenRef.current = false;
        returnFocusRef.current?.focus();
      }
      return;
    }

    wasOpenRef.current = true;
    closeButtonRef.current?.focus();
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";

    return () => {
      document.body.style.overflow = previousOverflow;
    };
  }, [open, returnFocusRef]);

  useEffect(() => {
    if (!open) {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        onClose();
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [open, onClose]);

  const tabIndex = open ? 0 : -1;

  return (
    <>
      <button
        type="button"
        aria-label="Close history overlay"
        aria-hidden={open ? "false" : "true"}
        tabIndex={tabIndex}
        onClick={onClose}
        className={`fixed inset-0 z-40 bg-black/60 transition-opacity duration-300 ${
          open ? "opacity-100" : "pointer-events-none opacity-0"
        }`}
      />

      <aside
        id="vodhunter-history-drawer"
        role="dialog"
        aria-modal={open ? "true" : "false"}
        aria-hidden={open ? "false" : "true"}
        aria-labelledby="vodhunter-history-title"
        className={`fixed inset-y-0 right-0 z-50 flex w-full max-w-md flex-col border-l border-gray-700 bg-gray-900 shadow-2xl transition-transform duration-300 ease-out ${
          open ? "translate-x-0" : "pointer-events-none translate-x-full"
        }`}
      >
        <div className="flex items-center justify-between border-b border-gray-700 px-5 py-5 sm:px-6">
          <div>
            <h2 id="vodhunter-history-title" className="text-xl font-bold text-white">
              History
            </h2>
          </div>

          <div className="flex items-center gap-3">
            {entries.length > 0 ? (
              <button
                type="button"
                tabIndex={tabIndex}
                onClick={onClear}
                className="text-sm font-semibold text-gray-400 underline decoration-gray-600 underline-offset-4 transition hover:text-white hover:decoration-gray-300 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#fb2844]"
              >
                Clear all
              </button>
            ) : null}

            <button
              ref={closeButtonRef}
              type="button"
              aria-label="Close history"
              tabIndex={tabIndex}
              onClick={onClose}
              className="rounded-lg p-2 text-gray-400 transition hover:bg-gray-800 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#fb2844]"
            >
              <X aria-hidden="true" className="size-5" />
            </button>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-5 sm:px-6">
          {groups.length > 0 ? (
            <div className="space-y-7">
              {groups.map((group) => (
                <section key={group.key} aria-labelledby={`history-group-${group.key}`}>
                  <h3
                    id={`history-group-${group.key}`}
                    className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-gray-500"
                  >
                    {group.label}
                  </h3>
                  <ul className="space-y-3">
                    {group.entries.map((entry) => (
                      <HistoryEntryRow key={entry.id} entry={entry} tabIndex={tabIndex} />
                    ))}
                  </ul>
                </section>
              ))}
            </div>
          ) : (
            <div className="flex min-h-[260px] flex-col items-center justify-center text-center">
              <HistoryIcon aria-hidden="true" className="size-9 text-gray-600" />
              <h3 className="mt-4 text-lg font-semibold text-white">No search history</h3>
              <p className="mt-2 max-w-xs text-sm leading-6 text-gray-400">Your searches will appear here.</p>
            </div>
          )}
        </div>
      </aside>
    </>
  );
}
