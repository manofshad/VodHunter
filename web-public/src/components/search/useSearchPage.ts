import { FormEvent, RefObject, useEffect, useMemo, useRef, useState } from "react";

import { createSearchJob, getSearchJob, listSearchableStreamers } from "../../api/client";
import { SearchJobResponse, SearchResponse, StreamerListItem } from "../../api/types";
import { createSearchHistoryEntry, SearchHistoryEntry } from "./searchHistory";
import { isSupportedTikTokUrl } from "./searchUtils";
import { isSearchToken, parseSharedSearchLocation, sharedSearchPath } from "./sharedSearch";
import { useSearchHistory } from "./useSearchHistory";

const ACTIVE_SEARCH_STORAGE_KEY = "vodhunter-public-active-search";

interface ActiveSearchState {
  searchToken: string;
  tiktokUrl: string;
  streamedFrom?: string;
  streamedTo?: string;
}

function persistActiveSearch(searchToken: string, tiktokUrl: string, streamedFrom: string, streamedTo: string): void {
  window.localStorage.setItem(
    ACTIVE_SEARCH_STORAGE_KEY,
    JSON.stringify({ searchToken, tiktokUrl, streamedFrom, streamedTo }),
  );
}

function clearActiveSearch(): void {
  window.localStorage.removeItem(ACTIVE_SEARCH_STORAGE_KEY);
}

function readActiveSearch(): ActiveSearchState | null {
  const raw = window.localStorage.getItem(ACTIVE_SEARCH_STORAGE_KEY);
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as Partial<ActiveSearchState>;
    if (!isSearchToken(parsed.searchToken) || typeof parsed.tiktokUrl !== "string") {
      return null;
    }
    return {
      searchToken: parsed.searchToken,
      tiktokUrl: parsed.tiktokUrl,
      streamedFrom: typeof parsed.streamedFrom === "string" ? parsed.streamedFrom : "",
      streamedTo: typeof parsed.streamedTo === "string" ? parsed.streamedTo : "",
    };
  } catch {
    return null;
  }
}

export interface SearchPageState {
  tiktokUrl: string;
  streamedFrom: string;
  streamedTo: string;
  streamer: string;
  streamers: StreamerListItem[];
  loadingStreamers: boolean;
  streamerLoadError: string | null;
  result: SearchResponse | null;
  requestError: string | null;
  streamerError: string | null;
  submitting: boolean;
  activeSearchStage: string | null;
  lastSubmittedUrl: string;
  hasUrl: boolean;
  searchButtonLabel: string;
  streamerTriggerRef: RefObject<HTMLButtonElement>;
  historyEntries: SearchHistoryEntry[];
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onUrlChange: (value: string) => void;
  onPaste: () => Promise<void>;
  onSelectStreamer: (value: string) => void;
  onDateRangeChange: (streamedFrom: string, streamedTo: string) => void;
  onClearHistory: () => void;
}

export function useSearchPage(): SearchPageState {
  const [tiktokUrl, setTiktokUrl] = useState("");
  const [streamedFrom, setStreamedFrom] = useState("");
  const [streamedTo, setStreamedTo] = useState("");
  const [streamer, setStreamer] = useState("");
  const [streamers, setStreamers] = useState<StreamerListItem[]>([]);
  const [loadingStreamers, setLoadingStreamers] = useState(true);
  const [streamerLoadError, setStreamerLoadError] = useState<string | null>(null);
  const [result, setResult] = useState<SearchResponse | null>(null);
  const [requestError, setRequestError] = useState<string | null>(null);
  const [streamerError, setStreamerError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [activeSearchToken, setActiveSearchToken] = useState<string | null>(null);
  const [activeSearchStage, setActiveSearchStage] = useState<string | null>(null);
  const [lastSubmittedUrl, setLastSubmittedUrl] = useState("");
  const streamerTriggerRef = useRef<HTMLButtonElement>(null);
  const { entries: historyEntries, addEntry, clearEntries: onClearHistory } = useSearchHistory();

  const hasUrl = tiktokUrl.trim().length > 0;
  const searchButtonLabel = useMemo(() => (submitting ? "Searching..." : "Search"), [submitting]);

  useEffect(() => {
    let cancelled = false;

    const loadStreamers = async () => {
      try {
        setLoadingStreamers(true);
        setStreamerLoadError(null);
        const next = await listSearchableStreamers();
        if (cancelled) {
          return;
        }
        setStreamers(next);
        setStreamer((current) => {
          if (current && next.some((item) => item.name === current)) {
            return current;
          }
          return "";
        });
      } catch (err) {
        if (cancelled) {
          return;
        }
        setStreamerLoadError(err instanceof Error ? err.message : "Could not load streamers");
      } finally {
        if (!cancelled) {
          setLoadingStreamers(false);
        }
      }
    };

    void loadStreamers();

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const sharedSearch = parseSharedSearchLocation(window.location);
    if (sharedSearch.kind !== "none") {
      // A share link is explicit navigation and must never resume a different,
      // older search saved by this browser.
      clearActiveSearch();
      if (sharedSearch.kind === "invalid") {
        setRequestError("This VodHunter search link is invalid.");
        return;
      }

      setActiveSearchToken(sharedSearch.searchToken);
      setActiveSearchStage("validating");
      setSubmitting(true);
      return;
    }

    const activeSearch = readActiveSearch();
    if (activeSearch === null) {
      return;
    }

    setActiveSearchToken(activeSearch.searchToken);
    setActiveSearchStage("validating");
    setLastSubmittedUrl(activeSearch.tiktokUrl);
    setTiktokUrl(activeSearch.tiktokUrl);
    setStreamedFrom(activeSearch.streamedFrom ?? "");
    setStreamedTo(activeSearch.streamedTo ?? "");
    setSubmitting(true);
  }, []);

  useEffect(() => {
    if (activeSearchToken === null) {
      return;
    }

    let cancelled = false;

    const poll = async () => {
      try {
        const job = await getSearchJob(activeSearchToken);
        if (cancelled) {
          return;
        }
        handlePolledJob(job);
      } catch (err) {
        if (cancelled) {
          return;
        }
        setSubmitting(false);
        setActiveSearchToken(null);
        setActiveSearchStage(null);
        clearActiveSearch();
        setRequestError(err instanceof Error ? err.message : "Search failed");
      }
    };

    void poll();
    const intervalId = window.setInterval(() => {
      void poll();
    }, 2000);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [activeSearchToken]);

  const handlePolledJob = (job: SearchJobResponse) => {
    setActiveSearchStage(job.stage);
    if (job.tiktok_url) {
      setTiktokUrl(job.tiktok_url);
      setLastSubmittedUrl(job.tiktok_url);
    }
    if (job.streamer) {
      setStreamer(job.streamer);
    }

    if (job.status === "queued" || job.status === "running") {
      setSubmitting(true);
      return;
    }

    setSubmitting(false);
    setActiveSearchToken(null);
    setActiveSearchStage(null);
    clearActiveSearch();

    if (job.status === "completed") {
      setResult(job.result);
      setRequestError(null);
      if (job.result) {
        const historyEntry = createSearchHistoryEntry(
          job.result,
          job.tiktok_url ?? lastSubmittedUrl,
          job.created_at,
        );
        if (historyEntry) {
          addEntry(historyEntry);
        }
      }
      return;
    }

    setResult(null);
    setRequestError(job.error?.message ?? "Search failed");
  };

  const onSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!hasUrl) {
      return;
    }

    const submittedUrl = tiktokUrl.trim();
    if (!isSupportedTikTokUrl(submittedUrl)) {
      setRequestError("Paste a TikTok video link, not a profile or other TikTok page.");
      return;
    }

    if (!streamer.trim()) {
      setStreamerError("Select a streamer to run the search.");
      streamerTriggerRef.current?.focus();
      return;
    }

    const submittedStreamedFrom = streamedFrom.trim();
    const submittedStreamedTo = streamedTo.trim();

    try {
      setSubmitting(true);
      setActiveSearchToken(null);
      setActiveSearchStage("validating");
      setStreamerError(null);
      setRequestError(null);
      setResult(null);
      setLastSubmittedUrl(submittedUrl);
      const created = await createSearchJob({
        tiktokUrl: submittedUrl,
        streamer,
        streamedFrom: submittedStreamedFrom || undefined,
        streamedTo: submittedStreamedTo || undefined,
      });
      if (!isSearchToken(created.search_token)) {
        throw new Error("Could not open the search. Please refresh and try again.");
      }
      persistActiveSearch(created.search_token, submittedUrl, submittedStreamedFrom, submittedStreamedTo);
      window.history.replaceState(null, "", sharedSearchPath(created.search_token));
      setActiveSearchToken(created.search_token);
      setActiveSearchStage(created.stage);
    } catch (err) {
      setActiveSearchStage(null);
      setRequestError(err instanceof Error ? err.message : "Search failed");
      setSubmitting(false);
    }
  };

  const onUrlChange = (value: string) => {
    setTiktokUrl(value);
    setStreamerError(null);
    setRequestError(null);
  };

  const onSelectStreamer = (value: string) => {
    setStreamer(value);
    setStreamerError(null);
  };

  const onDateRangeChange = (nextFrom: string, nextTo: string) => {
    setStreamedFrom(nextFrom);
    setStreamedTo(nextTo);
    setRequestError(null);
  };

  const onPaste = async () => {
    try {
      const text = await navigator.clipboard.readText();
      setTiktokUrl(text);
      setStreamerError(null);
    } catch {
      setRequestError("Clipboard access was blocked. Paste the TikTok URL manually.");
    }
  };

  return {
    tiktokUrl,
    streamedFrom,
    streamedTo,
    streamer,
    streamers,
    loadingStreamers,
    streamerLoadError,
    result,
    requestError,
    streamerError,
    submitting,
    activeSearchStage,
    lastSubmittedUrl,
    hasUrl,
    searchButtonLabel,
    streamerTriggerRef,
    historyEntries,
    onSubmit,
    onUrlChange,
    onPaste,
    onSelectStreamer,
    onDateRangeChange,
    onClearHistory,
  };
}
