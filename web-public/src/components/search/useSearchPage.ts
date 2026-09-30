import { FormEvent, RefObject, useEffect, useMemo, useRef, useState } from "react";

import { createSearchJob, getSearchJob, listSearchableStreamers } from "../../api/client";
import { SearchJobResponse, SearchResponse, StreamerListItem } from "../../api/types";
import { createSearchHistoryEntry, SearchHistoryEntry } from "./searchHistory";
import { isSupportedTikTokUrl } from "./searchUtils";
import { parseSharedSearchLocation, sharedSearchPath } from "./sharedSearch";
import { useSearchHistory } from "./useSearchHistory";
import { journeyAttributes, newJourney, requestFailureDetails, trackEvent, type SearchJourney } from "../../telemetry";

const ACTIVE_SEARCH_STORAGE_KEY = "vodhunter-public-active-search";

interface ActiveSearchState {
  searchId: number;
  tiktokUrl: string;
  streamedFrom?: string;
  streamedTo?: string;
}

function persistActiveSearch(searchId: number, tiktokUrl: string, streamedFrom: string, streamedTo: string): void {
  window.localStorage.setItem(
    ACTIVE_SEARCH_STORAGE_KEY,
    JSON.stringify({ searchId, tiktokUrl, streamedFrom, streamedTo }),
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
    if (typeof parsed.searchId !== "number" || typeof parsed.tiktokUrl !== "string") {
      return null;
    }
    return {
      searchId: parsed.searchId,
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
  onResultClick: (linkKind: "source" | "segment") => void;
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
  const [activeSearchId, setActiveSearchId] = useState<number | null>(null);
  const [activeSearchStage, setActiveSearchStage] = useState<string | null>(null);
  const [lastSubmittedUrl, setLastSubmittedUrl] = useState("");
  const streamerTriggerRef = useRef<HTMLButtonElement>(null);
  const { entries: historyEntries, addEntry, clearEntries: onClearHistory } = useSearchHistory();
  const journey = useRef<SearchJourney | null>(null);
  const resultJourney = useRef<SearchJourney | null>(null);
  const terminalEvent = useRef<Record<string, unknown> | null>(null);
  const terminalHandled = useRef(false);
  const pageReadyRecorded = useRef(false);
  const lastObservedStage = useRef<string | null>(null);

  useEffect(() => {
    if (!loadingStreamers && !pageReadyRecorded.current) {
      pageReadyRecorded.current = true;
      trackEvent(streamerLoadError ? "streamer_list_failed" : "search_page_ready");
    }
  }, [loadingStreamers, streamerLoadError]);

  useEffect(() => {
    if (activeSearchStage && activeSearchStage !== lastObservedStage.current && journey.current) {
      lastObservedStage.current = activeSearchStage;
      trackEvent("search_stage_changed", { ...journeyAttributes(journey.current), stage: activeSearchStage });
    }
  }, [activeSearchStage]);

  useEffect(() => {
    if (!submitting && terminalEvent.current && journey.current) {
      trackEvent("search_result_visible", { ...journeyAttributes(journey.current), ...terminalEvent.current });
      terminalEvent.current = null;
    }
  }, [submitting, result, requestError]);

  useEffect(() => {
    if (!submitting) return;
    const observeVisibility = () => {
      if (journey.current) trackEvent(document.hidden ? "search_view_hidden" : "search_view_returned", journeyAttributes(journey.current));
    };
    document.addEventListener("visibilitychange", observeVisibility);
    return () => document.removeEventListener("visibilitychange", observeVisibility);
  }, [submitting]);

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

      setActiveSearchId(sharedSearch.searchId);
      if (!journey.current) {
        journey.current = newJourney("shared", sharedSearch.searchId);
        trackEvent("search_resumed", journeyAttributes(journey.current));
      }
      setActiveSearchStage("validating");
      setSubmitting(true);
      return;
    }

    const activeSearch = readActiveSearch();
    if (activeSearch === null) {
      return;
    }

    setActiveSearchId(activeSearch.searchId);
    if (!journey.current) {
      journey.current = newJourney("resumed", activeSearch.searchId);
      trackEvent("search_resumed", journeyAttributes(journey.current));
    }
    setActiveSearchStage("validating");
    setLastSubmittedUrl(activeSearch.tiktokUrl);
    setTiktokUrl(activeSearch.tiktokUrl);
    setStreamedFrom(activeSearch.streamedFrom ?? "");
    setStreamedTo(activeSearch.streamedTo ?? "");
    setSubmitting(true);
  }, []);

  useEffect(() => {
    if (activeSearchId === null) {
      return;
    }

    let cancelled = false;
    let inFlight = false;

    const poll = async () => {
      if (inFlight || cancelled || terminalHandled.current) return;
      inFlight = true;
      try {
        const job = await getSearchJob(activeSearchId);
        if (cancelled) {
          return;
        }
        handlePolledJob(job);
      } catch (err) {
        if (cancelled) {
          return;
        }
        terminalHandled.current = true;
        terminalEvent.current = { outcome: "error", reason: "poll_failed" };
        if (journey.current) trackEvent("search_poll_failed", { ...journeyAttributes(journey.current), ...requestFailureDetails(err) });
        setSubmitting(false);
        setActiveSearchId(null);
        setActiveSearchStage(null);
        clearActiveSearch();
        setRequestError(err instanceof Error ? err.message : "Search failed");
      } finally {
        inFlight = false;
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
  }, [activeSearchId]);

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
    terminalHandled.current = true;
    terminalEvent.current = { outcome: job.status === "completed" ? (job.result?.found ? "match" : "no_match") : "error", error_code: job.error?.code ?? "unknown" };
    setActiveSearchId(null);
    setActiveSearchStage(null);
    clearActiveSearch();

    if (job.status === "completed") {
      resultJourney.current = journey.current;
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
    if (submitting) return;
    journey.current = newJourney("new");
    terminalHandled.current = false;
    terminalEvent.current = null;
    lastObservedStage.current = null;
    trackEvent("search_attempted", { ...journeyAttributes(journey.current), has_date_filter: Boolean(streamedFrom || streamedTo) });
    if (!hasUrl) {
      trackEvent("search_validation_blocked", { ...journeyAttributes(journey.current), reason: "empty_input" });
      return;
    }

    const submittedUrl = tiktokUrl.trim();
    if (!isSupportedTikTokUrl(submittedUrl)) {
      trackEvent("search_validation_blocked", { ...journeyAttributes(journey.current), reason: "unsupported_url" });
      setRequestError("Paste a TikTok video link, not a profile or other TikTok page.");
      return;
    }

    if (!streamer.trim()) {
      trackEvent("search_validation_blocked", { ...journeyAttributes(journey.current), reason: "missing_streamer" });
      setStreamerError("Select a streamer to run the search.");
      streamerTriggerRef.current?.focus();
      return;
    }

    const submittedStreamedFrom = streamedFrom.trim();
    const submittedStreamedTo = streamedTo.trim();

    try {
      setSubmitting(true);
      setActiveSearchId(null);
      setActiveSearchStage("validating");
      setStreamerError(null);
      setRequestError(null);
      setResult(null);
      resultJourney.current = null;
      setLastSubmittedUrl(submittedUrl);
      const created = await createSearchJob({
        tiktokUrl: submittedUrl,
        streamer,
        streamedFrom: submittedStreamedFrom || undefined,
        streamedTo: submittedStreamedTo || undefined,
      });
      journey.current.search_id = created.search_id;
      trackEvent("search_job_accepted", journeyAttributes(journey.current));
      persistActiveSearch(created.search_id, submittedUrl, submittedStreamedFrom, submittedStreamedTo);
      window.history.replaceState(null, "", sharedSearchPath(created.search_id));
      setActiveSearchId(created.search_id);
      setActiveSearchStage(created.stage);
    } catch (err) {
      terminalHandled.current = true;
      terminalEvent.current = { outcome: "error", ...requestFailureDetails(err) };
      trackEvent("search_submit_failed", { ...journeyAttributes(journey.current), ...requestFailureDetails(err) });
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
    trackEvent("date_filter_used", { has_date_filter: Boolean(nextFrom || nextTo) });
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
      trackEvent("clipboard_failed", { reason: "clipboard_denied" });
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
    onResultClick: (link_kind) => {
      if (resultJourney.current) trackEvent("result_vod_clicked", { ...journeyAttributes(resultJourney.current), link_kind });
    },
  };
}
