import { FormEvent, RefObject, useEffect, useRef, useState } from "react";

import { SearchClipInput, createSearchJob, getSearchJob, listSearchableStreamers } from "../../api/client";
import { CompletedSearchFeedback, SearchJobResponse, SearchResponse, StreamerListItem } from "../../api/types";
import { createSearchHistoryEntry, SearchHistoryEntry } from "./searchHistory";
import { isSupportedClipUrl } from "./searchUtils";
import { isSearchToken, parseSharedSearchLocation, sharedSearchPath } from "./sharedSearch";
import { useSearchHistory } from "./useSearchHistory";
import { ClipDraft, ClipSourceMetadata, parseClipSource, restoredDraft, sourceFromJob, validateClipFile } from "./clipSource";

const ACTIVE_SEARCH_STORAGE_KEY = "vodhunter-public-active-search";

interface ActiveSearchState {
  searchToken: string;
  source: ClipSourceMetadata;
  streamedFrom?: string;
  streamedTo?: string;
}

function persistActiveSearch(searchToken: string, source: ClipSourceMetadata, streamedFrom: string, streamedTo: string): void {
  window.localStorage.setItem(
    ACTIVE_SEARCH_STORAGE_KEY,
    JSON.stringify({ searchToken, source, streamedFrom, streamedTo }),
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
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || !("searchToken" in parsed) || !isSearchToken(parsed.searchToken)) {
      return null;
    }
    const source = "source" in parsed ? parseClipSource(parsed.source)
      : "tiktokUrl" in parsed && typeof parsed.tiktokUrl === "string" && parsed.tiktokUrl.trim()
        ? { kind: "link", url: parsed.tiktokUrl } satisfies ClipSourceMetadata : null;
    if (!source) {
      return null;
    }
    return {
      searchToken: parsed.searchToken,
      source,
      streamedFrom: "streamedFrom" in parsed && typeof parsed.streamedFrom === "string" ? parsed.streamedFrom : "",
      streamedTo: "streamedTo" in parsed && typeof parsed.streamedTo === "string" ? parsed.streamedTo : "",
    };
  } catch {
    return null;
  }
}

export interface SearchPageState {
  source: ClipDraft;
  streamedFrom: string;
  streamedTo: string;
  streamer: string;
  streamers: StreamerListItem[];
  loadingStreamers: boolean;
  streamerLoadError: string | null;
  result: SearchResponse | null;
  completedSearchFeedback: CompletedSearchFeedback | null;
  requestError: string | null;
  streamerError: string | null;
  submitting: boolean;
  activeSearchStage: string | null;
  submittedSource: ClipSourceMetadata | null;
  canSubmit: boolean;
  searchButtonLabel: string;
  streamerTriggerRef: RefObject<HTMLButtonElement>;
  historyEntries: SearchHistoryEntry[];
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onSourceModeChange: (mode: "link" | "upload") => void;
  onFileSelect: (file: File | null) => void;
  onUrlChange: (value: string) => void;
  onPaste: () => Promise<void>;
  onSelectStreamer: (value: string) => void;
  onDateRangeChange: (streamedFrom: string, streamedTo: string) => void;
  onClearHistory: () => void;
  onGoHome: () => void;
}

export function useSearchPage(): SearchPageState {
  const [source, setSource] = useState<ClipDraft>({ kind: "link", url: "" });
  const [streamedFrom, setStreamedFrom] = useState("");
  const [streamedTo, setStreamedTo] = useState("");
  const [streamer, setStreamer] = useState("");
  const [streamers, setStreamers] = useState<StreamerListItem[]>([]);
  const [loadingStreamers, setLoadingStreamers] = useState(true);
  const [streamerLoadError, setStreamerLoadError] = useState<string | null>(null);
  const [result, setResult] = useState<SearchResponse | null>(null);
  const [completedSearchFeedback, setCompletedSearchFeedback] = useState<CompletedSearchFeedback | null>(null);
  const [requestError, setRequestError] = useState<string | null>(null);
  const [streamerError, setStreamerError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [activeSearchToken, setActiveSearchToken] = useState<string | null>(null);
  const [activeSearchStage, setActiveSearchStage] = useState<string | null>(null);
  const [submittedSource, setSubmittedSource] = useState<ClipSourceMetadata | null>(null);
  const streamerTriggerRef = useRef<HTMLButtonElement>(null);
  const sourceRevisionRef = useRef(0);
  const submittedUploadRef = useRef<{ searchToken: string; file: File } | null>(null);
  const { entries: historyEntries, addEntry, clearEntries: onClearHistory } = useSearchHistory();

  const selectedFile = source.kind === "upload-selected" ? source.file : null;
  const canSubmit = source.kind === "link" ? source.url.trim().length > 0
    : source.kind === "upload-selected" && !validateClipFile(source.file, source.durationSeconds);
  const searchButtonLabel = submitting ? activeSearchStage === "uploading" ? "Uploading..." : "Searching..." : "Search";

  useEffect(() => {
    if (!selectedFile || typeof URL.createObjectURL !== "function") {
      return;
    }
    const video = document.createElement("video");
    const objectUrl = URL.createObjectURL(selectedFile);
    let cancelled = false;
    video.preload = "metadata";
    video.onloadedmetadata = () => {
      if (cancelled || !Number.isFinite(video.duration) || video.duration <= 0) {
        return;
      }
      const durationSeconds = video.duration;
      setSource((current) => current.kind === "upload-selected" && current.file === selectedFile
        ? { ...current, durationSeconds } : current);
      const fileError = validateClipFile(selectedFile, durationSeconds);
      if (fileError) {
        setRequestError(fileError);
      }
    };
    video.src = objectUrl;
    return () => {
      cancelled = true;
      video.onloadedmetadata = null;
      video.removeAttribute("src");
      URL.revokeObjectURL(objectUrl);
    };
  }, [selectedFile]);

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
    setSubmittedSource(activeSearch.source);
    setSource(restoredDraft(activeSearch.source));
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
    const jobSource = sourceFromJob(job);
    setSubmittedSource(jobSource);
    setSource((current) => {
      if (!jobSource) {
        return job.input_type === "upload" ? { kind: "upload-empty" } : { kind: "link", url: "" };
      }
      const submittedUpload = submittedUploadRef.current;
      if (jobSource.kind === "upload" && current.kind === "upload-selected"
        && submittedUpload?.searchToken === job.search_token && submittedUpload.file === current.file) {
        return current;
      }
      return restoredDraft(jobSource);
    });
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
      setCompletedSearchFeedback(job.result?.found ? { searchToken: job.search_token, feedback: job.feedback } : null);
      setRequestError(null);
      if (job.result && jobSource) {
        const historyEntry = createSearchHistoryEntry(
          job.result,
          jobSource,
          job.created_at,
          job.search_token,
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
    if (submitting) {
      return;
    }
    let input: SearchClipInput;
    let nextSubmittedSource: ClipSourceMetadata;
    const options = {
      streamer,
      streamedFrom: streamedFrom.trim() || undefined,
      streamedTo: streamedTo.trim() || undefined,
    };
    switch (source.kind) {
      case "link": {
        const url = source.url.trim();
        if (!url) {
          return;
        }
        if (!isSupportedClipUrl(url)) {
          setRequestError("Paste a TikTok video link, not a profile or other TikTok page.");
          return;
        }
        input = { ...options, kind: "link", url };
        nextSubmittedSource = { kind: "link", url };
        break;
      }
      case "upload-selected": {
        const fileError = validateClipFile(source.file, source.durationSeconds);
        if (fileError) {
          setRequestError(fileError);
          return;
        }
        input = { ...options, kind: "upload", file: source.file };
        nextSubmittedSource = { kind: "upload", filename: source.file.name };
        break;
      }
      case "upload-empty":
      case "upload-restored":
        setRequestError("Choose a video file to search again.");
        return;
      default: {
        const exhaustive: never = source;
        return exhaustive;
      }
    }

    if (!streamer.trim()) {
      setStreamerError("Select a streamer to run the search.");
      streamerTriggerRef.current?.focus();
      return;
    }

    const submittedStreamedFrom = streamedFrom.trim();
    const submittedStreamedTo = streamedTo.trim();

    try {
      sourceRevisionRef.current += 1;
      setSubmitting(true);
      setActiveSearchToken(null);
      setActiveSearchStage(input.kind === "upload" ? "uploading" : "validating");
      setStreamerError(null);
      setRequestError(null);
      setResult(null);
      setCompletedSearchFeedback(null);
      setSubmittedSource(nextSubmittedSource);
      const created = await createSearchJob(input);
      if (!isSearchToken(created.search_token)) {
        throw new Error("Could not open the search. Please refresh and try again.");
      }
      submittedUploadRef.current = input.kind === "upload" ? { searchToken: created.search_token, file: input.file } : null;
      persistActiveSearch(created.search_token, nextSubmittedSource, submittedStreamedFrom, submittedStreamedTo);
      window.history.replaceState(null, "", sharedSearchPath(created.search_token));
      setActiveSearchToken(created.search_token);
      setActiveSearchStage(created.stage);
    } catch (err) {
      setActiveSearchStage(null);
      setRequestError(err instanceof Error ? err.message : "Search failed");
      setSubmitting(false);
    }
  };

  const onSourceModeChange = (mode: "link" | "upload") => {
    if ((source.kind === "link") === (mode === "link")) {
      return;
    }
    submittedUploadRef.current = null;
    sourceRevisionRef.current += 1;
    setSource(mode === "link" ? { kind: "link", url: "" } : { kind: "upload-empty" });
    setRequestError(null);
    setStreamerError(null);
  };

  const onFileSelect = (file: File | null) => {
    sourceRevisionRef.current += 1;
    const fileError = file ? validateClipFile(file) : null;
    if (submittedUploadRef.current?.file !== file) {
      submittedUploadRef.current = null;
    }
    setSource(file && !fileError ? { kind: "upload-selected", file } : { kind: "upload-empty" });
    setRequestError(fileError);
    setStreamerError(null);
  };

  const onUrlChange = (value: string) => {
    submittedUploadRef.current = null;
    sourceRevisionRef.current += 1;
    setSource({ kind: "link", url: value });
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
    setRequestError(source.kind === "upload-selected" ? validateClipFile(source.file, source.durationSeconds) : null);
  };

  const onPaste = async () => {
    const revision = sourceRevisionRef.current;
    try {
      const text = await navigator.clipboard.readText();
      if (revision === sourceRevisionRef.current) {
        onUrlChange(text);
      }
    } catch {
      if (revision === sourceRevisionRef.current) {
        setRequestError("Clipboard access was blocked. Paste the clip link manually.");
      }
    }
  };

  return {
    source,
    streamedFrom,
    streamedTo,
    streamer,
    streamers,
    loadingStreamers,
    streamerLoadError,
    result,
    completedSearchFeedback,
    requestError,
    streamerError,
    submitting,
    activeSearchStage,
    submittedSource,
    canSubmit,
    searchButtonLabel,
    streamerTriggerRef,
    historyEntries,
    onSubmit,
    onSourceModeChange,
    onFileSelect,
    onUrlChange,
    onPaste,
    onSelectStreamer,
    onDateRangeChange,
    onClearHistory,
    onGoHome: clearActiveSearch,
  };
}
