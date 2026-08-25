import { FormEvent, useEffect, useId, useMemo, useRef, useState } from "react";
import { AlertCircle, Check, ChevronDown, Clipboard, ExternalLink, LoaderCircle, Search, TriangleAlert } from "lucide-react";

import { createSearchJob, getSearchJob, listSearchableStreamers } from "../api/client";
import { SearchJobResponse, SearchResponse, StreamerListItem } from "../api/types";
import defaultAvatar from "../assets/default-avatar.svg";

interface AvatarImageProps {
  src: string | null | undefined;
  alt: string;
  className?: string;
  decorative?: boolean;
}

function AvatarImage({ src, alt, className, decorative = false }: AvatarImageProps) {
  const [failed, setFailed] = useState(false);
  const resolvedSrc = !failed && src ? src : defaultAvatar;

  useEffect(() => {
    setFailed(false);
  }, [src]);

  return (
    <img
      src={resolvedSrc}
      alt={decorative ? "" : alt}
      className={className}
      aria-hidden={decorative ? "true" : undefined}
      onError={() => setFailed(true)}
    />
  );
}

function formatDuration(value: number | null): string | null {
  if (value === null || !Number.isFinite(value)) {
    return null;
  }

  const totalSeconds = Math.max(0, Math.floor(value));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  return [hours, minutes, seconds].map((part) => String(part).padStart(2, "0")).join(":");
}

export function formatTimelineTime(value: number): string {
  if (!Number.isFinite(value)) {
    return "Unknown time";
  }

  const totalTenths = Math.max(0, Math.round(value * 10));
  const hours = Math.floor(totalTenths / 36_000);
  const minutes = Math.floor((totalTenths % 36_000) / 600);
  const secondsWithTenths = (totalTenths % 600) / 10;
  const seconds = Number.isInteger(secondsWithTenths)
    ? String(secondsWithTenths).padStart(2, "0")
    : secondsWithTenths.toFixed(1).padStart(4, "0");

  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}:${seconds}`;
}

function getResultHref(result: SearchResponse | null): string | null {
  if (!result?.found) {
    return null;
  }

  return result.video_url_at_timestamp ?? null;
}

const ACTIVE_SEARCH_STORAGE_KEY = "vodhunter-public-active-search";

function getStageMessage(stage: string | null): string {
  switch (stage) {
    case "validating":
      return "Validating your TikTok URL.";
    case "downloading":
      return "Downloading the TikTok clip.";
    case "probing":
      return "Checking clip duration.";
    case "preprocessing":
      return "Preparing the clip audio for search.";
    case "fingerprinting":
      return "Generating neural audio fingerprints.";
    case "retrieving":
      return "Retrieving candidate VOD fingerprints.";
    case "aligning":
      return "Building supported clip-to-VOD timeline segments.";
    case "finalizing":
      return "Finalizing the search result.";
    default:
      return "We are matching your TikTok clip against indexed streamer audio.";
  }
}

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

function Header() {
  return (
    <header className="border-b border-gray-700 bg-gray-900 px-6 py-4">
      <div className="mx-auto flex max-w-7xl items-center justify-between">
        <div className="flex items-center gap-2" aria-label="VodHunter">
          <span className="text-xl font-bold text-white">
            <span className="font-bold">Vod</span>
            <span className="font-bold text-[#fb2844]">Hunter</span>
          </span>
        </div>
        <nav className="hidden items-center gap-8 md:flex" />
      </div>
    </header>
  );
}

function FeatureGrid() {
  const features = [
    {
      title: "Find The Exact VOD Moment",
      description: "Upload a short clip and match it to the exact timestamp inside a Twitch VOD.",
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

interface SearchResultCardProps {
  result: SearchResponse;
  lastSubmittedUrl: string;
}

export function SearchResultCard({ result, lastSubmittedUrl }: SearchResultCardProps) {
  const resultHref = getResultHref(result);
  const formattedTimestamp = formatDuration(result.timestamp_seconds);
  const [thumbnailLoadFailed, setThumbnailLoadFailed] = useState(false);
  const segments = result.segments ?? [];
  const unmatchedRanges = result.unmatched_ranges ?? [];
  const detailRows = [
    formattedTimestamp ? { label: "Timestamp", value: formattedTimestamp, emphasize: true } : null,
    result.reason ? { label: "Match reason", value: result.reason } : null,
    lastSubmittedUrl ? { label: "TikTok URL", value: lastSubmittedUrl, wrap: true } : null,
  ].filter(Boolean) as Array<{ label: string; value: string; emphasize?: boolean; wrap?: boolean }>;

  return (
    <section className="mx-auto max-w-4xl rounded-xl border border-gray-700 bg-gray-900 p-5 text-left shadow-lg">
      <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
        <span
          className={`inline-flex items-center rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-[0.14em] ${
            result.found ? "bg-emerald-500/15 text-emerald-200" : "bg-gray-700 text-gray-200"
          }`}
        >
          {result.found ? "Match found" : "No match found"}
        </span>
        {result.found && result.score !== null ? (
          <span className="text-xs font-medium uppercase tracking-[0.12em] text-gray-400">Score {result.score}</span>
        ) : null}
      </div>

      <div className="grid items-start gap-5 md:grid-cols-[minmax(0,240px)_minmax(0,1fr)]">
        <div className="self-start overflow-hidden rounded-xl border border-gray-700 bg-gray-800">
          {resultHref && result.thumbnail_url && !thumbnailLoadFailed ? (
            <a
              href={resultHref}
              target="_blank"
              rel="noreferrer"
              className="block aspect-video"
              aria-label={`Open ${result.title ?? "matched VOD"} at timestamp`}
            >
              <img
                src={result.thumbnail_url}
                alt={result.title ?? "Matched Twitch VOD thumbnail"}
                loading="lazy"
                className="h-full w-full object-cover"
                onError={() => setThumbnailLoadFailed(true)}
              />
            </a>
          ) : (
            <div className="flex aspect-video items-center justify-center bg-gray-800">
              <span className="rounded-full border border-gray-700 bg-gray-900 px-4 py-1.5 text-xs font-semibold uppercase tracking-[0.16em] text-gray-300">
                Twitch VOD
              </span>
            </div>
          )}
        </div>

        <div className="min-w-0 flex flex-col justify-between gap-4">
          <div>
            <div className="mb-3 flex items-center gap-3">
              <AvatarImage
                src={result.profile_image_url}
                alt={result.streamer ?? "Streamer"}
                className="size-11 rounded-full border border-gray-700 object-cover"
              />
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-[#fb2844]">
                {result.streamer ?? "Streamer unavailable"}
              </p>
            </div>
            {resultHref ? (
              <a href={resultHref} target="_blank" rel="noreferrer" className="group flex w-full items-start gap-3">
                <h3 className="min-w-0 flex-1 break-words [overflow-wrap:anywhere] text-lg font-bold leading-tight text-white transition group-hover:text-gray-100 md:text-[1.5rem]">
                  {result.title ?? "Matched Twitch VOD"}
                </h3>
                <ExternalLink className="mt-1 size-4 shrink-0 text-gray-400 transition group-hover:text-[#fb2844]" />
              </a>
            ) : (
              <h3 className="text-lg font-bold leading-tight text-white md:text-[1.5rem]">No matching Twitch VOD found</h3>
            )}
          </div>

        </div>

        <dl className="grid gap-0 text-left text-sm text-gray-300 md:col-span-2">
          {detailRows.map((item, index) => (
            <div
              key={item.label}
              className={`grid gap-1 py-3 ${index > 0 ? "border-t border-gray-700" : ""}`}
            >
              <dt className="text-xs font-semibold uppercase tracking-[0.16em] text-gray-500">{item.label}</dt>
              <dd
                className={[
                  item.emphasize ? "text-base font-semibold text-white" : "text-gray-300",
                  item.wrap ? "break-all" : "",
                ]
                  .filter(Boolean)
                  .join(" ")}
              >
                {item.value}
              </dd>
            </div>
          ))}
        </dl>

        {segments.length > 0 ? (
          <div className="border-t border-gray-700 pt-5 md:col-span-2">
            <h4 className="text-sm font-semibold uppercase tracking-[0.16em] text-gray-300">
              Matched clip segments
            </h4>
            <ol className="mt-3 grid gap-3">
              {segments.map((segment, index) => {
                const clipRange = `${formatTimelineTime(segment.query_start)}–${formatTimelineTime(segment.query_end)}`;
                const vodRange = `${formatTimelineTime(segment.vod_start)}–${formatTimelineTime(segment.vod_end)}`;
                const label = `VOD ${segment.video_id} · ${vodRange}`;
                return (
                  <li
                    key={`${segment.video_id}-${segment.query_start}-${segment.vod_start}`}
                    className="grid gap-2 rounded-lg border border-gray-700 bg-gray-800/70 p-3 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-center"
                  >
                    <div className="min-w-0">
                      <p className="text-xs font-semibold uppercase tracking-[0.14em] text-gray-500">
                        Clip {clipRange}
                      </p>
                      {segment.video_url_at_timestamp ? (
                        <a
                          href={segment.video_url_at_timestamp}
                          target="_blank"
                          rel="noreferrer"
                          className="mt-1 inline-flex items-center gap-2 break-all font-semibold text-white transition hover:text-[#fb2844]"
                          aria-label={`Open matched segment ${index + 1} in VOD ${segment.video_id} at ${formatTimelineTime(segment.vod_start)}`}
                        >
                          {label}
                          <ExternalLink className="size-4 shrink-0" />
                        </a>
                      ) : (
                        <p className="mt-1 font-semibold text-white">
                          {label} <span className="font-normal text-gray-400">· Link unavailable</span>
                        </p>
                      )}
                    </div>
                    <span className="text-xs font-medium text-gray-400">Score {segment.score.toFixed(3)}</span>
                  </li>
                );
              })}
            </ol>
          </div>
        ) : null}

        {unmatchedRanges.length > 0 ? (
          <div className="border-t border-gray-700 pt-5 md:col-span-2">
            <h4 className="text-sm font-semibold uppercase tracking-[0.16em] text-gray-300">
              Unmatched clip ranges
            </h4>
            <p className="mt-2 text-sm leading-6 text-gray-400">
              These portions did not contain enough continuous fingerprint evidence to identify a VOD moment.
            </p>
            <ul className="mt-3 flex flex-wrap gap-2">
              {unmatchedRanges.map((range) => (
                <li
                  key={`${range.query_start}-${range.query_end}`}
                  className="rounded-full border border-gray-700 bg-gray-800 px-3 py-1.5 text-sm font-medium text-gray-200"
                >
                  {formatTimelineTime(range.query_start)}–{formatTimelineTime(range.query_end)}
                </li>
              ))}
            </ul>
          </div>
        ) : null}
      </div>
    </section>
  );
}

export default function SearchPage() {
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
  const [isStreamerMenuOpen, setIsStreamerMenuOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [activeSearchId, setActiveSearchId] = useState<number | null>(null);
  const [activeSearchStage, setActiveSearchStage] = useState<string | null>(null);
  const [lastSubmittedUrl, setLastSubmittedUrl] = useState("");
  const streamerTriggerRef = useRef<HTMLButtonElement | null>(null);
  const streamerMenuRef = useRef<HTMLDivElement | null>(null);
  const streamerMenuId = useId();

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
    const activeSearch = readActiveSearch();
    if (activeSearch === null) {
      return;
    }

    setActiveSearchId(activeSearch.searchId);
    setActiveSearchStage("validating");
    setLastSubmittedUrl(activeSearch.tiktokUrl);
    setTiktokUrl(activeSearch.tiktokUrl);
    setStreamedFrom(activeSearch.streamedFrom ?? "");
    setStreamedTo(activeSearch.streamedTo ?? "");
    setSubmitting(true);
  }, []);

  useEffect(() => {
    if (!isStreamerMenuOpen) {
      return;
    }

    const onPointerDown = (event: MouseEvent) => {
      const target = event.target as Node;
      if (streamerTriggerRef.current?.contains(target) || streamerMenuRef.current?.contains(target)) {
        return;
      }
      setIsStreamerMenuOpen(false);
    };

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setIsStreamerMenuOpen(false);
        streamerTriggerRef.current?.focus();
      }
    };

    window.addEventListener("mousedown", onPointerDown);
    window.addEventListener("keydown", onKeyDown);

    return () => {
      window.removeEventListener("mousedown", onPointerDown);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [isStreamerMenuOpen]);

  useEffect(() => {
    if (activeSearchId === null) {
      return;
    }

    let cancelled = false;

    const poll = async () => {
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
        setSubmitting(false);
        setActiveSearchId(null);
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
  }, [activeSearchId]);

  const handlePolledJob = (job: SearchJobResponse) => {
    setActiveSearchStage(job.stage);

    if (job.status === "queued" || job.status === "running") {
      setSubmitting(true);
      return;
    }

    setSubmitting(false);
    setActiveSearchId(null);
    setActiveSearchStage(null);
    clearActiveSearch();

    if (job.status === "completed") {
      setResult(job.result);
      setRequestError(null);
      return;
    }

    setResult(null);
    setRequestError(job.error?.message ?? "Search failed");
  };

  const onSubmit = async (event: FormEvent) => {
    event.preventDefault();
    if (!hasUrl) {
      return;
    }

    if (!streamer.trim()) {
      setStreamerError("Select a streamer to run the search.");
      streamerTriggerRef.current?.focus();
      return;
    }

    const submittedUrl = tiktokUrl.trim();
    const submittedStreamedFrom = streamedFrom.trim();
    const submittedStreamedTo = streamedTo.trim();

    try {
      setSubmitting(true);
      setActiveSearchId(null);
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
      persistActiveSearch(created.search_id, submittedUrl, submittedStreamedFrom, submittedStreamedTo);
      setActiveSearchId(created.search_id);
      setActiveSearchStage(created.stage);
    } catch (err) {
      setActiveSearchStage(null);
      setRequestError(err instanceof Error ? err.message : "Search failed");
      setSubmitting(false);
    }
  };

  const onSelectStreamer = (value: string) => {
    setStreamer(value);
    setStreamerError(null);
    setIsStreamerMenuOpen(false);
    streamerTriggerRef.current?.focus();
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

  return (
    <div className="min-h-screen bg-gray-900">
      <Header />

      <main>
        <div
          className="relative px-6 py-24 md:py-28"
          style={{ background: "linear-gradient(160deg, #fb2844 0%, #f55b70 100%)" }}
        >
          <div className="mx-auto max-w-6xl text-center">
            <div className="mx-auto max-w-4xl">
              <h1 className="mb-12 text-3xl font-bold text-white">Find That Exact Moment</h1>

              <form onSubmit={onSubmit} noValidate className="flex flex-col items-stretch gap-3">
                <div className="flex flex-col items-stretch gap-3 md:flex-row">
                  <div className="flex-1 rounded-xl bg-gray-800 p-1">
                    <div className="flex flex-col items-stretch gap-2 md:flex-row md:items-center">
                      <div className="relative md:w-[180px] md:shrink-0">
                        <button
                          ref={streamerTriggerRef}
                          type="button"
                          disabled={submitting || loadingStreamers || streamers.length === 0}
                          aria-invalid={streamerError ? "true" : "false"}
                          aria-describedby={streamerError ? "streamer-error" : undefined}
                          aria-expanded={isStreamerMenuOpen ? "true" : "false"}
                          aria-controls={streamerMenuId}
                          onClick={() => setIsStreamerMenuOpen((open) => !open)}
                          className="flex h-10 w-full items-center gap-2 border-0 bg-gray-800 px-4 text-sm font-medium text-gray-100 outline-none disabled:cursor-not-allowed disabled:text-gray-500"
                        >
                          {streamer ? (
                            <AvatarImage
                              src={streamers.find((item) => item.name === streamer)?.profile_image_url}
                              alt=""
                              className="size-6 rounded-full object-cover"
                              decorative
                            />
                          ) : null}
                          <span className={streamer ? "text-gray-100" : "text-gray-400"}>
                            {loadingStreamers
                              ? "Loading streamers..."
                              : streamers.length === 0
                                ? "No searchable streamers"
                                : streamer || "Streamer"}
                          </span>
                          <ChevronDown className="ml-auto size-4 text-gray-500" />
                        </button>

                        {isStreamerMenuOpen && !loadingStreamers && streamers.length > 0 ? (
                          <div
                            ref={streamerMenuRef}
                            id={streamerMenuId}
                            role="listbox"
                            className="absolute top-[calc(100%+8px)] left-0 z-20 w-full overflow-hidden rounded-xl border border-gray-700 bg-gray-800 shadow-xl"
                          >
                            {streamers.map((item) => {
                              const selected = item.name === streamer;
                              return (
                                <button
                                  key={item.name}
                                  type="button"
                                  role="option"
                                  aria-selected={selected}
                                  onClick={() => onSelectStreamer(item.name)}
                                  className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-gray-100 transition hover:bg-gray-700"
                                >
                                  <AvatarImage
                                    src={item.profile_image_url}
                                    alt=""
                                    className="size-6 rounded-full object-cover"
                                    decorative
                                  />
                                  <span className="flex-1">{item.name}</span>
                                  {selected ? <Check className="size-4 text-[#fb2844]" /> : null}
                                </button>
                              );
                            })}
                          </div>
                        ) : null}
                      </div>

                      <div className="hidden h-10 w-px bg-gray-700 md:block" aria-hidden="true" />

                      <div className="relative min-w-0 flex-1">
                        <input
                          type="url"
                          placeholder="Paste TikTok URL here"
                          value={tiktokUrl}
                          disabled={submitting}
                          onChange={(event) => {
                            setTiktokUrl(event.target.value);
                            setStreamerError(null);
                            setRequestError(null);
                          }}
                          className="h-10 w-full border-0 bg-gray-800 px-4 pr-12 text-sm text-gray-100 outline-none placeholder:text-gray-400 disabled:cursor-not-allowed disabled:text-gray-500"
                        />
                        <button
                          type="button"
                          onClick={onPaste}
                          disabled={submitting}
                          className="absolute top-1/2 right-1 flex size-8 -translate-y-1/2 items-center justify-center rounded-md text-pink-400 transition hover:bg-gray-700 hover:text-pink-300 disabled:cursor-not-allowed disabled:text-gray-500"
                          aria-label="Paste TikTok URL from clipboard"
                        >
                          <Clipboard className="size-4" style={{ color: "#fb2844" }} />
                        </button>
                      </div>

                      <button
                        type="submit"
                        disabled={submitting || !hasUrl}
                        className="inline-flex h-10 items-center justify-center rounded-xl border-2 border-[#fb2844] bg-[#fb2844] px-8 text-base font-semibold text-white transition hover:border-[#f55b70] hover:bg-[#f55b70] disabled:border-gray-600 disabled:bg-gray-700 disabled:text-gray-400"
                      >
                        {searchButtonLabel}
                      </button>
                    </div>
                  </div>
                </div>

                <div className="grid gap-3 rounded-xl bg-gray-800/90 p-3 text-left md:grid-cols-[auto_minmax(0,1fr)_minmax(0,1fr)] md:items-end">
                  <div className="text-xs font-semibold uppercase tracking-[0.16em] text-gray-300 md:pb-3">
                    Stream date range
                  </div>
                  <label className="grid gap-1 text-xs font-medium text-gray-300">
                    From
                    <input
                      type="date"
                      value={streamedFrom}
                      max={streamedTo || undefined}
                      disabled={submitting}
                      onChange={(event) => {
                        setStreamedFrom(event.target.value);
                        setRequestError(null);
                      }}
                      className="h-10 rounded-lg border border-gray-700 bg-gray-900 px-3 text-sm text-gray-100 outline-none disabled:cursor-not-allowed disabled:text-gray-500"
                    />
                  </label>
                  <label className="grid gap-1 text-xs font-medium text-gray-300">
                    To
                    <input
                      type="date"
                      value={streamedTo}
                      min={streamedFrom || undefined}
                      disabled={submitting}
                      onChange={(event) => {
                        setStreamedTo(event.target.value);
                        setRequestError(null);
                      }}
                      className="h-10 rounded-lg border border-gray-700 bg-gray-900 px-3 text-sm text-gray-100 outline-none disabled:cursor-not-allowed disabled:text-gray-500"
                    />
                  </label>
                </div>

                <div className="min-h-6 text-left">
                  {streamerError ? (
                    <p id="streamer-error" className="flex items-center gap-2 text-sm font-medium text-white">
                      <TriangleAlert className="size-4" />
                      {streamerError}
                    </p>
                  ) : null}
                  {streamerLoadError ? (
                    <p className="flex items-center gap-2 text-sm font-medium text-white">
                      <AlertCircle className="size-4" />
                      {streamerLoadError}
                    </p>
                  ) : null}
                </div>

                {requestError || submitting || result ? (
                  <div className="mt-8 space-y-6">
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
                            <p className="mt-2 text-sm text-gray-400">
                          {getStageMessage(activeSearchStage)}
                            </p>
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
                ) : null}
              </form>
            </div>
          </div>
        </div>

        <FeatureGrid />
      </main>
    </div>
  );
}
