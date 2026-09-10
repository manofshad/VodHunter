import { FormEvent, useEffect, useId, useMemo, useRef, useState } from "react";
import {
  AlertCircle,
  Check,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  Clipboard,
  ExternalLink,
  LoaderCircle,
  Search,
  TriangleAlert,
} from "lucide-react";

import { createSearchJob, getSearchJob, listSearchableStreamers } from "../api/client";
import { SearchJobResponse, SearchResponse, SearchSegment, SearchSource, StreamerListItem } from "../api/types";
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

const DIRECT_TIKTOK_HOSTS = new Set(["tiktok.com", "www.tiktok.com", "www.tiktokv.com"]);
const SHORT_TIKTOK_HOSTS = new Set(["tiktok.com", "www.tiktok.com", "vm.tiktok.com", "vt.tiktok.com"]);
const DIRECT_VIDEO_PATHS = [
  /^\/@(?:[A-Za-z0-9_.-]+)?\/video\/[0-9]+\/?$/,
  /^\/share\/video\/[0-9]+\/?$/,
  /^\/embed\/[0-9]+\/?$/,
];

export function isSupportedTikTokUrl(rawUrl: string): boolean {
  const url = rawUrl.trim();
  if (!url) {
    return false;
  }

  try {
    const parsed = new URL(url);
    const host = parsed.hostname.toLowerCase();
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return false;
    }
    if (parsed.username || parsed.password || parsed.port) {
      return false;
    }

    const isDirectVideo = DIRECT_TIKTOK_HOSTS.has(host) && DIRECT_VIDEO_PATHS.some((pattern) => pattern.test(parsed.pathname));
    const isShortShare =
      (host === "tiktok.com" || host === "www.tiktok.com") && /^\/t\/[A-Za-z0-9_]+\/?$/.test(parsed.pathname);
    const isVmShare =
      (host === "vm.tiktok.com" || host === "vt.tiktok.com") && /^\/[A-Za-z0-9_]+\/?$/.test(parsed.pathname);

    return isDirectVideo || (SHORT_TIKTOK_HOSTS.has(host) && (isShortShare || isVmShare));
  } catch {
    return false;
  }
}

const DATE_RANGE_PLACEHOLDER = "mm/dd/yyyy – mm/dd/yyyy";
const WEEKDAYS = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"];

function parseDateInput(value: string): Date | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return null;
  }

  const [year, month, day] = value.split("-").map(Number);
  const date = new Date(year, month - 1, day);
  if (date.getFullYear() !== year || date.getMonth() !== month - 1 || date.getDate() !== day) {
    return null;
  }

  return date;
}

function formatDateInput(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function formatDateDisplay(value: string): string {
  const date = parseDateInput(value);
  if (date === null) {
    return "";
  }

  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${month}/${day}/${date.getFullYear()}`;
}

function formatDateRangeDisplay(streamedFrom: string, streamedTo: string): string {
  const from = formatDateDisplay(streamedFrom);
  const to = formatDateDisplay(streamedTo);

  if (!from && !to) {
    return DATE_RANGE_PLACEHOLDER;
  }
  if (!to) {
    return `${from} –`;
  }
  if (!from) {
    return `– ${to}`;
  }

  return `${from} – ${to}`;
}

function formatDateRangePill(streamedFrom: string, streamedTo: string): string {
  const from = parseDateInput(streamedFrom);
  const to = parseDateInput(streamedTo);
  if (from === null || to === null) {
    return "Custom";
  }

  const fromMonth = from.toLocaleDateString("en-US", { month: "short" });
  const toMonth = to.toLocaleDateString("en-US", { month: "short" });
  if (from.getFullYear() === to.getFullYear() && from.getMonth() === to.getMonth()) {
    return `${fromMonth} ${from.getDate()}–${to.getDate()}`;
  }
  if (from.getFullYear() === to.getFullYear()) {
    return `${fromMonth} ${from.getDate()}–${toMonth} ${to.getDate()}`;
  }

  return `${fromMonth} ${from.getDate()}, ${from.getFullYear()}–${toMonth} ${to.getDate()}, ${to.getFullYear()}`;
}

function recentDateRange(dayCount: number): [string, string] {
  const to = new Date();
  const from = new Date(to);
  from.setDate(from.getDate() - (dayCount - 1));
  return [formatDateInput(from), formatDateInput(to)];
}

function startOfMonth(date: Date): Date {
  return new Date(date.getFullYear(), date.getMonth(), 1);
}

function addMonths(date: Date, amount: number): Date {
  return new Date(date.getFullYear(), date.getMonth() + amount, 1);
}

function getCalendarDays(month: Date): Date[] {
  const firstDay = startOfMonth(month);
  const daysInMonth = new Date(month.getFullYear(), month.getMonth() + 1, 0).getDate();
  const leadingDays = firstDay.getDay();
  const totalCells = Math.ceil((leadingDays + daysInMonth) / 7) * 7;

  return Array.from({ length: totalCells }, (_, index) => {
    return new Date(month.getFullYear(), month.getMonth(), index - leadingDays + 1);
  });
}

function isDateInRange(value: string, streamedFrom: string, streamedTo: string): boolean {
  return Boolean(streamedFrom && streamedTo && value > streamedFrom && value < streamedTo);
}

interface CalendarMonthProps {
  month: Date;
  streamedFrom: string;
  streamedTo: string;
  onSelect: (value: string) => void;
  onPrevious: () => void;
  onNext: () => void;
}

function CalendarMonth({ month, streamedFrom, streamedTo, onSelect, onPrevious, onNext }: CalendarMonthProps) {
  const monthLabel = month.toLocaleDateString("en-US", { month: "short", year: "numeric" });

  return (
    <section aria-label={monthLabel}>
      <div className="mb-2 grid grid-cols-[2rem_1fr_2rem] items-center">
        <button
          type="button"
          aria-label="Previous month"
          onClick={onPrevious}
          className="flex size-8 items-center justify-center rounded-md text-gray-300 transition hover:bg-gray-800 hover:text-white"
        >
          <ChevronLeft className="size-4" />
        </button>
        <h3 className="text-center text-sm font-semibold text-gray-100">{monthLabel}</h3>
        <button
          type="button"
          aria-label="Next month"
          onClick={onNext}
          className="flex size-8 items-center justify-center rounded-md text-gray-300 transition hover:bg-gray-800 hover:text-white"
        >
          <ChevronRight className="size-4" />
        </button>
      </div>
      <div className="grid grid-cols-7 gap-0.5 text-center text-[0.64rem] font-semibold uppercase tracking-wide text-gray-500">
        {WEEKDAYS.map((weekday) => (
          <span key={weekday} aria-hidden="true" className="py-0.5">
            {weekday}
          </span>
        ))}
      </div>
      <div className="mt-0.5 grid grid-cols-7 gap-0.5 text-sm">
        {getCalendarDays(month).map((date) => {
          const value = formatDateInput(date);
          const isCurrentMonth = date.getMonth() === month.getMonth();
          const isStart = value === streamedFrom;
          const isEnd = value === streamedTo;
          const isSelected = isStart || isEnd;
          const isInRange = isDateInRange(value, streamedFrom, streamedTo);

          return (
            <button
              key={value}
              type="button"
              aria-label={date.toLocaleDateString("en-US", {
                month: "long",
                day: "numeric",
                year: "numeric",
              })}
              aria-pressed={isSelected}
              onClick={() => onSelect(value)}
              className={`flex h-8 items-center justify-center rounded-md transition ${
                isSelected
                  ? "bg-[#fb2844] font-semibold text-white"
                  : isInRange
                    ? "bg-[#fb2844]/20 text-gray-100"
                    : isCurrentMonth
                      ? "text-gray-200 hover:bg-gray-800"
                      : "text-gray-600 hover:bg-gray-800/60"
              }`}
            >
              {date.getDate()}
            </button>
          );
        })}
      </div>
    </section>
  );
}

interface DateRangePickerProps {
  streamedFrom: string;
  streamedTo: string;
  disabled: boolean;
  onChange: (streamedFrom: string, streamedTo: string) => void;
}

export function DateRangePicker({ streamedFrom, streamedTo, disabled, onChange }: DateRangePickerProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [draftFrom, setDraftFrom] = useState(streamedFrom);
  const [draftTo, setDraftTo] = useState(streamedTo);
  const [visibleMonth, setVisibleMonth] = useState(() =>
    startOfMonth(parseDateInput(streamedFrom) ?? parseDateInput(streamedTo) ?? new Date()),
  );
  const customTriggerRef = useRef<HTMLButtonElement | null>(null);
  const customPanelId = useId();

  useEffect(() => {
    if (!isOpen) {
      setDraftFrom(streamedFrom);
      setDraftTo(streamedTo);
    }
  }, [isOpen, streamedFrom, streamedTo]);

  useEffect(() => {
    if (!isOpen) {
      return;
    }
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setDraftFrom(streamedFrom);
        setDraftTo(streamedTo);
        setIsOpen(false);
        customTriggerRef.current?.focus();
      }
    };

    window.addEventListener("keydown", onKeyDown);

    return () => {
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [isOpen, streamedFrom, streamedTo]);

  const openCustomPicker = () => {
    setDraftFrom(streamedFrom);
    setDraftTo(streamedTo);
    setVisibleMonth(startOfMonth(parseDateInput(streamedFrom) ?? parseDateInput(streamedTo) ?? new Date()));
    setIsOpen(true);
  };

  const selectDate = (value: string) => {
    if (!draftFrom || draftTo) {
      setDraftFrom(value);
      setDraftTo("");
      return;
    }

    if (value < draftFrom) {
      setDraftTo(draftFrom);
      setDraftFrom(value);
      return;
    }

    setDraftTo(value);
  };

  const cancelPicker = () => {
    setDraftFrom(streamedFrom);
    setDraftTo(streamedTo);
    setIsOpen(false);
    customTriggerRef.current?.focus();
  };

  const savePicker = () => {
    if (!draftFrom || !draftTo) {
      return;
    }
    onChange(draftFrom, draftTo);
    setIsOpen(false);
    customTriggerRef.current?.focus();
  };

  const resetPicker = () => {
    setDraftFrom("");
    setDraftTo("");
  };

  const selectPreset = (dayCount: number | null) => {
    const range: [string, string] = dayCount === null ? ["", ""] : recentDateRange(dayCount);
    onChange(range[0], range[1]);
    setDraftFrom(range[0]);
    setDraftTo(range[1]);
    setIsOpen(false);
  };

  const [weekFrom, weekTo] = recentDateRange(7);
  const [monthFrom, monthTo] = recentDateRange(30);
  const appliedScope =
    !streamedFrom && !streamedTo
      ? "any"
      : streamedFrom === weekFrom && streamedTo === weekTo
        ? "week"
        : streamedFrom === monthFrom && streamedTo === monthTo
          ? "month"
          : "custom";
  const selectedScope = isOpen ? "custom" : appliedScope;
  const pillClass = (selected: boolean) =>
    `inline-flex h-5 items-center rounded-full border px-2 text-[0.65rem] font-semibold leading-none transition disabled:cursor-not-allowed disabled:opacity-50 ${
      selected
        ? "border-[#fb2844] bg-[#fb2844]/15 text-white"
        : "border-transparent bg-gray-700/75 text-gray-300 hover:border-gray-500 hover:text-white"
    }`;

  return (
    <div className="border-t border-gray-700/80 text-left">
      <div className="flex min-h-8 flex-wrap items-center gap-1.5 px-3 py-2">
        <span className="mr-1 text-[0.61rem] font-semibold uppercase tracking-[0.14em] text-gray-400">Streamed</span>
        <button
          type="button"
          disabled={disabled}
          aria-pressed={selectedScope === "any"}
          onClick={() => selectPreset(null)}
          className={pillClass(selectedScope === "any")}
        >
          Any time
        </button>
        <button
          type="button"
          disabled={disabled}
          aria-pressed={selectedScope === "week"}
          onClick={() => selectPreset(7)}
          className={pillClass(selectedScope === "week")}
        >
          Past 7 days
        </button>
        <button
          type="button"
          disabled={disabled}
          aria-pressed={selectedScope === "month"}
          onClick={() => selectPreset(30)}
          className={pillClass(selectedScope === "month")}
        >
          Past 30 days
        </button>
        <button
          ref={customTriggerRef}
          type="button"
          disabled={disabled}
          aria-pressed={selectedScope === "custom"}
          aria-expanded={isOpen}
          aria-controls={customPanelId}
          onClick={isOpen ? cancelPicker : openCustomPicker}
          className={pillClass(selectedScope === "custom")}
        >
          {appliedScope === "custom" ? formatDateRangePill(streamedFrom, streamedTo) : "Custom"}
        </button>
      </div>

      {isOpen ? (
        <section id={customPanelId} aria-label="Custom stream date range" className="border-t border-gray-700 bg-gray-900/35">
          <div className="flex flex-col gap-2 border-b border-gray-700 px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
            <p className="text-sm font-semibold text-white">Custom range</p>
            <p className="rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-xs font-medium text-gray-100">
              {formatDateRangeDisplay(draftFrom, draftTo)}
            </p>
          </div>

          <div className="mx-auto w-full max-w-[30rem] px-4 py-3">
            <CalendarMonth
              month={visibleMonth}
              streamedFrom={draftFrom}
              streamedTo={draftTo}
              onSelect={selectDate}
              onPrevious={() => setVisibleMonth((month) => addMonths(month, -1))}
              onNext={() => setVisibleMonth((month) => addMonths(month, 1))}
            />
          </div>

          <div className="flex items-center justify-end gap-2 border-t border-gray-700 px-3 py-2">
            <button
              type="button"
              onClick={resetPicker}
              className="mr-auto px-1 py-2 text-xs font-semibold text-gray-300 underline decoration-gray-500 underline-offset-4 transition hover:text-white"
            >
              Clear dates
            </button>
            <button
              type="button"
              onClick={cancelPicker}
              className="rounded-lg border border-gray-600 bg-gray-700 px-3 py-2 text-xs font-semibold text-gray-200 transition hover:bg-gray-600"
            >
              Cancel
            </button>
            <button
              type="button"
              disabled={!draftFrom || !draftTo}
              onClick={savePicker}
              className="rounded-lg bg-[#fb2844] px-3 py-2 text-xs font-semibold text-white transition hover:bg-[#f55b70] disabled:cursor-not-allowed disabled:bg-gray-700 disabled:text-gray-500"
            >
              Apply
            </button>
          </div>
        </section>
      ) : null}
    </div>
  );
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

interface SearchResultCardProps {
  result: SearchResponse;
  lastSubmittedUrl: string;
}

function getSearchSources(result: SearchResponse): SearchSource[] {
  if (result.sources?.length) {
    return result.sources;
  }

  if (!result.found) {
    return [];
  }

  const segmentsByVideoId = new Map<number, SearchSegment[]>();
  for (const segment of result.segments ?? []) {
    const sourceSegments = segmentsByVideoId.get(segment.video_id) ?? [];
    sourceSegments.push(segment);
    segmentsByVideoId.set(segment.video_id, sourceSegments);
  }

  if (result.video_id !== null && !segmentsByVideoId.has(result.video_id)) {
    segmentsByVideoId.set(result.video_id, []);
  }

  return Array.from(segmentsByVideoId.entries()).map(([videoId, sourceSegments]) => {
    const isPrimary = videoId === result.video_id;
    return {
      video_id: videoId,
      video_url: isPrimary ? result.video_url : null,
      video_url_at_timestamp: isPrimary ? result.video_url_at_timestamp : sourceSegments[0]?.video_url_at_timestamp ?? null,
      thumbnail_url: isPrimary ? result.thumbnail_url : null,
      title: isPrimary ? result.title : null,
      streamer: isPrimary ? result.streamer : null,
      profile_image_url: isPrimary ? result.profile_image_url : null,
      segments: sourceSegments,
    };
  });
}

interface SourceThumbnailProps {
  source: SearchSource;
  href: string | null;
}

function SourceThumbnail({ source, href }: SourceThumbnailProps) {
  const [thumbnailLoadFailed, setThumbnailLoadFailed] = useState(false);

  useEffect(() => {
    setThumbnailLoadFailed(false);
  }, [source.video_id, source.thumbnail_url]);

  const content = source.thumbnail_url && !thumbnailLoadFailed ? (
    <img
      src={source.thumbnail_url}
      alt={source.title ?? "Matched Twitch VOD thumbnail"}
      loading="lazy"
      className="h-full w-full object-cover"
      onError={() => setThumbnailLoadFailed(true)}
    />
  ) : (
    <div className="flex aspect-video items-center justify-center bg-gray-800">
      <span className="rounded-full border border-gray-700 bg-gray-900 px-4 py-1.5 text-xs font-semibold uppercase tracking-[0.16em] text-gray-300">
        Twitch VOD
      </span>
    </div>
  );

  return (
    <div className="self-start overflow-hidden rounded-xl border border-gray-700 bg-gray-800">
      {href ? (
        <a
          href={href}
          target="_blank"
          rel="noreferrer"
          className="block aspect-video"
          aria-label={`Open ${source.title ?? "matched VOD"} at its strongest match`}
        >
          {content}
        </a>
      ) : (
        content
      )}
    </div>
  );
}

interface SearchSourceBlockProps {
  source: SearchSource;
  fallbackStreamer: string | null;
  fallbackProfileImageUrl: string | null;
}

function SearchSourceBlock({ source, fallbackStreamer, fallbackProfileImageUrl }: SearchSourceBlockProps) {
  const sourceHref = source.video_url_at_timestamp ?? source.segments[0]?.video_url_at_timestamp ?? null;
  const sourceTitle = source.title ?? "Matched Twitch VOD";
  const streamer = source.streamer ?? fallbackStreamer ?? "Streamer unavailable";
  const profileImageUrl = source.profile_image_url ?? fallbackProfileImageUrl;

  return (
    <article className="rounded-xl border border-gray-700 bg-gray-800/30 p-4">
      <div className="grid items-start gap-5 md:grid-cols-[minmax(0,240px)_minmax(0,1fr)]">
        <SourceThumbnail source={source} href={sourceHref} />

        <div className="min-w-0">
          <div className="mb-3 flex items-center gap-3">
            <AvatarImage
              src={profileImageUrl}
              alt={streamer}
              className="size-11 rounded-full border border-gray-700 object-cover"
            />
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-[#fb2844]">{streamer}</p>
          </div>
          {sourceHref ? (
            <a href={sourceHref} target="_blank" rel="noreferrer" className="group flex w-full items-start gap-3">
              <h3 className="min-w-0 flex-1 break-words [overflow-wrap:anywhere] text-lg font-bold leading-tight text-white transition group-hover:text-gray-100 md:text-[1.5rem]">
                {sourceTitle}
              </h3>
              <ExternalLink className="mt-1 size-4 shrink-0 text-gray-400 transition group-hover:text-[#fb2844]" />
            </a>
          ) : (
            <h3 className="text-lg font-bold leading-tight text-white md:text-[1.5rem]">{sourceTitle}</h3>
          )}
        </div>
      </div>

      {source.segments.length > 0 ? (
        <div className="mt-5 border-t border-gray-700 pt-5">
          <h4 className="text-sm font-semibold uppercase tracking-[0.16em] text-gray-300">Matched clip segments</h4>
          <ol className="mt-3 grid gap-3">
            {source.segments.map((segment, index) => {
              const clipRange = `${formatTimelineTime(segment.query_start)}–${formatTimelineTime(segment.query_end)}`;
              const vodRange = `${formatTimelineTime(segment.vod_start)}–${formatTimelineTime(segment.vod_end)}`;
              return (
                <li
                  key={`${source.video_id}-${segment.query_start}-${segment.vod_start}`}
                  className="grid gap-2 rounded-lg border border-gray-700 bg-gray-800/70 p-3 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-center"
                >
                  <div className="min-w-0">
                    <p className="text-xs font-semibold uppercase tracking-[0.14em] text-gray-500">Clip {clipRange}</p>
                    {segment.video_url_at_timestamp ? (
                      <a
                        href={segment.video_url_at_timestamp}
                        target="_blank"
                        rel="noreferrer"
                        className="mt-1 inline-flex items-center gap-2 break-all font-semibold text-white transition hover:text-[#fb2844]"
                        aria-label={`Open matched segment ${index + 1} in ${sourceTitle} at ${formatTimelineTime(segment.vod_start)}`}
                      >
                        {vodRange}
                        <ExternalLink className="size-4 shrink-0" />
                      </a>
                    ) : (
                      <p className="mt-1 font-semibold text-white">
                        {vodRange} <span className="font-normal text-gray-400">· Link unavailable</span>
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
    </article>
  );
}

export function SearchResultCard({ result, lastSubmittedUrl }: SearchResultCardProps) {
  const sources = getSearchSources(result);
  const unmatchedRanges = result.unmatched_ranges ?? [];

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
      </div>

      {sources.length > 0 ? (
        <div className="grid gap-5">
          {sources.map((source) => (
            <SearchSourceBlock
              key={source.video_id}
              source={source}
              fallbackStreamer={result.streamer}
              fallbackProfileImageUrl={result.profile_image_url}
            />
          ))}
        </div>
      ) : (
        <h3 className="text-lg font-bold leading-tight text-white md:text-[1.5rem]">No matching Twitch VOD found</h3>
      )}

      {unmatchedRanges.length > 0 ? (
        <div className="mt-5 border-t border-gray-700 pt-5">
          <h4 className="text-sm font-semibold uppercase tracking-[0.16em] text-gray-300">Unmatched clip ranges</h4>
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

      {lastSubmittedUrl ? (
        <div className="mt-5 border-t border-gray-700 pt-5">
          <p className="text-xs font-semibold uppercase tracking-[0.16em] text-gray-500">TikTok URL</p>
          <a
            href={lastSubmittedUrl}
            target="_blank"
            rel="noreferrer"
            className="mt-1 inline-flex max-w-full items-start gap-2 break-all text-sm text-gray-300 transition hover:text-[#fb2844]"
            aria-label="Open original TikTok clip"
          >
            <span>{lastSubmittedUrl}</span>
            <ExternalLink className="mt-0.5 size-4 shrink-0" />
          </a>
        </div>
      ) : null}
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
                <div className="relative rounded-xl bg-gray-800">
                  <div className="p-1">
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

                  <DateRangePicker
                    streamedFrom={streamedFrom}
                    streamedTo={streamedTo}
                    disabled={submitting}
                    onChange={(nextFrom, nextTo) => {
                      setStreamedFrom(nextFrom);
                      setStreamedTo(nextTo);
                      setRequestError(null);
                    }}
                  />
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
