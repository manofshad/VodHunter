import { SearchResponse } from "../../api/types";

export const SEARCH_HISTORY_STORAGE_KEY = "vodhunter-public-search-history-v1";
export const MAX_SEARCH_HISTORY_ENTRIES = 25;

export interface SearchHistoryEntry {
  id: string;
  searchedAt: string;
  tiktokUrl: string;
  streamer: string;
  profileImageUrl: string | null;
  streamTitle: string;
  twitchUrlAtTimestamp: string;
  timestampSeconds: number;
  additionalMatchCount: number;
}

export interface SearchHistoryGroup {
  key: string;
  label: string;
  entries: SearchHistoryEntry[];
}

function getStorage(): Storage | null {
  if (typeof window === "undefined") {
    return null;
  }

  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

function isSearchHistoryEntry(value: unknown): value is SearchHistoryEntry {
  if (!value || typeof value !== "object") {
    return false;
  }

  const entry = value as Partial<SearchHistoryEntry>;
  return (
    typeof entry.id === "string" &&
    entry.id.length > 0 &&
    typeof entry.searchedAt === "string" &&
    !Number.isNaN(Date.parse(entry.searchedAt)) &&
    typeof entry.tiktokUrl === "string" &&
    entry.tiktokUrl.length > 0 &&
    typeof entry.streamer === "string" &&
    entry.streamer.length > 0 &&
    (typeof entry.profileImageUrl === "string" || entry.profileImageUrl === null) &&
    typeof entry.streamTitle === "string" &&
    entry.streamTitle.length > 0 &&
    typeof entry.twitchUrlAtTimestamp === "string" &&
    entry.twitchUrlAtTimestamp.length > 0 &&
    typeof entry.timestampSeconds === "number" &&
    Number.isFinite(entry.timestampSeconds) &&
    entry.timestampSeconds >= 0 &&
    typeof entry.additionalMatchCount === "number" &&
    Number.isInteger(entry.additionalMatchCount) &&
    entry.additionalMatchCount >= 0
  );
}

/**
 * Normalize the parts of a TikTok URL that identify a clip. Tracking query
 * parameters and trailing slashes should not create duplicate history rows.
 */
export function normalizeHistoryUrl(rawUrl: string): string {
  const trimmed = rawUrl.trim();
  if (!trimmed) {
    return "";
  }

  try {
    const parsed = new URL(trimmed);
    const host = parsed.hostname.toLowerCase().replace(/^www\./, "");
    const path = parsed.pathname.replace(/\/+$/, "");
    return `${host}${path}`;
  } catch {
    return trimmed.toLowerCase().replace(/\/+$/, "");
  }
}

function historyEntryId(tiktokUrl: string, streamer: string): string {
  return `${streamer.trim().toLowerCase()}::${normalizeHistoryUrl(tiktokUrl)}`;
}

export function readSearchHistory(): SearchHistoryEntry[] {
  const storage = getStorage();
  if (!storage) {
    return [];
  }

  try {
    const raw = storage.getItem(SEARCH_HISTORY_STORAGE_KEY);
    if (!raw) {
      return [];
    }

    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }

    return parsed.filter(isSearchHistoryEntry).slice(0, MAX_SEARCH_HISTORY_ENTRIES);
  } catch {
    return [];
  }
}

function writeSearchHistory(entries: SearchHistoryEntry[]): void {
  const storage = getStorage();
  if (!storage) {
    return;
  }

  try {
    storage.setItem(SEARCH_HISTORY_STORAGE_KEY, JSON.stringify(entries.slice(0, MAX_SEARCH_HISTORY_ENTRIES)));
  } catch {
    // Private browsing and full storage can make localStorage unavailable.
    // History should remain a best-effort enhancement and never break search.
  }
}

function countAdditionalMatches(result: SearchResponse): number {
  const timestampKeys = new Set(
    result.segments
      .filter((segment) => Number.isFinite(segment.vod_start))
      .map((segment) => `${segment.video_id}:${Math.round(segment.vod_start * 10) / 10}`),
  );

  const matchCount = timestampKeys.size || result.sources.length || 1;
  return Math.max(0, matchCount - 1);
}

export function createSearchHistoryEntry(
  result: SearchResponse,
  tiktokUrl: string,
  searchedAt: string,
): SearchHistoryEntry | null {
  const twitchUrlAtTimestamp = result.video_url_at_timestamp?.trim();
  const timestampSeconds = result.timestamp_seconds;
  const submittedUrl = tiktokUrl.trim();

  if (
    !result.found ||
    !submittedUrl ||
    !twitchUrlAtTimestamp ||
    timestampSeconds === null ||
    !Number.isFinite(timestampSeconds) ||
    timestampSeconds < 0
  ) {
    return null;
  }

  const streamer = result.streamer?.trim() || "Unknown streamer";
  const safeSearchedAt = Number.isNaN(Date.parse(searchedAt)) ? new Date().toISOString() : searchedAt;

  return {
    id: historyEntryId(submittedUrl, streamer),
    searchedAt: safeSearchedAt,
    tiktokUrl: submittedUrl,
    streamer,
    profileImageUrl: result.profile_image_url,
    streamTitle: result.title?.trim() || "Matched Twitch VOD",
    twitchUrlAtTimestamp,
    timestampSeconds,
    additionalMatchCount: countAdditionalMatches(result),
  };
}

export function addSearchHistoryEntry(entry: SearchHistoryEntry): SearchHistoryEntry[] {
  const next = [entry, ...readSearchHistory().filter((existing) => existing.id !== entry.id)].slice(
    0,
    MAX_SEARCH_HISTORY_ENTRIES,
  );
  writeSearchHistory(next);
  return next;
}

export function clearSearchHistory(): void {
  const storage = getStorage();
  if (!storage) {
    return;
  }

  try {
    storage.removeItem(SEARCH_HISTORY_STORAGE_KEY);
  } catch {
    // Clearing is best-effort when storage is blocked by the browser.
  }
}

function localDateKey(date: Date): string {
  return `${date.getFullYear()}-${date.getMonth()}-${date.getDate()}`;
}

export function historyDateLabel(searchedAt: string, now = new Date()): string {
  const date = new Date(searchedAt);
  if (Number.isNaN(date.getTime())) {
    return "Earlier";
  }

  const todayKey = localDateKey(now);
  if (localDateKey(date) === todayKey) {
    return "Today";
  }

  const yesterday = new Date(now);
  yesterday.setDate(yesterday.getDate() - 1);
  if (localDateKey(date) === localDateKey(yesterday)) {
    return "Yesterday";
  }

  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    year: date.getFullYear() === now.getFullYear() ? undefined : "numeric",
  }).format(date);
}

export function groupSearchHistory(entries: SearchHistoryEntry[], now = new Date()): SearchHistoryGroup[] {
  const groups = new Map<string, SearchHistoryGroup>();

  for (const entry of entries) {
    const date = new Date(entry.searchedAt);
    const key = Number.isNaN(date.getTime()) ? "earlier" : localDateKey(date);
    const existing = groups.get(key);
    if (existing) {
      existing.entries.push(entry);
      continue;
    }

    groups.set(key, {
      key,
      label: historyDateLabel(entry.searchedAt, now),
      entries: [entry],
    });
  }

  return Array.from(groups.values());
}

export function formatHistoryTime(searchedAt: string): string {
  const date = new Date(searchedAt);
  if (Number.isNaN(date.getTime())) {
    return "Unknown time";
  }

  return new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

export function displayTikTokUrl(rawUrl: string): string {
  try {
    const parsed = new URL(rawUrl);
    const host = parsed.hostname.toLowerCase().replace(/^www\./, "");
    return `${host}${parsed.pathname}`;
  } catch {
    return rawUrl;
  }
}
