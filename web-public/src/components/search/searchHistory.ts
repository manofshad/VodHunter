import { SearchResponse } from "../../api/types";
import { ClipSourceMetadata, parseClipSource } from "./clipSource";
import { isSearchToken } from "./sharedSearch";

export const SEARCH_HISTORY_STORAGE_KEY = "vodhunter-public-search-history-v1";
export const MAX_SEARCH_HISTORY_ENTRIES = 25;

export interface SearchHistoryEntry {
  id: string;
  searchToken: string | null;
  searchedAt: string;
  source: ClipSourceMetadata;
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

function parseSearchHistoryEntry(value: unknown): SearchHistoryEntry | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const source = "source" in value ? parseClipSource(value.source)
    : "tiktokUrl" in value && typeof value.tiktokUrl === "string" && value.tiktokUrl.trim()
      ? { kind: "link", url: value.tiktokUrl } satisfies ClipSourceMetadata : null;
  if (
    !source ||
    !("id" in value) || typeof value.id !== "string" || !value.id ||
    !("searchedAt" in value) || typeof value.searchedAt !== "string" || Number.isNaN(Date.parse(value.searchedAt)) ||
    !("streamer" in value) || typeof value.streamer !== "string" || !value.streamer ||
    !("profileImageUrl" in value) || !(typeof value.profileImageUrl === "string" || value.profileImageUrl === null) ||
    !("streamTitle" in value) || typeof value.streamTitle !== "string" || !value.streamTitle ||
    !("twitchUrlAtTimestamp" in value) || typeof value.twitchUrlAtTimestamp !== "string" || !value.twitchUrlAtTimestamp ||
    !("timestampSeconds" in value) || typeof value.timestampSeconds !== "number" || !Number.isFinite(value.timestampSeconds) || value.timestampSeconds < 0 ||
    !("additionalMatchCount" in value) || typeof value.additionalMatchCount !== "number" || !Number.isInteger(value.additionalMatchCount) || value.additionalMatchCount < 0
  ) {
    return null;
  }
  return {
    id: value.id,
    searchToken: "searchToken" in value
      ? isSearchToken(value.searchToken) ? value.searchToken : null
      : source.kind === "upload" && value.id.startsWith("upload::") && isSearchToken(value.id.slice("upload::".length))
        ? value.id.slice("upload::".length) : null,
    searchedAt: value.searchedAt,
    source,
    streamer: value.streamer,
    profileImageUrl: value.profileImageUrl,
    streamTitle: value.streamTitle,
    twitchUrlAtTimestamp: value.twitchUrlAtTimestamp,
    timestampSeconds: value.timestampSeconds,
    additionalMatchCount: value.additionalMatchCount,
  };
}

/**
 * Normalize the parts of a clip URL that identify a clip. Tracking query
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

function historyEntryId(clipUrl: string, streamer: string): string {
  return `${streamer.trim().toLowerCase()}::${normalizeHistoryUrl(clipUrl)}`;
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

    return parsed.flatMap((value: unknown) => {
      const entry = parseSearchHistoryEntry(value);
      return entry ? [entry] : [];
    }).slice(0, MAX_SEARCH_HISTORY_ENTRIES);
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
  source: ClipSourceMetadata,
  searchedAt: string,
  searchToken: string,
): SearchHistoryEntry | null {
  const twitchUrlAtTimestamp = result.video_url_at_timestamp?.trim();
  const timestampSeconds = result.timestamp_seconds;

  if (
    !isSearchToken(searchToken) ||
    !result.found ||
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
    id: source.kind === "link" ? historyEntryId(source.url, streamer) : `upload::${searchToken}`,
    searchToken,
    searchedAt: safeSearchedAt,
    source,
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

export function displayClipUrl(rawUrl: string): string {
  try {
    const parsed = new URL(rawUrl);
    const host = parsed.hostname.toLowerCase().replace(/^www\./, "");
    return `${host}${parsed.pathname}`;
  } catch {
    return rawUrl;
  }
}
