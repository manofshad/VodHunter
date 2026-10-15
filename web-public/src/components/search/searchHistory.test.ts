import { afterEach, describe, expect, it } from "vitest";

import { SearchResponse } from "../../api/types";
import {
  addSearchHistoryEntry,
  createSearchHistoryEntry,
  displayClipUrl,
  groupSearchHistory,
  MAX_SEARCH_HISTORY_ENTRIES,
  readSearchHistory,
  SEARCH_HISTORY_STORAGE_KEY,
} from "./searchHistory";

function matchedResult(overrides: Partial<SearchResponse> = {}): SearchResponse {
  return {
    found: true,
    streamer: "jasontheween",
    profile_image_url: "https://example.test/avatar.png",
    video_id: 7,
    video_url: "https://www.twitch.tv/videos/7",
    video_url_at_timestamp: "https://www.twitch.tv/videos/7?t=1h2m3s",
    thumbnail_url: null,
    title: "Late Night Stream",
    timestamp_seconds: 3723,
    score: 0.95,
    reason: "Accepted match",
    sources: [],
    segments: [
      {
        query_start: 0,
        query_end: 4,
        video_id: 7,
        vod_start: 3723,
        vod_end: 3727,
        video_url_at_timestamp: "https://www.twitch.tv/videos/7?t=1h2m3s",
        score: 0.95,
      },
      {
        query_start: 8,
        query_end: 12,
        video_id: 8,
        vod_start: 99,
        vod_end: 103,
        video_url_at_timestamp: "https://www.twitch.tv/videos/8?t=1m39s",
        score: 0.9,
      },
    ],
    unmatched_ranges: [],
    query_duration_seconds: 12,
    ...overrides,
  };
}

afterEach(() => {
  window.localStorage.clear();
});

describe("createSearchHistoryEntry", () => {
  it("keeps the primary result and counts additional distinct timestamps", () => {
    const entry = createSearchHistoryEntry(
      matchedResult(),
      { kind: "link", url: "https://www.tiktok.com/t/ZP87/?utm_source=share" },
      "2026-09-11T22:42:00.000Z",
      "A".repeat(43),
    );

    expect(entry).toMatchObject({
      id: "jasontheween::tiktok.com/t/ZP87",
      searchToken: "A".repeat(43),
      source: { kind: "link", url: "https://www.tiktok.com/t/ZP87/?utm_source=share" },
      streamTitle: "Late Night Stream",
      twitchUrlAtTimestamp: "https://www.twitch.tv/videos/7?t=1h2m3s",
      timestampSeconds: 3723,
      additionalMatchCount: 1,
    });
  });

  it("does not create entries for unsuccessful or unusable results", () => {
    expect(createSearchHistoryEntry(matchedResult({ found: false }), { kind: "link", url: "https://tiktok.com/t/abc" }, "now", "A".repeat(43))).toBeNull();
    expect(
      createSearchHistoryEntry(
        matchedResult({ video_url_at_timestamp: null }),
        { kind: "link", url: "https://tiktok.com/t/abc" },
        "2026-09-11T22:42:00.000Z",
        "A".repeat(43),
      ),
    ).toBeNull();
  });
});

describe("search history storage", () => {
  it("deduplicates an existing search and keeps the newest entry first", () => {
    const first = createSearchHistoryEntry(
      matchedResult(),
      { kind: "link", url: "https://tiktok.com/t/abc/" },
      "2026-09-10T22:42:00.000Z",
      "A".repeat(43),
    )!;
    const replacement = createSearchHistoryEntry(
      matchedResult({ timestamp_seconds: 4000, video_url_at_timestamp: "https://www.twitch.tv/videos/7?t=1h6m40s" }),
      { kind: "link", url: "https://www.tiktok.com/t/abc?tracking=1" },
      "2026-09-11T22:42:00.000Z",
      "A".repeat(43),
    )!;

    addSearchHistoryEntry(first);
    const entries = addSearchHistoryEntry(replacement);

    expect(entries).toHaveLength(1);
    expect(entries[0].timestampSeconds).toBe(4000);
    expect(readSearchHistory()).toEqual(entries);
  });

  it("limits persisted entries", () => {
    for (let index = 0; index < MAX_SEARCH_HISTORY_ENTRIES + 3; index += 1) {
      addSearchHistoryEntry({
        id: `entry-${index}`,
        searchToken: "A".repeat(43),
        searchedAt: `2026-09-${String((index % 9) + 1).padStart(2, "0")}T12:00:00.000Z`,
        source: { kind: "link", url: `https://tiktok.com/t/${index}` },
        streamer: "streamer",
        profileImageUrl: null,
        streamTitle: `Stream ${index}`,
        twitchUrlAtTimestamp: `https://twitch.tv/videos/${index}?t=1s`,
        timestampSeconds: 1,
        additionalMatchCount: 0,
      });
    }

    expect(readSearchHistory()).toHaveLength(MAX_SEARCH_HISTORY_ENTRIES);
    expect(readSearchHistory()[0].id).toBe(`entry-${MAX_SEARCH_HISTORY_ENTRIES + 2}`);
  });

  it("ignores malformed persisted data", () => {
    window.localStorage.setItem(SEARCH_HISTORY_STORAGE_KEY, JSON.stringify([{ id: "not enough fields" }]));
    expect(readSearchHistory()).toEqual([]);
  });
});

describe("history display helpers", () => {
  it("groups entries by local calendar day", () => {
    const now = new Date(2026, 8, 11, 12, 0, 0);
    const entries = [
      {
        id: "today",
        searchedAt: new Date(2026, 8, 11, 10, 0, 0).toISOString(),
      },
      {
        id: "yesterday",
        searchedAt: new Date(2026, 8, 10, 10, 0, 0).toISOString(),
      },
    ].map((entry) => ({
      ...entry,
      searchToken: null,
      source: { kind: "link" as const, url: "https://tiktok.com/t/abc" },
      streamer: "streamer",
      profileImageUrl: null,
      streamTitle: "Stream",
      twitchUrlAtTimestamp: "https://twitch.tv/videos/1?t=1s",
      timestampSeconds: 1,
      additionalMatchCount: 0,
    }));

    expect(groupSearchHistory(entries, now).map((group) => group.label)).toEqual(["Today", "Yesterday"]);
  });

  it("shows a readable TikTok host and path without protocol", () => {
    expect(displayClipUrl("https://www.tiktok.com/t/ZP87/?utm_source=share")).toBe("tiktok.com/t/ZP87/");
  });
});


it("migrates old URL history under the existing key and keeps upload identities distinct by token", () => {
  const legacy = { id: "legacy", searchedAt: "2026-10-01T12:00:00Z", tiktokUrl: "https://tiktok.com/t/abc", streamer: "jason", profileImageUrl: null,
    streamTitle: "Stream", twitchUrlAtTimestamp: "https://twitch.tv/videos/1?t=1s", timestampSeconds: 1, additionalMatchCount: 0 };
  window.localStorage.setItem(SEARCH_HISTORY_STORAGE_KEY, JSON.stringify([legacy]));
  expect(readSearchHistory()[0]).toEqual({ id: "legacy", searchToken: null, searchedAt: legacy.searchedAt, source: { kind: "link", url: legacy.tiktokUrl }, streamer: "jason", profileImageUrl: null,
    streamTitle: "Stream", twitchUrlAtTimestamp: legacy.twitchUrlAtTimestamp, timestampSeconds: 1, additionalMatchCount: 0 });
  for (const searchToken of ["A".repeat(43), "B".repeat(43)]) {
    const entry = createSearchHistoryEntry(matchedResult(), { kind: "upload", filename: "same-name.mp4" }, "2026-10-12T12:00:00Z", searchToken);
    if (!entry) { throw new Error("Expected a usable uploaded match"); }
    addSearchHistoryEntry(entry);
  }
  const entries = readSearchHistory();
  expect(entries.map((entry) => entry.id)).toEqual([`upload::${"B".repeat(43)}`, `upload::${"A".repeat(43)}`, "legacy"]);
  expect(JSON.parse(window.localStorage.getItem(SEARCH_HISTORY_STORAGE_KEY) ?? "[]")).toEqual(entries);
});

it("persists the job token for reopening the complete result", () => {
  const entry = createSearchHistoryEntry(matchedResult(), { kind: "link", url: "https://tiktok.com/t/abc" }, "2026-10-12T12:00:00Z", "B".repeat(43));
  if (!entry) { throw new Error("Expected a usable match"); }
  addSearchHistoryEntry(entry);
  expect(readSearchHistory()[0].searchToken).toBe("B".repeat(43));
  expect(createSearchHistoryEntry(matchedResult(), entry.source, entry.searchedAt, "invalid")).toBeNull();
});

it("validates persisted tokens and recovers only absent tokens from valid legacy upload identities", () => {
  const entry = createSearchHistoryEntry(matchedResult(), { kind: "upload", filename: "moment.mp4" }, "2026-10-12T12:00:00Z", "A".repeat(43));
  if (!entry) { throw new Error("Expected a usable match"); }
  const { searchToken, ...legacy } = entry;
  window.localStorage.setItem(SEARCH_HISTORY_STORAGE_KEY, JSON.stringify([
    entry,
    legacy,
    { ...legacy, id: "upload::invalid" },
    { ...legacy, source: { kind: "link", url: "https://tiktok.com/t/abc" } },
    { ...entry, searchToken: "invalid" },
    { ...entry, searchToken: null },
    { ...entry, searchToken: 42 },
  ]));
  expect(readSearchHistory().map((saved) => saved.searchToken)).toEqual([searchToken, searchToken, null, null, null, null, null]);
});
