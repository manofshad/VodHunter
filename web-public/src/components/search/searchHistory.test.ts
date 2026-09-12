import { afterEach, describe, expect, it } from "vitest";

import { SearchResponse } from "../../api/types";
import {
  addSearchHistoryEntry,
  createSearchHistoryEntry,
  displayTikTokUrl,
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
      "https://www.tiktok.com/t/ZP87/?utm_source=share",
      "2026-09-11T22:42:00.000Z",
    );

    expect(entry).toMatchObject({
      id: "jasontheween::tiktok.com/t/ZP87",
      tiktokUrl: "https://www.tiktok.com/t/ZP87/?utm_source=share",
      streamTitle: "Late Night Stream",
      twitchUrlAtTimestamp: "https://www.twitch.tv/videos/7?t=1h2m3s",
      timestampSeconds: 3723,
      additionalMatchCount: 1,
    });
  });

  it("does not create entries for unsuccessful or unusable results", () => {
    expect(createSearchHistoryEntry(matchedResult({ found: false }), "https://tiktok.com/t/abc", "now")).toBeNull();
    expect(
      createSearchHistoryEntry(
        matchedResult({ video_url_at_timestamp: null }),
        "https://tiktok.com/t/abc",
        "2026-09-11T22:42:00.000Z",
      ),
    ).toBeNull();
  });
});

describe("search history storage", () => {
  it("deduplicates an existing search and keeps the newest entry first", () => {
    const first = createSearchHistoryEntry(
      matchedResult(),
      "https://tiktok.com/t/abc/",
      "2026-09-10T22:42:00.000Z",
    )!;
    const replacement = createSearchHistoryEntry(
      matchedResult({ timestamp_seconds: 4000, video_url_at_timestamp: "https://www.twitch.tv/videos/7?t=1h6m40s" }),
      "https://www.tiktok.com/t/abc?tracking=1",
      "2026-09-11T22:42:00.000Z",
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
        searchedAt: `2026-09-${String((index % 9) + 1).padStart(2, "0")}T12:00:00.000Z`,
        tiktokUrl: `https://tiktok.com/t/${index}`,
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
      tiktokUrl: "https://tiktok.com/t/abc",
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
    expect(displayTikTokUrl("https://www.tiktok.com/t/ZP87/?utm_source=share")).toBe("tiktok.com/t/ZP87/");
  });
});
