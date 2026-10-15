import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { createSearchJob, getSearchJob, listSearchableStreamers, saveSearchFeedback } from "../api/client";
import { SearchJobResponse, SearchResponse } from "../api/types";
import SearchPage from "./SearchPage";
import { createSearchHistoryEntry, SearchHistoryEntry, SEARCH_HISTORY_STORAGE_KEY } from "./search/searchHistory";

vi.mock("../api/client", () => ({
  createSearchJob: vi.fn(),
  getSearchJob: vi.fn(),
  listSearchableStreamers: vi.fn(),
  saveSearchFeedback: vi.fn(),
}));

const tokenA = "A".repeat(43);
const tokenB = "B".repeat(43);
const clipUrl = "https://www.tiktok.com/@demo/video/123456789";

function matchedResult(title = "Saved stream"): SearchResponse {
  const segments = [100, 200].map((start, index) => ({
    query_start: index * 5,
    query_end: index * 5 + 4,
    video_id: 7,
    vod_start: start,
    vod_end: start + 4,
    video_url_at_timestamp: `https://www.twitch.tv/videos/7?t=${start}s`,
    score: 0.9,
  }));
  return {
    found: true, streamer: "jason", profile_image_url: null, video_id: 7,
    video_url: "https://www.twitch.tv/videos/7", video_url_at_timestamp: segments[0].video_url_at_timestamp,
    thumbnail_url: null, title, timestamp_seconds: 100, score: 0.9, reason: "match",
    sources: [{ video_id: 7, video_url: "https://www.twitch.tv/videos/7", video_url_at_timestamp: segments[0].video_url_at_timestamp,
      thumbnail_url: null, title, streamer: "jason", profile_image_url: null, segments }],
    segments, unmatched_ranges: [], query_duration_seconds: 10,
  };
}

function completedJob(searchToken = tokenB, overrides: Partial<SearchJobResponse> = {}): SearchJobResponse {
  return {
    search_token: searchToken, status: "completed", stage: null, input_type: "tiktok_url", clip_filename: null,
    tiktok_url: clipUrl, streamer: "jason", created_at: "2026-10-12T12:00:00Z", started_at: null,
    finished_at: "2026-10-12T12:00:05Z", result: matchedResult(), error: null,
    feedback: { rating: "down", updated_at: "2026-10-12T12:05:00Z" }, ...overrides,
  };
}

function savedEntry(overrides: Partial<SearchHistoryEntry> = {}): SearchHistoryEntry {
  const entry = createSearchHistoryEntry(matchedResult(), { kind: "link", url: clipUrl }, "2026-10-12T12:00:00Z", tokenB);
  if (!entry) { throw new Error("Expected a usable history entry"); }
  return { ...entry, ...overrides };
}

function openSavedEntry(name = "Saved stream") {
  fireEvent.click(screen.getByRole("button", { name: "History" }));
  fireEvent.click(within(screen.getByRole("dialog", { name: "History" })).getByRole("button", { name: `Open search results for ${name}` }));
}

describe("opening saved search results", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    window.localStorage.clear();
    window.history.replaceState(null, "", "/");
    vi.mocked(listSearchableStreamers).mockResolvedValue([{ name: "jason", profile_image_url: null }]);
    window.localStorage.setItem(SEARCH_HISTORY_STORAGE_KEY, JSON.stringify([savedEntry()]));
    vi.mocked(getSearchJob).mockResolvedValue(completedJob());
  });

  it("switches a mounted share page to the full saved result, canonical URL, and saved feedback without creating a job", async () => {
    window.history.replaceState(null, "", `/share#${tokenA}`);
    vi.mocked(getSearchJob).mockResolvedValueOnce(completedJob(tokenA, { result: matchedResult("Current stream"), tiktok_url: `${clipUrl}0` }));
    render(<SearchPage />);
    await screen.findByRole("link", { name: "Current stream" });

    openSavedEntry();

    expect(screen.queryByRole("dialog", { name: "History" })).toBeNull();
    expect(window.location.pathname + window.location.hash).toBe(`/share#${tokenB}`);
    await screen.findByRole("link", { name: "Saved stream" });
    expect(screen.queryByRole("link", { name: "Current stream" })).toBeNull();
    expect(getSearchJob).toHaveBeenLastCalledWith(tokenB);
    expect(createSearchJob).not.toHaveBeenCalled();
    expect(screen.getByRole("link", { name: "Open original clip" }).getAttribute("href")).toBe(clipUrl);
    expect(screen.getByRole("button", { name: "Thumbs down, incorrect match" }).getAttribute("aria-pressed")).toBe("true");
    expect(screen.getByText("Match 1 of 2")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Next match" }));
    expect(screen.getByText("Match 2 of 2")).toBeTruthy();
    expect(screen.getByRole("link", { name: /Open matched segment 2/ }).getAttribute("href")).toContain("t=200s");

    vi.mocked(saveSearchFeedback).mockResolvedValue({ rating: "up", updated_at: "2026-10-12T12:06:00Z" });
    fireEvent.click(screen.getByRole("button", { name: "Thumbs up, correct match" }));
    await waitFor(() => expect(saveSearchFeedback).toHaveBeenCalledWith(tokenB, "up"));
  });

  it("clears an old result and feedback while an unavailable saved job loads and fails", async () => {
    window.history.replaceState(null, "", `/share#${tokenA}`);
    let rejectSaved: ((error: Error) => void) | undefined;
    vi.mocked(getSearchJob).mockResolvedValueOnce(completedJob(tokenA, { result: matchedResult("Current stream"), tiktok_url: `${clipUrl}0` }))
      .mockImplementationOnce(() => new Promise((_, reject) => { rejectSaved = reject; }));
    render(<SearchPage />);
    await screen.findByRole("link", { name: "Current stream" });
    openSavedEntry();
    expect(screen.queryByRole("link", { name: "Current stream" })).toBeNull();
    expect(screen.queryByRole("group", { name: "Rate this match" })).toBeNull();
    await act(async () => { rejectSaved?.(new Error("Search job was not found")); });
    expect(await screen.findByText("Search job was not found")).toBeTruthy();
    expect(screen.queryByRole("link", { name: "Current stream" })).toBeNull();
    expect(screen.queryByRole("group", { name: "Rate this match" })).toBeNull();
    expect(createSearchJob).not.toHaveBeenCalled();
  });

  it("prefills a legacy entry at the root and explicitly asks for another search", async () => {
    window.localStorage.setItem(SEARCH_HISTORY_STORAGE_KEY, JSON.stringify([savedEntry({ searchToken: null })]));
    render(<SearchPage />);
    await waitFor(() => expect(screen.getByRole("combobox", { name: "Streamer" }).hasAttribute("disabled")).toBe(false));
    fireEvent.click(screen.getByRole("button", { name: "History" }));
    expect(screen.getByText("Search again to view results")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Search again for Saved stream" }));
    expect(screen.getByPlaceholderText("Paste TikTok URL here").getAttribute("value")).toBe(clipUrl);
    expect(screen.getByRole("combobox", { name: "Streamer" }).textContent).toContain("jason");
    expect(screen.getByText("Search again to view results for this saved clip.")).toBeTruthy();
    expect(window.location.pathname + window.location.hash).toBe("/");
    expect(getSearchJob).not.toHaveBeenCalled();
    expect(createSearchJob).not.toHaveBeenCalled();
    expect(screen.queryByRole("dialog", { name: "History" })).toBeNull();
  });

  it("restores an uploaded saved result and requires a file only for a new search", async () => {
    window.localStorage.setItem(SEARCH_HISTORY_STORAGE_KEY, JSON.stringify([savedEntry({ id: `upload::${tokenB}`, source: { kind: "upload", filename: "moment.mp4" } })]));
    vi.mocked(getSearchJob).mockResolvedValue(completedJob(tokenB, { input_type: "upload", clip_filename: "moment.mp4", tiktok_url: null }));
    render(<SearchPage />);
    openSavedEntry();
    await screen.findByRole("link", { name: "Saved stream" });
    expect(screen.getByText("Uploaded file")).toBeTruthy();
    expect(screen.queryByRole("link", { name: "Open original clip" })).toBeNull();
    expect(screen.getByRole("button", { name: "Search" }).hasAttribute("disabled")).toBe(true);
    expect(screen.getByText("Match 1 of 2")).toBeTruthy();
    expect(createSearchJob).not.toHaveBeenCalled();
  });

  it.each(["resolve", "reject"])("ignores a pending create that later %ss after opening saved results", async (settlement) => {
    let finishCreate: ((value: Awaited<ReturnType<typeof createSearchJob>>) => void) | undefined;
    let rejectCreate: ((error: Error) => void) | undefined;
    vi.mocked(createSearchJob).mockImplementation(() => new Promise((resolve, reject) => { finishCreate = resolve; rejectCreate = reject; }));
    render(<SearchPage />);
    await waitFor(() => expect(screen.getByRole("combobox", { name: "Streamer" }).hasAttribute("disabled")).toBe(false));
    fireEvent.change(screen.getByPlaceholderText("Paste TikTok URL here"), { target: { value: `${clipUrl}0` } });
    fireEvent.click(screen.getByRole("combobox", { name: "Streamer" }));
    fireEvent.click(screen.getByRole("option", { name: "jason" }));
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(createSearchJob).toHaveBeenCalledOnce();
    openSavedEntry();
    await screen.findByRole("link", { name: "Saved stream" });

    await act(async () => {
      if (settlement === "resolve") { finishCreate?.({ search_token: tokenA, status: "queued", stage: "validating" }); }
      else { rejectCreate?.(new Error("Old create failed")); }
    });
    expect(window.location.pathname + window.location.hash).toBe(`/share#${tokenB}`);
    expect(screen.getByRole("link", { name: "Saved stream" })).toBeTruthy();
    expect(screen.queryByText("Old create failed")).toBeNull();
    expect(getSearchJob).not.toHaveBeenCalledWith(tokenA);
    expect(window.localStorage.getItem("vodhunter-public-active-search")).toBeNull();
    expect(screen.getByRole("button", { name: "Thumbs down, incorrect match" }).getAttribute("aria-pressed")).toBe("true");
  });
});
