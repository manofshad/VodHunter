import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { createSearchJob, getSearchJob, listSearchableStreamers } from "../../api/client";
import { SearchJobResponse, SearchResponse } from "../../api/types";
import { useSearchPage } from "./useSearchPage";
import { trackEvent } from "../../telemetry";

vi.mock("../../telemetry", async (importOriginal) => ({
  ...await importOriginal<typeof import("../../telemetry")>(), trackEvent: vi.fn(),
}));

vi.mock("../../api/client", () => ({
  createSearchJob: vi.fn(),
  getSearchJob: vi.fn(),
  listSearchableStreamers: vi.fn(),
}));

const tiktokUrl = "https://www.tiktok.com/@demo/video/123456789";

function result(): SearchResponse {
  return {
    found: true,
    streamer: "jason",
    profile_image_url: null,
    video_id: 7,
    video_url: "https://www.twitch.tv/videos/7",
    video_url_at_timestamp: "https://www.twitch.tv/videos/7?t=1m40s",
    thumbnail_url: null,
    title: "Matched stream",
    timestamp_seconds: 100,
    score: 0.91,
    reason: "match",
    sources: [],
    segments: [],
    unmatched_ranges: [],
    query_duration_seconds: 10,
  };
}

function job(overrides: Partial<SearchJobResponse> = {}): SearchJobResponse {
  return {
    search_id: 42,
    status: "queued",
    stage: "validating",
    tiktok_url: tiktokUrl,
    streamer: "jason",
    created_at: "2026-09-14T12:00:00Z",
    started_at: null,
    finished_at: null,
    result: null,
    error: null,
    ...overrides,
  };
}

describe("useSearchPage shared jobs", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    Object.defineProperty(document, "hidden", { configurable: true, value: false });
    window.localStorage.clear();
    window.history.replaceState(null, "", "/");
    vi.mocked(listSearchableStreamers).mockResolvedValue([
      { name: "jason", profile_image_url: null },
    ]);
  });

  it("records one visible result for a shared job and excludes raw input from events", async () => {
    window.history.replaceState(null, "", "/share?search_id=42");
    vi.mocked(getSearchJob).mockResolvedValue(job({ status: "completed", result: result(), stage: null }));
    const { result: hook, rerender } = renderHook(() => useSearchPage());
    await waitFor(() => expect(hook.current.result?.found).toBe(true));
    rerender();
    const visible = vi.mocked(trackEvent).mock.calls.filter(([name]) => name === "search_result_visible");
    expect(visible).toHaveLength(1);
    expect(visible[0][1]).toMatchObject({ entry_kind: "shared", search_id: 42, outcome: "match" });
    expect(JSON.stringify(vi.mocked(trackEvent).mock.calls)).not.toContain(tiktokUrl);
    act(() => hook.current.onUrlChange("https://example.com/invalid"));
    await act(async () => hook.current.onSubmit({ preventDefault: vi.fn() } as never));
    // Validation leaves the previous result visible; clicks retain its journey.
    act(() => hook.current.onResultClick("segment"));
    expect(trackEvent).toHaveBeenCalledWith("result_vod_clicked", expect.objectContaining({ link_kind: "segment", search_id: 42 }));
  });

  it("waits for a foreground view before recording a visible result", async () => {
    Object.defineProperty(document, "hidden", { configurable: true, value: true });
    window.history.replaceState(null, "", "/share?search_id=42");
    vi.mocked(getSearchJob).mockResolvedValue(job({ status: "completed", result: result(), stage: null }));
    const { result: hook } = renderHook(() => useSearchPage());
    await waitFor(() => expect(hook.current.result?.found).toBe(true));
    expect(vi.mocked(trackEvent).mock.calls.filter(([name]) => name === "search_result_visible")).toHaveLength(0);
    act(() => {
      Object.defineProperty(document, "hidden", { configurable: true, value: false });
      document.dispatchEvent(new Event("visibilitychange"));
      document.dispatchEvent(new Event("visibilitychange"));
    });
    expect(vi.mocked(trackEvent).mock.calls.filter(([name]) => name === "search_result_visible")).toHaveLength(1);
  });

  it("observes client validation without submitting a backend job", async () => {
    const { result: hook } = renderHook(() => useSearchPage());
    await waitFor(() => expect(hook.current.loadingStreamers).toBe(false));
    act(() => hook.current.onUrlChange("https://example.com/private"));
    await act(async () => hook.current.onSubmit({ preventDefault: vi.fn() } as never));
    expect(createSearchJob).not.toHaveBeenCalled();
    expect(trackEvent).toHaveBeenCalledWith("search_validation_blocked", expect.objectContaining({ reason: "unsupported_url" }));
    expect(JSON.stringify(vi.mocked(trackEvent).mock.calls)).not.toContain("example.com/private");
  });

  it("distinguishes a failed status fetch from a failed backend search", async () => {
    window.history.replaceState(null, "", "/share?search_id=42");
    vi.mocked(getSearchJob).mockRejectedValue(new TypeError("Failed to fetch"));
    const { result: hook } = renderHook(() => useSearchPage());
    await waitFor(() => expect(hook.current.requestError).toBe("Failed to fetch"));
    expect(trackEvent).toHaveBeenCalledWith("search_poll_failed", expect.objectContaining({ reason: "network", search_id: 42 }));
    expect(trackEvent).toHaveBeenCalledWith("search_result_visible", expect.objectContaining({ reason: "poll_failed", outcome: "error" }));
  });

  it("gives a share URL priority over an older local search", async () => {
    window.localStorage.setItem(
      "vodhunter-public-active-search",
      JSON.stringify({ searchId: 99, tiktokUrl: "https://www.tiktok.com/@old/video/1" }),
    );
    window.history.replaceState(null, "", "/share?search_id=42");
    vi.mocked(getSearchJob).mockResolvedValue(job());

    const { result: hook } = renderHook(() => useSearchPage());

    await waitFor(() => expect(getSearchJob).toHaveBeenCalledWith(42));
    expect(getSearchJob).not.toHaveBeenCalledWith(99);
    expect(hook.current.submitting).toBe(true);
    expect(hook.current.activeSearchStage).toBe("validating");
    expect(hook.current.tiktokUrl).toBe(tiktokUrl);
    expect(hook.current.streamer).toBe("jason");
    expect(window.localStorage.getItem("vodhunter-public-active-search")).toBeNull();
  });

  it("restores a completed result and its original TikTok URL", async () => {
    window.history.replaceState(null, "", "/share?search_id=42");
    vi.mocked(getSearchJob).mockResolvedValue(
      job({
        status: "completed",
        stage: null,
        finished_at: "2026-09-14T12:00:10Z",
        result: result(),
      }),
    );

    const { result: hook } = renderHook(() => useSearchPage());

    await waitFor(() => expect(hook.current.result?.video_id).toBe(7));
    expect(hook.current.submitting).toBe(false);
    expect(hook.current.lastSubmittedUrl).toBe(tiktokUrl);
    expect(hook.current.historyEntries).toHaveLength(1);
  });

  it("shows a failed job error", async () => {
    window.history.replaceState(null, "", "/share?search_id=42");
    vi.mocked(getSearchJob).mockResolvedValue(
      job({
        status: "failed",
        stage: null,
        finished_at: "2026-09-14T12:00:10Z",
        error: { code: "DOWNLOAD_ERROR", message: "TikTok could not be downloaded" },
      }),
    );

    const { result: hook } = renderHook(() => useSearchPage());

    await waitFor(() => expect(hook.current.requestError).toBe("TikTok could not be downloaded"));
    expect(hook.current.submitting).toBe(false);
    expect(hook.current.result).toBeNull();
  });

  it("shows an unknown-job error from the API", async () => {
    window.history.replaceState(null, "", "/share?search_id=404");
    vi.mocked(getSearchJob).mockRejectedValue(new Error("Search job was not found"));

    const { result: hook } = renderHook(() => useSearchPage());

    await waitFor(() => expect(hook.current.requestError).toBe("Search job was not found"));
    expect(hook.current.submitting).toBe(false);
  });

  it("rejects a malformed link instead of restoring local state", async () => {
    window.localStorage.setItem(
      "vodhunter-public-active-search",
      JSON.stringify({ searchId: 99, tiktokUrl: "https://www.tiktok.com/@old/video/1" }),
    );
    window.history.replaceState(null, "", "/share?search_id=not-a-job");

    const { result: hook } = renderHook(() => useSearchPage());

    await waitFor(() => expect(hook.current.requestError).toBe("This VodHunter search link is invalid."));
    expect(getSearchJob).not.toHaveBeenCalled();
    expect(window.localStorage.getItem("vodhunter-public-active-search")).toBeNull();
  });

  it("moves a new browser search onto its canonical share URL", async () => {
    vi.mocked(createSearchJob).mockResolvedValue({
      search_id: 84,
      status: "queued",
      stage: "validating",
    });
    vi.mocked(getSearchJob).mockResolvedValue(job({ search_id: 84 }));
    const { result: hook } = renderHook(() => useSearchPage());

    act(() => {
      hook.current.onUrlChange(tiktokUrl);
      hook.current.onSelectStreamer("jason");
    });
    await act(async () => {
      hook.current.onSubmit({ preventDefault: vi.fn() } as never);
    });

    await waitFor(() => expect(createSearchJob).toHaveBeenCalled());
    expect(window.location.pathname).toBe("/share");
    expect(window.location.search).toBe("?search_id=84");
  });
});
