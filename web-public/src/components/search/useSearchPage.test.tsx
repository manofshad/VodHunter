import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { createSearchJob, getSearchJob, listSearchableStreamers } from "../../api/client";
import { SearchJobResponse, SearchResponse } from "../../api/types";
import { useSearchPage } from "./useSearchPage";

vi.mock("../../api/client", () => ({
  createSearchJob: vi.fn(),
  getSearchJob: vi.fn(),
  listSearchableStreamers: vi.fn(),
}));

const searchToken = "A".repeat(43);
const otherToken = "B".repeat(43);

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
    search_token: searchToken,
    status: "queued",
    stage: "validating",
    input_type: "tiktok_url",
    clip_filename: null,
    tiktok_url: tiktokUrl,
    streamer: "jason",
    created_at: "2026-09-14T12:00:00Z",
    started_at: null,
    finished_at: null,
    result: null,
    error: null,
    feedback: null,
    ...overrides,
  };
}

describe("useSearchPage shared jobs", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    window.localStorage.clear();
    window.history.replaceState(null, "", "/");
    vi.mocked(listSearchableStreamers).mockResolvedValue([
      { name: "jason", profile_image_url: null },
    ]);
  });

  it("gives a share URL priority over an older local search", async () => {
    window.localStorage.setItem(
      "vodhunter-public-active-search",
      JSON.stringify({ searchToken: otherToken, tiktokUrl: "https://www.tiktok.com/@old/video/1" }),
    );
    window.history.replaceState(null, "", `/share#${searchToken}`);
    vi.mocked(getSearchJob).mockResolvedValue(job());

    const { result: hook } = renderHook(() => useSearchPage());

    await waitFor(() => expect(getSearchJob).toHaveBeenCalledWith(searchToken));
    expect(getSearchJob).not.toHaveBeenCalledWith(otherToken);
    expect(hook.current.submitting).toBe(true);
    expect(hook.current.activeSearchStage).toBe("validating");
    expect(hook.current.source).toEqual({ kind: "link", url: tiktokUrl });
    expect(hook.current.streamer).toBe("jason");
    expect(window.localStorage.getItem("vodhunter-public-active-search")).toBeNull();
  });

  it.each([
    tiktokUrl,
    "https://www.instagram.com/reel/Cx123/",
    "https://www.youtube.com/shorts/BaW_jenozKc",
  ])("restores a completed result and its original clip URL %s", async (clipUrl) => {
    window.history.replaceState(null, "", `/share#${searchToken}`);
    vi.mocked(getSearchJob).mockResolvedValue(
      job({
        status: "completed",
        stage: null,
        finished_at: "2026-09-14T12:00:10Z",
        result: result(),
        tiktok_url: clipUrl,
      }),
    );

    const { result: hook } = renderHook(() => useSearchPage());

    await waitFor(() => expect(hook.current.result?.video_id).toBe(7));
    expect(hook.current.submitting).toBe(false);
    expect(hook.current.submittedSource).toEqual({ kind: "link", url: clipUrl });
    expect(hook.current.historyEntries).toHaveLength(1);
    expect(hook.current.completedSearchFeedback).toEqual({ searchToken, feedback: null });
  });

  it("restores a saved rating through authenticated job reload", async () => {
    window.history.replaceState(null, "", `/share#${searchToken}`);
    const feedback = { rating: "down", updated_at: "2026-10-12T12:00:00Z" } satisfies NonNullable<SearchJobResponse["feedback"]>;
    vi.mocked(getSearchJob).mockResolvedValue(job({ status: "completed", result: result(), feedback }));
    const { result: hook } = renderHook(() => useSearchPage());
    await waitFor(() => expect(hook.current.completedSearchFeedback).toEqual({ searchToken, feedback }));
    expect(hook.current.submitting).toBe(false);
  });

  it("does not enable ratings for a completed no-match", async () => {
    window.history.replaceState(null, "", `/share#${searchToken}`);
    vi.mocked(getSearchJob).mockResolvedValue(job({ status: "completed", result: { ...result(), found: false } }));
    const { result: hook } = renderHook(() => useSearchPage());
    await waitFor(() => expect(hook.current.result?.found).toBe(false));
    expect(hook.current.completedSearchFeedback).toBeNull();
  });

  it("shows a failed job error", async () => {
    window.history.replaceState(null, "", `/share#${searchToken}`);
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
    window.history.replaceState(null, "", `/share#${otherToken}`);
    vi.mocked(getSearchJob).mockRejectedValue(new Error("Search job was not found"));

    const { result: hook } = renderHook(() => useSearchPage());

    await waitFor(() => expect(hook.current.requestError).toBe("Search job was not found"));
    expect(hook.current.submitting).toBe(false);
  });

  it.each(["/share?search_id=42", "/share?search_id=not-a-job"])("rejects a legacy or malformed link %s instead of restoring local state", async (path) => {
    window.localStorage.setItem(
      "vodhunter-public-active-search",
      JSON.stringify({ searchToken: otherToken, tiktokUrl: "https://www.tiktok.com/@old/video/1" }),
    );
    window.history.replaceState(null, "", path);

    const { result: hook } = renderHook(() => useSearchPage());

    await waitFor(() => expect(hook.current.requestError).toBe("This VodHunter search link is invalid."));
    expect(getSearchJob).not.toHaveBeenCalled();
    expect(window.localStorage.getItem("vodhunter-public-active-search")).toBeNull();
  });

  it.each([
    tiktokUrl,
    "https://www.instagram.com/reel/Cx123/",
    "https://www.youtube.com/shorts/BaW_jenozKc",
  ])("submits clip %s and moves to its canonical share URL", async (clipUrl) => {
    vi.mocked(createSearchJob).mockResolvedValue({
      search_token: otherToken,
      status: "queued",
      stage: "validating",
    });
    vi.mocked(getSearchJob).mockResolvedValue(job({ search_token: otherToken, tiktok_url: clipUrl }));
    const { result: hook } = renderHook(() => useSearchPage());

    act(() => {
      hook.current.onUrlChange(clipUrl);
      hook.current.onSelectStreamer("jason");
    });
    await act(async () => {
      hook.current.onSubmit({ preventDefault: vi.fn() } as never);
    });

    await waitFor(() => expect(createSearchJob).toHaveBeenCalledWith({
      kind: "link",
      url: clipUrl,
      streamer: "jason",
      streamedFrom: undefined,
      streamedTo: undefined,
    }));
    expect(window.location.pathname).toBe("/share");
    expect(window.location.search).toBe("");
    expect(window.location.hash).toBe(`#${otherToken}`);
  });

  it("does not resume legacy numeric local storage", async () => {
    window.localStorage.setItem("vodhunter-public-active-search", JSON.stringify({ searchId: 99, tiktokUrl }));
    const { result: hook } = renderHook(() => useSearchPage());
    await waitFor(() => expect(hook.current.loadingStreamers).toBe(false));
    expect(getSearchJob).not.toHaveBeenCalled();
    expect(hook.current.submitting).toBe(false);
  });

  it("resumes a token-based search from local storage", async () => {
    window.localStorage.setItem("vodhunter-public-active-search", JSON.stringify({ searchToken, tiktokUrl }));
    vi.mocked(getSearchJob).mockResolvedValue(job());
    renderHook(() => useSearchPage());
    await waitFor(() => expect(getSearchJob).toHaveBeenCalledWith(searchToken));
  });
});


describe("useSearchPage uploaded clips", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    window.localStorage.clear();
    window.history.replaceState(null, "", "/");
    vi.mocked(listSearchableStreamers).mockResolvedValue([{ name: "jason", profile_image_url: null }]);
  });

  it("switches away from a URL, uploads the real file, and persists only metadata", async () => {
    let finishCreate: ((created: Awaited<ReturnType<typeof createSearchJob>>) => void) | undefined;
    vi.mocked(createSearchJob).mockImplementation(() => new Promise((resolve) => { finishCreate = resolve; }));
    vi.mocked(getSearchJob).mockResolvedValue(job({ input_type: "upload", clip_filename: "moment.mkv", tiktok_url: null }));
    const { result: hook } = renderHook(() => useSearchPage());
    await waitFor(() => expect(hook.current.loadingStreamers).toBe(false));
    const file = new File(["clip bytes"], "moment.mkv");
    act(() => {
      hook.current.onUrlChange(tiktokUrl);
      hook.current.onSourceModeChange("upload");
      hook.current.onFileSelect(file);
      hook.current.onSelectStreamer("jason");
      hook.current.onDateRangeChange("2026-10-01", "2026-10-12");
    });
    act(() => { hook.current.onSubmit({ preventDefault: vi.fn() } as never); });
    expect(hook.current.searchButtonLabel).toBe("Uploading...");
    expect(hook.current.activeSearchStage).toBe("uploading");
    expect(createSearchJob).toHaveBeenCalledWith({ kind: "upload", file, streamer: "jason", streamedFrom: "2026-10-01", streamedTo: "2026-10-12" });
    await act(async () => { finishCreate?.({ search_token: searchToken, status: "queued", stage: "validating" }); });
    expect(hook.current.searchButtonLabel).toBe("Searching...");
    expect(JSON.parse(window.localStorage.getItem("vodhunter-public-active-search") ?? "null")).toEqual({
      searchToken, source: { kind: "upload", filename: "moment.mkv" }, streamedFrom: "2026-10-01", streamedTo: "2026-10-12",
    });
    expect(hook.current.source).toEqual({ kind: "upload-selected", file });
  });

  it("keeps the real file after polling a sanitized filename and uploads it again", async () => {
    const file = new File(["clip bytes"], "  cafe\u0301   moment.mp4");
    vi.mocked(createSearchJob)
      .mockResolvedValueOnce({ search_token: searchToken, status: "queued", stage: "validating" })
      .mockResolvedValueOnce({ search_token: otherToken, status: "queued", stage: "validating" });
    vi.mocked(getSearchJob).mockImplementation(async (token) => job({
      search_token: token,
      status: "completed",
      input_type: "upload",
      clip_filename: "caf\u00e9 moment.mp4",
      tiktok_url: null,
      result: result(),
    }));
    const { result: hook } = renderHook(() => useSearchPage());
    await waitFor(() => expect(hook.current.loadingStreamers).toBe(false));
    act(() => {
      hook.current.onUrlChange(tiktokUrl);
      hook.current.onFileSelect(file);
      hook.current.onSelectStreamer("jason");
    });
    await act(async () => { hook.current.onSubmit({ preventDefault: vi.fn() } as never); });
    await waitFor(() => expect(hook.current.result?.found).toBe(true));
    expect(hook.current.submittedSource).toEqual({ kind: "upload", filename: "caf\u00e9 moment.mp4" });
    expect(hook.current.source).toEqual({ kind: "upload-selected", file });
    expect(hook.current.canSubmit).toBe(true);

    await act(async () => { hook.current.onSubmit({ preventDefault: vi.fn() } as never); });
    expect(createSearchJob).toHaveBeenNthCalledWith(2, {
      kind: "upload", file, streamer: "jason", streamedFrom: undefined, streamedTo: undefined,
    });
    await waitFor(() => expect(hook.current.completedSearchFeedback?.searchToken).toBe(otherToken));
    expect(hook.current.source).toEqual({ kind: "upload-selected", file });
    expect(hook.current.requestError).toBeNull();
  });

  it.each(["share", "saved"])("does not associate a selected file with a different %s upload token", async (entryPoint) => {
    window.localStorage.setItem("vodhunter-public-active-search", JSON.stringify({ searchToken, tiktokUrl, source: { kind: "upload", filename: "moment.mp4" } }));
    if (entryPoint === "share") { window.history.replaceState(null, "", `/share#${searchToken}`); }
    let finishPoll: ((value: SearchJobResponse) => void) | undefined;
    vi.mocked(getSearchJob).mockImplementation(() => new Promise((resolve) => { finishPoll = resolve; }));
    const { result: hook } = renderHook(() => useSearchPage());
    await waitFor(() => expect(getSearchJob).toHaveBeenCalledWith(searchToken));
    act(() => { hook.current.onFileSelect(new File(["different clip"], "moment.mp4")); });
    await act(async () => {
      finishPoll?.(job({ status: "completed", input_type: "upload", clip_filename: "moment.mp4", tiktok_url: tiktokUrl, result: result() }));
    });
    expect(hook.current.source).toEqual({ kind: "upload-restored", filename: "moment.mp4" });
    expect(hook.current.submittedSource).toEqual({ kind: "upload", filename: "moment.mp4" });
    expect(hook.current.canSubmit).toBe(false);
  });

  it.each(["share", "saved"])("restores %s upload metadata without a stale URL and requires a file before rerunning", async (entryPoint) => {
    window.localStorage.setItem("vodhunter-public-active-search", JSON.stringify({ searchToken, tiktokUrl, source: { kind: "upload", filename: "moment.mp4" } }));
    if (entryPoint === "share") { window.history.replaceState(null, "", `/share#${searchToken}`); }
    vi.mocked(getSearchJob).mockResolvedValue(job({ status: "completed", input_type: "upload", clip_filename: "moment.mp4", tiktok_url: tiktokUrl, result: result() }));
    const { result: hook } = renderHook(() => useSearchPage());
    await waitFor(() => expect(hook.current.result?.found).toBe(true));
    expect(hook.current.submittedSource).toEqual({ kind: "upload", filename: "moment.mp4" });
    expect(hook.current.source).toEqual({ kind: "upload-restored", filename: "moment.mp4" });
    expect(hook.current.canSubmit).toBe(false);
    expect(hook.current.historyEntries[0]).toMatchObject({ id: `upload::${searchToken}`, source: { kind: "upload", filename: "moment.mp4" } });
    await act(async () => { hook.current.onSubmit({ preventDefault: vi.fn() } as never); });
    expect(createSearchJob).not.toHaveBeenCalled();
    expect(hook.current.requestError).toBe("Choose a video file to search again.");
    act(() => { hook.current.onFileSelect(new File(["new clip"], "next.webm")); });
    expect(hook.current.canSubmit).toBe(true);
    expect(hook.current.submittedSource).toEqual({ kind: "upload", filename: "moment.mp4" });
    act(() => { hook.current.onSourceModeChange("link"); hook.current.onUrlChange(tiktokUrl); });
    expect(hook.current.submittedSource).toEqual({ kind: "upload", filename: "moment.mp4" });
    expect(hook.current.requestError).toBeNull();
  });

  it("never falls back to a stale link when an uploaded job has no filename", async () => {
    window.localStorage.setItem("vodhunter-public-active-search", JSON.stringify({ searchToken, tiktokUrl }));
    vi.mocked(getSearchJob).mockResolvedValue(job({ status: "completed", input_type: "upload", clip_filename: null, tiktok_url: null, result: result() }));
    const { result: hook } = renderHook(() => useSearchPage());
    await waitFor(() => expect(hook.current.result?.found).toBe(true));
    expect(hook.current.submittedSource).toBeNull();
    expect(hook.current.source).toEqual({ kind: "upload-empty" });
    expect(hook.current.canSubmit).toBe(false);
    expect(hook.current.historyEntries).toEqual([]);
  });

  it("rejects invalid extension and oversize files, then clears errors when switching modes", async () => {
    const { result: hook } = renderHook(() => useSearchPage());
    await waitFor(() => expect(hook.current.loadingStreamers).toBe(false));
    act(() => { hook.current.onSourceModeChange("upload"); hook.current.onFileSelect(new File(["text"], "notes.txt")); });
    expect(hook.current.canSubmit).toBe(false);
    expect(hook.current.requestError).toBe("Choose an MP4, MOV, M4V, WebM, or MKV video file.");
    const hugeFile = new File(["clip"], "large.mp4");
    Object.defineProperty(hugeFile, "size", { value: 100 * 1024 * 1024 + 1 });
    act(() => { hook.current.onFileSelect(hugeFile); });
    expect(hook.current.requestError).toBe("Choose a video file of 100 MB or smaller.");
    act(() => { hook.current.onSourceModeChange("link"); });
    expect(hook.current.requestError).toBeNull();
    expect(hook.current.source).toEqual({ kind: "link", url: "" });
    expect(createSearchJob).not.toHaveBeenCalled();
  });

  it("uses optional duration, revokes previews, and ignores metadata from replaced files", async () => {
    const createObjectURL = vi.fn().mockReturnValueOnce("blob:first").mockReturnValueOnce("blob:second");
    const revokeObjectURL = vi.fn();
    Object.defineProperty(URL, "createObjectURL", { configurable: true, value: createObjectURL });
    Object.defineProperty(URL, "revokeObjectURL", { configurable: true, value: revokeObjectURL });
    const videos: HTMLVideoElement[] = [];
    const originalCreate = document.createElement.bind(document);
    const createElement = vi.spyOn(document, "createElement").mockImplementation((tag, options) => {
      const element = originalCreate(tag, options);
      if (element instanceof HTMLVideoElement) { videos.push(element); }
      return element;
    });
    const { result: hook, unmount } = renderHook(() => useSearchPage());
    await waitFor(() => expect(hook.current.loadingStreamers).toBe(false));
    act(() => { hook.current.onFileSelect(new File(["first"], "first.mp4")); });
    const first = videos[0];
    const staleCallback = first.onloadedmetadata;
    Object.defineProperty(first, "duration", { value: 181 });
    act(() => { first.dispatchEvent(new Event("loadedmetadata")); });
    expect(hook.current.canSubmit).toBe(false);
    expect(hook.current.requestError).toBe("Choose a video no longer than 3 minutes.");
    act(() => { hook.current.onDateRangeChange("2026-10-01", "2026-10-12"); });
    expect(hook.current.requestError).toBe("Choose a video no longer than 3 minutes.");
    act(() => { hook.current.onSourceModeChange("upload"); });
    expect(hook.current.requestError).toBe("Choose a video no longer than 3 minutes.");
    const secondFile = new File(["second"], "second.mkv");
    act(() => { hook.current.onFileSelect(secondFile); });
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:first");
    act(() => { staleCallback?.call(first, new Event("loadedmetadata")); videos[1].dispatchEvent(new Event("error")); });
    expect(hook.current.source).toEqual({ kind: "upload-selected", file: secondFile });
    expect(hook.current.canSubmit).toBe(true);
    expect(hook.current.requestError).toBeNull();
    Object.defineProperty(videos[1], "duration", { value: 12.5 });
    act(() => { videos[1].dispatchEvent(new Event("loadedmetadata")); });
    expect(hook.current.source).toEqual({ kind: "upload-selected", file: secondFile, durationSeconds: 12.5 });
    unmount();
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:second");
    createElement.mockRestore();
    Reflect.deleteProperty(URL, "createObjectURL");
    Reflect.deleteProperty(URL, "revokeObjectURL");
  });
});


it("does not let a pending clipboard read replace a newly selected upload", async () => {
  vi.resetAllMocks();
  window.localStorage.clear();
  window.history.replaceState(null, "", "/");
  vi.mocked(listSearchableStreamers).mockResolvedValue([{ name: "jason", profile_image_url: null }]);
  let finishPaste: ((text: string) => void) | undefined;
  Object.defineProperty(navigator, "clipboard", { configurable: true, value: {
    readText: () => new Promise<string>((resolve) => { finishPaste = resolve; }),
  } });
  const { result: hook } = renderHook(() => useSearchPage());
  await waitFor(() => expect(hook.current.loadingStreamers).toBe(false));
  let paste: Promise<void> | undefined;
  act(() => { paste = hook.current.onPaste(); });
  const file = new File(["clip"], "moment.mp4");
  act(() => { hook.current.onSourceModeChange("upload"); hook.current.onFileSelect(file); });
  await act(async () => { finishPaste?.(tiktokUrl); await paste; });
  expect(hook.current.source).toEqual({ kind: "upload-selected", file });
  expect(hook.current.requestError).toBeNull();
});
