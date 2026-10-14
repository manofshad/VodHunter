import { afterEach, expect, it, vi } from "vitest";

import { getSearchJob, saveSearchFeedback } from "./client";

afterEach(() => {
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
  vi.resetModules();
});

it("saves a rating with the bearer header and returns the confirmed rating", async () => {
  const feedback = { rating: "down", updated_at: "2026-10-12T12:00:00Z" };
  const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify(feedback)));
  vi.stubGlobal("fetch", fetchMock);
  expect(await saveSearchFeedback("B".repeat(43), "down")).toEqual(feedback);
  const [url, options] = fetchMock.mock.calls[0];
  expect(url).toContain("/api/search/clip/feedback");
  expect(url).not.toContain("B".repeat(43));
  expect(options).toMatchObject({ method: "PUT", cache: "no-store", body: JSON.stringify({ rating: "down" }) });
  expect(new Headers(options.headers).get("Authorization")).toBe(`Bearer ${"B".repeat(43)}`);
});

it.each([
  { dev: false, apiBase: "", expectedBase: "/api" },
  { dev: true, apiBase: "", expectedBase: `http://${window.location.hostname}:8001/api` },
  { dev: false, apiBase: "  https://api.example.com/api  ", expectedBase: "https://api.example.com/api" },
  { dev: true, apiBase: "https://api.example.com/api", expectedBase: "https://api.example.com/api" },
])("requests streamers from $expectedBase when DEV=$dev and VITE_API_BASE='$apiBase'", async ({ dev, apiBase, expectedBase }) => {
  vi.stubEnv("DEV", dev);
  vi.stubEnv("VITE_API_BASE", apiBase);
  vi.resetModules();
  const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify([])));
  vi.stubGlobal("fetch", fetchMock);
  const { listSearchableStreamers } = await import("./client");

  await listSearchableStreamers();

  expect(fetchMock).toHaveBeenCalledWith(`${expectedBase}/search/streamers`);
});

it("sends the search capability in authorization without leaking it into the request URL", async () => {
  const searchToken = "A".repeat(43);
  const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ status: "queued" })));
  vi.stubGlobal("fetch", fetchMock);

  await getSearchJob(searchToken);

  const [requestUrl, options] = fetchMock.mock.calls[0];
  const url = new URL(requestUrl, window.location.href);
  expect(url.pathname).toBe("/api/search/clip");
  expect(url.search).toBe("");
  expect(url.hash).toBe("");
  expect(requestUrl).not.toContain(searchToken);
  expect(new Headers(options.headers).get("Authorization")).toBe(`Bearer ${searchToken}`);
  expect(options.cache).toBe("no-store");
});


it.each(["link", "upload"] as const)("submits exactly one %s clip source with shared search options", async (kind) => {
  const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ search_token: "A".repeat(43), status: "queued", stage: "validating" })));
  vi.stubGlobal("fetch", fetchMock);
  const { createSearchJob } = await import("./client");
  const options = { streamer: "jason", streamedFrom: "2026-10-01", streamedTo: "2026-10-12" };
  const file = new File(["clip bytes"], "moment.mkv", { type: "video/x-matroska" });
  await createSearchJob(kind === "link" ? { ...options, kind, url: "https://www.youtube.com/shorts/BaW_jenozKc" } : { ...options, kind, file });
  const [url, request] = fetchMock.mock.calls[0];
  expect(url).toContain("/api/search/clip");
  expect(request.method).toBe("POST");
  expect(request.headers).toBeUndefined();
  expect(request.body).toBeInstanceOf(FormData);
  const form: FormData = request.body;
  expect(form.get("streamer")).toBe("jason");
  expect(form.get("streamed_from")).toBe("2026-10-01");
  expect(form.get("streamed_to")).toBe("2026-10-12");
  expect([...form.keys()].sort()).toEqual([kind === "link" ? "tiktok_url" : "clip_file", "streamed_from", "streamed_to", "streamer"].sort());
  if (kind === "link") {
    expect(form.get("tiktok_url")).toBe("https://www.youtube.com/shorts/BaW_jenozKc");
  } else {
    expect(form.get("clip_file")).toBe(file);
  }
});
