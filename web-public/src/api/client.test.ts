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
