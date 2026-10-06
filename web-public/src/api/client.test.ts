import { afterEach, expect, it, vi } from "vitest";

import { getSearchJob } from "./client";

afterEach(() => vi.unstubAllGlobals());

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
