import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { SearchResponse } from "../api/types";
import { SearchResultDetails } from "./SearchCard";

function resultFixture(): SearchResponse {
  return {
    found: true,
    streamer: "jasontheween",
    profile_image_url: null,
    video_id: 100,
    video_url: "https://www.twitch.tv/videos/100",
    video_url_at_timestamp: "https://www.twitch.tv/videos/100?t=1m40s",
    thumbnail_url: null,
    title: "Admin match",
    timestamp_seconds: 100,
    score: 0.88,
    reason: "Accepted 2 supported segments",
    query_duration_seconds: 16,
    segments: [
      {
        query_start: 0,
        query_end: 5,
        video_id: 100,
        vod_start: 100,
        vod_end: 105,
        video_url_at_timestamp: "https://www.twitch.tv/videos/100?t=1m40s",
        score: 0.88,
      },
      {
        query_start: 8,
        query_end: 14,
        video_id: 200,
        vod_start: 300,
        vod_end: 306,
        video_url_at_timestamp: null,
        score: 0.75,
      },
    ],
    unmatched_ranges: [
      { query_start: 5, query_end: 8 },
      { query_start: 14, query_end: 16 },
    ],
  };
}

describe("SearchResultDetails", () => {
  it("keeps primary compatibility fields and renders all segment identities", () => {
    render(<SearchResultDetails result={resultFixture()} />);

    expect(screen.getByText("Primary VOD:").parentElement?.textContent).toContain("100");
    expect(screen.getByText("Primary timestamp:").parentElement?.textContent).toContain("100");
    const linkedSegment = screen.getByRole("link", { name: /Open matched segment 1 in VOD 100/ });
    expect(linkedSegment.getAttribute("href")).toContain("t=1m40s");
    expect(screen.getByText(/VOD 200 · 00:05:00–00:05:06 · Link unavailable/)).toBeTruthy();
    expect(screen.getByText("Score 0.750")).toBeTruthy();
  });

  it("renders unmatched ranges and explains insufficient evidence", () => {
    render(<SearchResultDetails result={resultFixture()} />);

    expect(screen.getByRole("heading", { name: "Unmatched clip ranges" })).toBeTruthy();
    expect(screen.getByText("00:00:05–00:00:08")).toBeTruthy();
    expect(screen.getByText("00:00:14–00:00:16")).toBeTruthy();
    expect(screen.getByText(/did not have enough continuous fingerprint evidence/i)).toBeTruthy();
  });
});
