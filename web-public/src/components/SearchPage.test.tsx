import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { SearchResponse } from "../api/types";
import { SearchResultCard, formatTimelineTime } from "./SearchPage";

function multiSegmentResult(): SearchResponse {
  return {
    found: true,
    streamer: "jasontheween",
    profile_image_url: null,
    video_id: 2848966623,
    video_url: "https://www.twitch.tv/videos/2848966623",
    video_url_at_timestamp: "https://www.twitch.tv/videos/2848966623?t=5h49m30s",
    thumbnail_url: null,
    title: "Matched stream",
    timestamp_seconds: 20970,
    score: 0.91,
    reason: "Accepted 2 supported segments",
    query_duration_seconds: 70,
    segments: [
      {
        query_start: 7.5,
        query_end: 11.5,
        video_id: 2848966623,
        vod_start: 20970,
        vod_end: 20974,
        video_url_at_timestamp: "https://www.twitch.tv/videos/2848966623?t=5h49m30s",
        score: 0.82,
      },
      {
        query_start: 30,
        query_end: 66,
        video_id: 2848966624,
        vod_start: 21275,
        vod_end: 21311,
        video_url_at_timestamp: "https://www.twitch.tv/videos/2848966624?t=5h54m35s",
        score: 0.91,
      },
    ],
    unmatched_ranges: [
      { query_start: 0, query_end: 7.5 },
      { query_start: 11.5, query_end: 30 },
      { query_start: 66, query_end: 70 },
    ],
  };
}

describe("SearchResultCard", () => {
  it("keeps the primary result and links every matched segment", () => {
    render(<SearchResultCard result={multiSegmentResult()} lastSubmittedUrl="https://tiktok.test/clip" />);

    expect(screen.getByRole("link", { name: "Matched stream" }).getAttribute("href")).toBe(
      "https://www.twitch.tv/videos/2848966623?t=5h49m30s",
    );
    expect(screen.getByText("05:49:30")).toBeTruthy();

    const first = screen.getByRole("link", { name: /Open matched segment 1 in VOD 2848966623/ });
    const second = screen.getByRole("link", { name: /Open matched segment 2 in VOD 2848966624/ });
    expect(first.getAttribute("href")).toContain("t=5h49m30s");
    expect(second.getAttribute("href")).toContain("t=5h54m35s");
    expect(screen.getByText("Clip 00:00:07.5–00:00:11.5")).toBeTruthy();
    expect(screen.getByText(/VOD 2848966624 · 05:54:35–05:55:11/)).toBeTruthy();
  });

  it("renders every unsupported query range with an evidence caveat", () => {
    render(<SearchResultCard result={multiSegmentResult()} lastSubmittedUrl="https://tiktok.test/clip" />);

    expect(screen.getByRole("heading", { name: "Unmatched clip ranges" })).toBeTruthy();
    expect(screen.getByText("00:00:00–00:00:07.5")).toBeTruthy();
    expect(screen.getByText("00:00:11.5–00:00:30")).toBeTruthy();
    expect(screen.getByText("00:01:06–00:01:10")).toBeTruthy();
    expect(screen.getByText(/did not contain enough continuous fingerprint evidence/i)).toBeTruthy();
  });

  it("shows a fully unmatched clip without inventing a segment", () => {
    const result = {
      ...multiSegmentResult(),
      found: false,
      video_id: null,
      video_url: null,
      video_url_at_timestamp: null,
      timestamp_seconds: null,
      score: null,
      segments: [],
      unmatched_ranges: [{ query_start: 0, query_end: 70 }],
    };
    render(<SearchResultCard result={result} lastSubmittedUrl="https://tiktok.test/clip" />);

    expect(screen.queryByRole("heading", { name: "Matched clip segments" })).toBeNull();
    expect(screen.getByText("00:00:00–00:01:10")).toBeTruthy();
  });
});

describe("formatTimelineTime", () => {
  it("keeps half-second boundaries", () => {
    expect(formatTimelineTime(21275.5)).toBe("05:54:35.5");
  });
});
