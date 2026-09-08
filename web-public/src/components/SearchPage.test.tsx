import { fireEvent, render, screen, within } from "@testing-library/react";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";

import { SearchResponse } from "../api/types";
import { DateRangePicker, SearchResultCard, formatTimelineTime, isSupportedTikTokUrl } from "./SearchPage";

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
    sources: [
      {
        video_id: 2848966623,
        video_url: "https://www.twitch.tv/videos/2848966623",
        video_url_at_timestamp: "https://www.twitch.tv/videos/2848966623?t=5h49m30s",
        thumbnail_url: null,
        title: "Matched stream",
        streamer: "jasontheween",
        profile_image_url: null,
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
        ],
      },
      {
        video_id: 2848966624,
        video_url: "https://www.twitch.tv/videos/2848966624",
        video_url_at_timestamp: "https://www.twitch.tv/videos/2848966624?t=5h54m35s",
        thumbnail_url: null,
        title: "Another matched stream",
        streamer: "jasontheween",
        profile_image_url: null,
        segments: [
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
      },
    ],
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

function multipleSegmentsInOneSourceResult(): SearchResponse {
  const result = multiSegmentResult();
  const firstSource = result.sources[0];
  const secondSegment = {
    ...result.segments[1],
    video_id: firstSource.video_id,
    video_url_at_timestamp: "https://www.twitch.tv/videos/2848966623?t=5h54m35s",
  };

  return {
    ...result,
    video_url_at_timestamp: firstSource.video_url_at_timestamp,
    sources: [
      {
        ...firstSource,
        video_url_at_timestamp: secondSegment.video_url_at_timestamp,
        segments: [...firstSource.segments, secondSegment],
      },
    ],
    segments: [...firstSource.segments, secondSegment],
  };
}

describe("SearchResultCard", () => {
  it("groups matched segments by source VOD and links each source to its strongest match", () => {
    render(<SearchResultCard result={multiSegmentResult()} lastSubmittedUrl="https://tiktok.test/clip" />);

    expect(screen.getByRole("link", { name: "Matched stream" }).getAttribute("href")).toBe(
      "https://www.twitch.tv/videos/2848966623?t=5h49m30s",
    );
    expect(screen.getByRole("link", { name: "Another matched stream" }).getAttribute("href")).toBe(
      "https://www.twitch.tv/videos/2848966624?t=5h54m35s",
    );

    const first = screen.getByRole("link", { name: /Open matched segment 1 in Matched stream/ });
    const second = screen.getByRole("link", { name: /Open matched segment 1 in Another matched stream/ });
    expect(first.getAttribute("href")).toContain("t=5h49m30s");
    expect(second.getAttribute("href")).toContain("t=5h54m35s");
    expect(screen.getByText("Clip 00:00:07.5–00:00:11.5")).toBeTruthy();
    expect(screen.getByText("05:54:35–05:55:11")).toBeTruthy();
    expect(screen.queryByText(/VOD 284896662[34]/)).toBeNull();
    expect(screen.queryByText("Match reason")).toBeNull();
    expect(screen.queryByText("Score 0.91")).toBeNull();
    expect(screen.getAllByRole("heading", { name: "Matched clip segments" })).toHaveLength(2);
    expect(screen.getByRole("link", { name: "Open original TikTok clip" }).getAttribute("href")).toBe(
      "https://tiktok.test/clip",
    );
  });

  it("links a source title to the strongest segment within that source", () => {
    render(
      <SearchResultCard
        result={multipleSegmentsInOneSourceResult()}
        lastSubmittedUrl="https://tiktok.test/clip"
      />,
    );

    expect(screen.getByRole("link", { name: "Matched stream" }).getAttribute("href")).toContain("t=5h54m35s");
    expect(screen.getAllByRole("heading", { name: "Matched clip segments" })).toHaveLength(1);
    expect(screen.getAllByRole("link", { name: /Open matched segment/ })).toHaveLength(2);
  });

  it("renders every unmatched query range without technical explanation", () => {
    render(<SearchResultCard result={multiSegmentResult()} lastSubmittedUrl="https://tiktok.test/clip" />);

    expect(screen.getByRole("heading", { name: "Unmatched clip ranges" })).toBeTruthy();
    expect(screen.getByText("00:00:00–00:00:07.5")).toBeTruthy();
    expect(screen.getByText("00:00:11.5–00:00:30")).toBeTruthy();
    expect(screen.getByText("00:01:06–00:01:10")).toBeTruthy();
    expect(screen.queryByText(/did not contain enough continuous fingerprint evidence/i)).toBeNull();
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
      sources: [],
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

describe("isSupportedTikTokUrl", () => {
  it.each([
    "https://www.tiktok.com/@demo/video/1234567890",
    "https://www.tiktok.com/share/video/1234567890",
    "https://www.tiktok.com/embed/1234567890",
    "https://www.tiktok.com/t/ZP8ctwC2V/",
    "https://vm.tiktok.com/ZTR45GpSF/",
    "https://vt.tiktok.com/ZSe4FqkKd",
  ])("accepts supported video link %s", (url) => {
    expect(isSupportedTikTokUrl(url)).toBe(true);
  });

  it.each([
    "https://www.tiktok.com/@demo",
    "https://www.tiktok.com/@demo/live",
    "https://www.tiktok.com/music/song-123",
    "https://www.tiktok.com/@demo/photo/1234567890",
    "https://example.com/@demo/video/1234567890",
  ])("rejects non-video link %s", (url) => {
    expect(isSupportedTikTokUrl(url)).toBe(false);
  });
});

describe("DateRangePicker", () => {
  it("updates the field immediately and saves without a duplicate preview", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(2026, 7, 26));

    try {
      function StatefulDateRangePicker() {
        const [streamedFrom, setStreamedFrom] = useState("");
        const [streamedTo, setStreamedTo] = useState("");

        return (
          <DateRangePicker
            streamedFrom={streamedFrom}
            streamedTo={streamedTo}
            disabled={false}
            onChange={(nextFrom, nextTo) => {
              setStreamedFrom(nextFrom);
              setStreamedTo(nextTo);
            }}
          />
        );
      }

      render(<StatefulDateRangePicker />);

      const trigger = screen.getByRole("button", { name: "Stream date range" });
      fireEvent.click(trigger);
      expect(screen.getByRole("dialog", { name: "Choose stream date range" })).toBeTruthy();

      fireEvent.click(screen.getByRole("button", { name: "August 26, 2026" }));
      expect(trigger.textContent).toContain("08/26/2026 –");

      fireEvent.click(screen.getByRole("button", { name: "August 28, 2026" }));
      expect(trigger.textContent).toContain("08/26/2026 – 08/28/2026");

      const dialog = screen.getByRole("dialog", { name: "Choose stream date range" });
      expect(within(dialog).queryByText("08/26/2026 – 08/28/2026")).toBeNull();
      expect(screen.queryByRole("button", { name: "Cancel" })).toBeNull();
      expect(screen.queryByRole("button", { name: "Clear" })).toBeNull();

      fireEvent.click(screen.getByRole("button", { name: "Save" }));
      expect(screen.queryByRole("dialog", { name: "Choose stream date range" })).toBeNull();

      fireEvent.click(trigger);
      fireEvent.click(screen.getByRole("button", { name: "Reset" }));
      expect(trigger.textContent).toContain("mm/dd/yyyy – mm/dd/yyyy");
    } finally {
      vi.useRealTimers();
    }
  });
});
