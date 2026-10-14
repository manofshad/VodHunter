import { fireEvent, render, screen, within } from "@testing-library/react";
import { createRef, useState } from "react";
import { describe, expect, it, vi } from "vitest";

import { SearchResponse } from "../api/types";
import { StreamerPicker } from "./search/StreamerPicker";
import { SearchFeedback } from "./search/SearchFeedback";
import { MatchedAreaTimeline } from "./search/MatchedAreaTimeline";
import { DateRangePicker, SearchResultCard, formatTimelineTime, isSupportedClipUrl } from "./SearchPage";

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

function threeSegmentsInOneSourceResult(): SearchResponse {
  const result = multipleSegmentsInOneSourceResult();
  const source = result.sources[0];
  const first = source.segments[0];
  const middle = {
    ...first,
    query_start: 12,
    query_end: 16,
    vod_start: 22838,
    vod_end: 22842,
    video_url_at_timestamp: "https://www.twitch.tv/videos/2848966623?t=6h20m38s",
    score: 0.5,
  };
  const last = {
    ...source.segments[1],
    vod_start: 100,
    vod_end: 136,
    video_url_at_timestamp: "https://www.twitch.tv/videos/2848966623?t=1m40s",
  };
  const segments = [last, first, middle];
  return {
    ...result,
    sources: [{ ...source, segments, video_url_at_timestamp: middle.video_url_at_timestamp }],
    segments,
  };
}

describe("SearchResultCard", () => {
  it("groups matched segments by source VOD and links each source to its strongest match", () => {
    render(<SearchResultCard result={multiSegmentResult()} submittedSource={{ kind: "link", url: "https://tiktok.test/clip" }} />);

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
    expect(screen.getAllByText("Match 1 of 1")).toHaveLength(2);
    expect(screen.queryByRole("heading", { name: "Matched area" })).toBeNull();
    expect(screen.getByRole("link", { name: "Open original clip" }).getAttribute("href")).toBe(
      "https://tiktok.test/clip",
    );
  });

  it("links a source title to the strongest segment within that source", () => {
    render(
      <SearchResultCard
        result={multipleSegmentsInOneSourceResult()}
        submittedSource={{ kind: "link", url: "https://tiktok.test/clip" }}
      />,
    );

    expect(screen.getByRole("link", { name: "Matched stream" }).getAttribute("href")).toContain("t=5h54m35s");
    expect(screen.getByText("Match 2 of 2")).toBeTruthy();
    expect(screen.getByRole("listitem", { name: /Match 2, clip/ }).getAttribute("aria-current")).toBe("true");
    expect(screen.getByRole("link", { name: /Open matched segment 2/ }).getAttribute("href")).toContain("t=5h54m35s");
  });

  it("uses the source URL for the default and keeps title and thumbnail links stable during bounded navigation", () => {
    const result = threeSegmentsInOneSourceResult();
    render(<SearchResultCard result={result} submittedSource={{ kind: "link", url: "https://tiktok.test/clip" }} />);

    const title = screen.getByRole("link", { name: "Matched stream" });
    const thumbnail = screen.getByRole("link", { name: "Open Matched stream at its strongest match" });
    const previous = screen.getByRole("button", { name: "Previous match" });
    const next = screen.getByRole("button", { name: "Next match" });
    const sourceHref = "https://www.twitch.tv/videos/2848966623?t=6h20m38s";
    expect(screen.getByText("Match 2 of 3")).toBeTruthy();
    expect(screen.getByRole("listitem", { name: /Match 2, clip/ }).getAttribute("aria-current")).toBe("true");
    expect(screen.getByRole("link", { name: /Open matched segment 2/ }).getAttribute("href")).toBe(sourceHref);
    expect(screen.getByText("Score 0.500")).toBeTruthy();
    expect(previous.hasAttribute("disabled")).toBe(false);
    expect(next.hasAttribute("disabled")).toBe(false);

    fireEvent.click(previous);
    expect(screen.getByText("Match 1 of 3")).toBeTruthy();
    expect(screen.getByRole("listitem", { name: /Match 1, clip/ }).getAttribute("aria-current")).toBe("true");
    expect(screen.getByRole("listitem", { name: /Match 2, clip/ }).hasAttribute("aria-current")).toBe(false);
    expect(screen.getByText("Clip 00:00:07.5–00:00:11.5")).toBeTruthy();
    expect(screen.getByRole("link", { name: /Open matched segment 1/ }).getAttribute("href")).toBe(
      "https://www.twitch.tv/videos/2848966623?t=5h49m30s",
    );
    expect(previous.hasAttribute("disabled")).toBe(true);
    fireEvent.click(previous);
    expect(screen.getByText("Match 1 of 3")).toBeTruthy();
    fireEvent.click(next);
    expect(screen.getByText("Match 2 of 3")).toBeTruthy();
    fireEvent.click(next);
    expect(screen.getByText("Match 3 of 3")).toBeTruthy();
    expect(screen.getByRole("link", { name: /Open matched segment 3/ }).getAttribute("href")).toBe(
      "https://www.twitch.tv/videos/2848966623?t=1m40s",
    );
    expect(next.hasAttribute("disabled")).toBe(true);
    fireEvent.click(next);
    expect(screen.getByText("Match 3 of 3")).toBeTruthy();
    expect(title.getAttribute("href")).toBe(sourceHref);
    expect(thumbnail.getAttribute("href")).toBe(sourceHref);
    expect(previous.closest("[aria-live]")).toBeNull();
    expect(next.closest("[aria-live]")).toBeNull();
    const announcement = screen.getByText("Match 3 of 3").closest("[aria-live]");
    expect(announcement?.getAttribute("aria-live")).toBe("polite");
    expect(announcement?.textContent).toContain("Clip 00:00:30–00:01:06");
  });

  it("keeps timeline markers static on hover, click, and focus", () => {
    render(<SearchResultCard result={multipleSegmentsInOneSourceResult()} submittedSource={null} />);
    const first = screen.getByRole("listitem", { name: /Match 1, clip/ });
    const second = screen.getByRole("listitem", { name: /Match 2, clip/ });
    fireEvent.mouseEnter(first);
    fireEvent.focus(first);
    fireEvent.click(first);
    fireEvent.blur(first);
    fireEvent.mouseLeave(first);
    expect(screen.getByText("Match 2 of 2")).toBeTruthy();
    expect(screen.getByRole("link", { name: /Open matched segment 2/ }).getAttribute("href")).toBe(
      "https://www.twitch.tv/videos/2848966623?t=5h54m35s",
    );
    expect(second.getAttribute("aria-current")).toBe("true");
    expect(first.hasAttribute("tabindex")).toBe(false);
    expect(first.hasAttribute("aria-pressed")).toBe(false);
    expect(screen.queryByRole("button", { name: /Match \d, clip/ })).toBeNull();
    expect(screen.queryByRole("heading", { name: "Matched area" })).toBeNull();
    expect(screen.queryByText(/hover|click.*match|select.*match/i)).toBeNull();
  });

  it("numbers and traverses in clip order while projecting markers into VOD chronology", () => {
    const result = threeSegmentsInOneSourceResult();
    render(<SearchResultCard result={result} submittedSource={null} />);
    const markers = screen.getAllByRole("listitem");
    expect(markers.map((marker) => marker.getAttribute("aria-label"))).toEqual([
      "Match 1, clip 00:00:07.5–00:00:11.5, VOD 05:49:30–05:49:34",
      "Match 2, clip 00:00:12–00:00:16, VOD 06:20:38–06:20:42",
      "Match 3, clip 00:00:30–00:01:06, VOD 00:01:40–00:02:16",
    ]);
    const positions = markers.map((marker) => parseFloat(marker.querySelector<HTMLElement>(".matched-area-marker")?.style.left ?? ""));
    expect(positions[2]).toBeLessThan(positions[0]);
    expect(positions[0]).toBeLessThan(positions[1]);
    fireEvent.click(screen.getByRole("button", { name: "Next match" }));
    expect(screen.getByText("Match 3 of 3")).toBeTruthy();
    expect(screen.getByText("00:01:40–00:02:16")).toBeTruthy();
  });

  it("disables both arrows for a single match", () => {
    render(<SearchResultCard result={multiSegmentResult()} submittedSource={null} />);
    for (const source of screen.getAllByRole("article")) {
      const previous = within(source).getByRole("button", { name: "Previous match" });
      const next = within(source).getByRole("button", { name: "Next match" });
      expect(previous.hasAttribute("disabled")).toBe(true);
      expect(next.hasAttribute("disabled")).toBe(true);
      const detailHref = within(source).getByRole("link", { name: /Open matched segment/ }).getAttribute("href");
      fireEvent.click(previous);
      fireEvent.click(next);
      expect(within(source).getByText("Match 1 of 1")).toBeTruthy();
      expect(within(source).getByRole("link", { name: /Open matched segment/ }).getAttribute("href")).toBe(detailHref);
    }
  });

  it("resets to the URL default when the same VOD returns fresh result segments", () => {
    const { rerender } = render(<SearchResultCard result={threeSegmentsInOneSourceResult()} submittedSource={null} />);
    fireEvent.click(screen.getByRole("button", { name: "Previous match" }));
    expect(screen.getByText("Match 1 of 3")).toBeTruthy();

    rerender(<SearchResultCard result={threeSegmentsInOneSourceResult()} submittedSource={null} />);
    expect(screen.getByText("Match 2 of 3")).toBeTruthy();
    expect(screen.getByRole("link", { name: /Open matched segment 2/ }).getAttribute("href")).toBe(
      "https://www.twitch.tv/videos/2848966623?t=6h20m38s",
    );
  });

  it("resets a choice when the source URL changes even with retained segment objects", () => {
    const source = threeSegmentsInOneSourceResult().sources[0];
    const { rerender } = render(<MatchedAreaTimeline segments={source.segments} sourceHref={source.video_url_at_timestamp} sourceTitle="Matched stream" />);
    fireEvent.click(screen.getByRole("button", { name: "Previous match" }));
    expect(screen.getByText("Match 1 of 3")).toBeTruthy();

    rerender(<MatchedAreaTimeline segments={source.segments} sourceHref="https://www.twitch.tv/videos/2848966623?t=1m40s" sourceTitle="Matched stream" />);
    expect(screen.getByText("Match 3 of 3")).toBeTruthy();
    expect(screen.getByRole("listitem", { name: /Match 3, clip/ }).getAttribute("aria-current")).toBe("true");
    expect(screen.getByRole("link", { name: /Open matched segment 3/ }).getAttribute("href")).toBe(
      "https://www.twitch.tv/videos/2848966623?t=1m40s",
    );
  });

  it("preserves a choice when legacy source fallback arrays are rebuilt on rerender", () => {
    const result = { ...threeSegmentsInOneSourceResult(), sources: [], video_url_at_timestamp: "https://www.twitch.tv/videos/2848966623?t=6h20m38s" };
    const { rerender } = render(<SearchResultCard result={result} submittedSource={{ kind: "link", url: "https://tiktok.test/first" }} />);
    fireEvent.click(screen.getByRole("button", { name: "Previous match" }));
    expect(screen.getByText("Match 1 of 3")).toBeTruthy();

    rerender(<SearchResultCard result={result} submittedSource={{ kind: "link", url: "https://tiktok.test/updated" }} />);
    expect(screen.getByText("Match 1 of 3")).toBeTruthy();
    expect(screen.getByRole("link", { name: /Open matched segment 1/ }).getAttribute("href")).toBe(
      "https://www.twitch.tv/videos/2848966623?t=5h49m30s",
    );
  });

  it("keeps separate VOD carousels independent", () => {
    const result = threeSegmentsInOneSourceResult();
    const otherSource = multiSegmentResult().sources[1];
    const earlierSegment = {
      ...otherSource.segments[0],
      query_start: 7.5,
      query_end: 11.5,
      vod_start: 200,
      vod_end: 204,
      video_url_at_timestamp: "https://www.twitch.tv/videos/2848966624?t=3m20s",
    };
    result.sources.push({ ...otherSource, segments: [earlierSegment, ...otherSource.segments] });
    render(<SearchResultCard result={result} submittedSource={null} />);
    const [firstSource, secondSource] = screen.getAllByRole("article");

    fireEvent.click(within(firstSource).getByRole("button", { name: "Previous match" }));
    expect(within(firstSource).getByText("Match 1 of 3")).toBeTruthy();
    expect(within(secondSource).getByText("Match 2 of 2")).toBeTruthy();
    expect(within(secondSource).getByRole("listitem", { name: /Match 2, clip/ }).getAttribute("aria-current")).toBe("true");
    expect(within(secondSource).getByRole("link", { name: /Open matched segment 2/ }).getAttribute("href")).toBe(
      "https://www.twitch.tv/videos/2848966624?t=5h54m35s",
    );

    fireEvent.click(within(secondSource).getByRole("button", { name: "Previous match" }));
    expect(within(secondSource).getByText("Match 1 of 2")).toBeTruthy();
    expect(within(firstSource).getByText("Match 1 of 3")).toBeTruthy();
  });

  it("renders no carousel for empty segments", () => {
    const { container } = render(<MatchedAreaTimeline segments={[]} sourceHref={null} sourceTitle="Matched stream" />);
    expect(container.childElementCount).toBe(0);
    expect(screen.queryByRole("button", { name: "Next match" })).toBeNull();
  });

  it("falls back to clip order when the source URL is absent from the segments", () => {
    const source = threeSegmentsInOneSourceResult().sources[0];
    render(<MatchedAreaTimeline segments={source.segments} sourceHref="https://www.twitch.tv/videos/2848966623?t=unknown" sourceTitle="Matched stream" />);
    expect(screen.getByText("Match 1 of 3")).toBeTruthy();
    expect(screen.getByRole("link", { name: /Open matched segment 1/ }).getAttribute("href")).toBe(
      "https://www.twitch.tv/videos/2848966623?t=5h49m30s",
    );
  });

  it("does not render unmatched query ranges", () => {
    render(<SearchResultCard result={multiSegmentResult()} submittedSource={{ kind: "link", url: "https://tiktok.test/clip" }} />);

    expect(screen.queryByRole("heading", { name: "Unmatched clip ranges" })).toBeNull();
    expect(screen.queryByText("00:00:00–00:00:07.5")).toBeNull();
    expect(screen.queryByText("00:00:11.5–00:00:30")).toBeNull();
    expect(screen.queryByText("00:01:06–00:01:10")).toBeNull();
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
    render(<SearchResultCard result={result} submittedSource={{ kind: "link", url: "https://tiktok.test/clip" }} />);

    expect(screen.queryByRole("heading", { name: "Matched area" })).toBeNull();
    expect(screen.queryByRole("heading", { name: "Unmatched clip ranges" })).toBeNull();
    expect(screen.queryByText("00:00:00–00:01:10")).toBeNull();
    expect(screen.queryByRole("group", { name: "Rate this match" })).toBeNull();
  });

  it("offers one rating for a successful multi-source search", () => {
    render(<SearchFeedback requestError={null} submitting={false} activeSearchStage={null}
      result={multiSegmentResult()} submittedSource={{ kind: "link", url: "https://tiktok.test/clip" }}
      completedSearchFeedback={{ searchToken: "A".repeat(43), feedback: null }} />);
    expect(screen.getAllByRole("group", { name: "Rate this match" })).toHaveLength(1);
    expect(screen.getAllByRole("button", { name: "Thumbs up, correct match" })).toHaveLength(1);
    expect(screen.getAllByText("Match 1 of 1")).toHaveLength(2);
  });

  it.each([true, false])("hides rating while searching or without an authenticated completed token", (submitting) => {
    render(<SearchFeedback requestError={null} submitting={submitting} activeSearchStage="validating"
      result={multiSegmentResult()} submittedSource={{ kind: "link", url: "https://tiktok.test/clip" }} completedSearchFeedback={null} />);
    expect(screen.queryByRole("group", { name: "Rate this match" })).toBeNull();
  });
});

describe("SearchFeedback", () => {
  it("resets carousel selection across a new search submission", () => {
    const result = threeSegmentsInOneSourceResult();
    const { rerender } = render(<SearchFeedback requestError={null} submitting={false} activeSearchStage={null}
      result={result} submittedSource={{ kind: "link", url: "https://tiktok.test/first" }} completedSearchFeedback={null} />);
    fireEvent.click(screen.getByRole("button", { name: "Previous match" }));
    expect(screen.getByText("Match 1 of 3")).toBeTruthy();

    rerender(<SearchFeedback requestError={null} submitting={true} activeSearchStage="validating"
      result={null} submittedSource={{ kind: "link", url: "https://tiktok.test/second" }} completedSearchFeedback={null} />);
    expect(screen.queryByText("Match 1 of 3")).toBeNull();
    rerender(<SearchFeedback requestError={null} submitting={false} activeSearchStage={null}
      result={result} submittedSource={{ kind: "link", url: "https://tiktok.test/second" }} completedSearchFeedback={null} />);
    expect(screen.getByText("Match 2 of 3")).toBeTruthy();
  });

  it("shows only the simple no-match card for an unsuccessful search", () => {
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

    render(
      <SearchFeedback
        requestError={null}
        submitting={false}
        activeSearchStage={null}
        result={result}
        submittedSource={{ kind: "link", url: "https://tiktok.test/clip" }}
        completedSearchFeedback={null}
      />,
    );

    expect(screen.getByRole("heading", { name: "No exact match yet" })).toBeTruthy();
    expect(screen.queryByRole("heading", { name: "No matching Twitch VOD found" })).toBeNull();
    expect(screen.queryByText("00:00:00–00:01:10")).toBeNull();
  });
});

describe("formatTimelineTime", () => {
  it("keeps half-second boundaries", () => {
    expect(formatTimelineTime(21275.5)).toBe("05:54:35.5");
  });
});

describe("isSupportedClipUrl", () => {
  it.each([
    "https://www.tiktok.com/@demo/video/1234567890",
    "https://www.tiktok.com/share/video/1234567890",
    "https://www.tiktok.com/embed/1234567890",
    "https://www.tiktok.com/t/ZP8ctwC2V/",
    "https://vm.tiktok.com/ZTR45GpSF/",
    "https://vt.tiktok.com/ZSe4FqkKd",
    "https://www.instagram.com/reel/Cx_123-abc/?igsh=share",
    "https://instagram.com/reels/Cx_123-abc",
    "https://www.youtube.com/shorts/BaW_jenozKc?si=share",
    "https://youtube.com/shorts/BaW_jenozKc/",
    "https://m.youtube.com/shorts/BaW_jenozKc",
  ])("accepts supported video link %s", (url) => {
    expect(isSupportedClipUrl(url)).toBe(true);
  });

  it.each([
    "https://www.tiktok.com/@demo",
    "https://www.tiktok.com/@demo/live",
    "https://www.tiktok.com/music/song-123",
    "https://www.tiktok.com/@demo/photo/1234567890",
    "https://example.com/@demo/video/1234567890",
    "https://www.instagram.com/demo/",
    "https://www.instagram.com/p/Cx123/",
    "https://www.instagram.com/stories/demo/123/",
    "https://www.instagram.com/reels/audio/123/",
    "https://www.youtube.com/watch?v=BaW_jenozKc",
    "https://www.youtube.com/playlist?list=123",
    "https://www.youtube.com/@demo/shorts",
    "https://www.youtube.com/shorts/too-short",
    "https://youtu.be/BaW_jenozKc",
    "https://www.youtube.com.evil.test/shorts/BaW_jenozKc",
    "https://user@www.instagram.com/reel/Cx123/",
    "https://www.instagram.com:444/reel/Cx123/",
    "file://www.youtube.com/shorts/BaW_jenozKc",
  ])("rejects non-video link %s", (url) => {
    expect(isSupportedClipUrl(url)).toBe(false);
  });
});

describe("DateRangePicker", () => {
  it("keeps custom dates as a draft until Apply and collapses to a date-range pill", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(2026, 7, 26));

    try {
      const changes = vi.fn();

      function StatefulDateRangePicker() {
        const [streamedFrom, setStreamedFrom] = useState("");
        const [streamedTo, setStreamedTo] = useState("");

        return (
          <DateRangePicker
            streamedFrom={streamedFrom}
            streamedTo={streamedTo}
            disabled={false}
            onChange={(nextFrom, nextTo) => {
              changes(nextFrom, nextTo);
              setStreamedFrom(nextFrom);
              setStreamedTo(nextTo);
            }}
          />
        );
      }

      render(<StatefulDateRangePicker />);

      fireEvent.click(screen.getByRole("button", { name: "Custom" }));
      const customRange = screen.getByRole("region", { name: "Custom stream date range" });
      const apply = within(customRange).getByRole("button", { name: "Apply" });
      expect(apply.hasAttribute("disabled")).toBe(true);
      expect(screen.queryByText(/your search uses the range immediately/i)).toBeNull();

      fireEvent.click(screen.getByRole("button", { name: "August 26, 2026" }));
      expect(changes).not.toHaveBeenCalled();
      expect(within(customRange).getByText("08/26/2026 –")).toBeTruthy();
      expect(apply.hasAttribute("disabled")).toBe(true);

      fireEvent.click(screen.getByRole("button", { name: "August 28, 2026" }));
      expect(changes).not.toHaveBeenCalled();
      expect(within(customRange).getByText("08/26/2026 – 08/28/2026")).toBeTruthy();
      expect(apply.hasAttribute("disabled")).toBe(false);

      fireEvent.click(apply);
      expect(changes).toHaveBeenLastCalledWith("2026-08-26", "2026-08-28");
      expect(screen.queryByRole("region", { name: "Custom stream date range" })).toBeNull();

      const appliedRange = screen.getByRole("button", { name: "Aug 26–28" });
      expect(appliedRange.getAttribute("aria-pressed")).toBe("true");
      fireEvent.click(appliedRange);
      const reopenedRange = screen.getByRole("region", { name: "Custom stream date range" });

      fireEvent.click(screen.getByRole("button", { name: "Clear dates" }));
      expect(within(reopenedRange).getByText("mm/dd/yyyy – mm/dd/yyyy")).toBeTruthy();
      expect(within(reopenedRange).getByRole("button", { name: "Apply" }).hasAttribute("disabled")).toBe(true);
      expect(changes).toHaveBeenLastCalledWith("2026-08-26", "2026-08-28");

      fireEvent.click(within(reopenedRange).getByRole("button", { name: "Cancel" }));
      expect(screen.queryByRole("region", { name: "Custom stream date range" })).toBeNull();
      expect(screen.getByRole("button", { name: "Aug 26–28" })).toBeTruthy();

      fireEvent.click(screen.getByRole("button", { name: "Aug 26–28" }));
      fireEvent.click(screen.getByRole("button", { name: "Any time" }));
      expect(changes).toHaveBeenLastCalledWith("", "");
      expect(screen.queryByRole("region", { name: "Custom stream date range" })).toBeNull();
      expect(screen.getByRole("button", { name: "Any time" }).getAttribute("aria-pressed")).toBe("true");
    } finally {
      vi.useRealTimers();
    }
  });

  it("applies relative date presets immediately", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(2026, 7, 26));

    try {
      const onChange = vi.fn();
      render(<DateRangePicker streamedFrom="" streamedTo="" disabled={false} onChange={onChange} />);

      fireEvent.click(screen.getByRole("button", { name: "Past 7 days" }));
      expect(onChange).toHaveBeenLastCalledWith("2026-08-20", "2026-08-26");

      fireEvent.click(screen.getByRole("button", { name: "Past 30 days" }));
      expect(onChange).toHaveBeenLastCalledWith("2026-07-28", "2026-08-26");
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("StreamerPicker", () => {
  it("keeps the trigger seamless while attaching a self-contained menu", () => {
    const triggerRef = createRef<HTMLButtonElement>();
    render(
      <StreamerPicker
        streamer=""
        streamers={[{ name: "alice", profile_image_url: null }]}
        loading={false}
        disabled={false}
        error={null}
        triggerRef={triggerRef}
        onSelect={vi.fn()}
      />,
    );

    const trigger = screen.getByRole("combobox", { name: "Streamer" });
    fireEvent.click(trigger);

    expect(trigger.classList.contains("border-0")).toBe(true);
    expect(trigger.getAttribute("aria-expanded")).toBe("true");
    expect(screen.getByRole("listbox")).toBeTruthy();
    expect(screen.getByRole("option", { name: "alice" }).classList.contains("hover:bg-gray-700")).toBe(true);
    expect(screen.getByRole("option", { name: "alice" }).classList.contains("data-[highlighted]:bg-gray-700")).toBe(
      false,
    );
  });

  it("opens from the keyboard with ArrowDown", () => {
    const triggerRef = createRef<HTMLButtonElement>();
    render(
      <StreamerPicker
        streamer=""
        streamers={[{ name: "alice", profile_image_url: null }]}
        loading={false}
        disabled={false}
        error={null}
        triggerRef={triggerRef}
        onSelect={vi.fn()}
      />,
    );

    const trigger = screen.getByRole("combobox", { name: "Streamer" });
    trigger.focus();
    fireEvent.keyDown(trigger, { key: "ArrowDown" });

    expect(trigger.getAttribute("aria-expanded")).toBe("true");
    expect(screen.getByRole("listbox")).toBeTruthy();
    expect(screen.getByRole("option", { name: "alice" }).classList.contains("data-[highlighted]:bg-gray-700")).toBe(
      true,
    );
  });

  it("closes after a selection and returns focus to its trigger", () => {
    const onSelect = vi.fn();
    const triggerRef = createRef<HTMLButtonElement>();
    render(
      <StreamerPicker
        streamer=""
        streamers={[
          { name: "alice", profile_image_url: null },
          { name: "bob", profile_image_url: null },
        ]}
        loading={false}
        disabled={false}
        error={null}
        triggerRef={triggerRef}
        onSelect={onSelect}
      />,
    );

    const trigger = screen.getByRole("combobox", { name: "Streamer" });
    fireEvent.click(trigger);
    expect(screen.getByRole("listbox")).toBeTruthy();

    fireEvent.click(screen.getByRole("option", { name: "alice" }));

    expect(onSelect).toHaveBeenCalledWith("alice");
    expect(screen.queryByRole("listbox")).toBeNull();
    expect(document.activeElement).toBe(trigger);
  });

  it("closes on Escape and returns focus to its trigger", () => {
    const triggerRef = createRef<HTMLButtonElement>();
    render(
      <StreamerPicker
        streamer=""
        streamers={[{ name: "alice", profile_image_url: null }]}
        loading={false}
        disabled={false}
        error={null}
        triggerRef={triggerRef}
        onSelect={vi.fn()}
      />,
    );

    const trigger = screen.getByRole("combobox", { name: "Streamer" });
    fireEvent.click(trigger);
    fireEvent.keyDown(document.activeElement ?? window, { key: "Escape" });

    expect(screen.queryByRole("listbox")).toBeNull();
    expect(document.activeElement).toBe(trigger);
  });
});


it("shows upload identity without an original-clip link and preserves match feedback", () => {
  render(<SearchResultCard result={multiSegmentResult()} submittedSource={{ kind: "upload", filename: "downloaded-moment.mp4" }}
    completedSearchFeedback={{ searchToken: "A".repeat(43), feedback: null }} />);
  expect(screen.getByText("Uploaded file")).toBeTruthy();
  expect(screen.getByText("downloaded-moment.mp4")).toBeTruthy();
  expect(screen.queryByRole("link", { name: "Open original clip" })).toBeNull();
  expect(screen.getByRole("link", { name: "Matched stream" })).toBeTruthy();
  expect(screen.getByRole("group", { name: "Rate this match" })).toBeTruthy();
});
