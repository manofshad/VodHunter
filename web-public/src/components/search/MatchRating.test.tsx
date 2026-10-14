import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";

import { saveSearchFeedback } from "../../api/client";
import { SearchFeedbackResponse, SearchResponse } from "../../api/types";
import { MatchRating } from "./MatchRating";
import { SearchResultCard } from "./SearchResults";

vi.mock("../../api/client", () => ({ saveSearchFeedback: vi.fn() }));
const searchToken = "A".repeat(43);
const savedUp = { rating: "up", updated_at: "2026-10-12T12:00:00Z" } satisfies SearchFeedbackResponse;
const savedDown = { rating: "down", updated_at: "2026-10-12T12:01:00Z" } satisfies SearchFeedbackResponse;
const upButton = () => screen.getByRole("button", { name: "Thumbs up, correct match" });
const downButton = () => screen.getByRole("button", { name: "Thumbs down, incorrect match" });
const result: SearchResponse = {
  found: true, streamer: "jason", profile_image_url: null, video_id: 7, video_url: null,
  video_url_at_timestamp: null, thumbnail_url: null, title: "Matched VOD", timestamp_seconds: 100,
  score: 0.9, reason: "match", sources: [], segments: [], unmatched_ranges: [], query_duration_seconds: 10,
};

beforeEach(() => vi.resetAllMocks());

it("keeps the previous rating selected until the replacement is saved and disables both buttons", async () => {
  let resolveSave: (value: SearchFeedbackResponse) => void = () => { throw new Error("Save not started"); };
  vi.mocked(saveSearchFeedback).mockImplementation(() => new Promise(resolve => { resolveSave = resolve; }));
  render(<MatchRating search={{ searchToken, feedback: savedUp }} />);
  fireEvent.click(downButton());
  expect(upButton().getAttribute("aria-pressed")).toBe("true");
  expect(upButton().hasAttribute("disabled")).toBe(true);
  expect(downButton().hasAttribute("disabled")).toBe(true);
  expect(screen.getByRole("status").textContent).toBe("Saving feedback...");
  await act(async () => resolveSave(savedDown));
  expect(downButton().getAttribute("aria-pressed")).toBe("true");
  expect(screen.getByRole("status").textContent).toContain("Feedback saved");
  expect(saveSearchFeedback).toHaveBeenCalledWith(searchToken, "down");
});

it("retains the saved selection after failure and retries the attempted rating", async () => {
  vi.mocked(saveSearchFeedback).mockRejectedValueOnce(new Error("network")).mockResolvedValueOnce(savedDown);
  render(<MatchRating search={{ searchToken, feedback: savedUp }} />);
  fireEvent.click(downButton());
  await screen.findByRole("button", { name: "Retry" });
  expect(upButton().getAttribute("aria-pressed")).toBe("true");
  expect(screen.getByRole("status").textContent).toContain("Your previous rating is still saved");
  fireEvent.click(screen.getByRole("button", { name: "Retry" }));
  await waitFor(() => expect(downButton().getAttribute("aria-pressed")).toBe("true"));
  expect(saveSearchFeedback).toHaveBeenNthCalledWith(2, searchToken, "down");
});

it("isolates a delayed save from the next result through the result token key", async () => {
  let resolveSave: (value: SearchFeedbackResponse) => void = () => { throw new Error("Save not started"); };
  vi.mocked(saveSearchFeedback).mockImplementation(() => new Promise(resolve => { resolveSave = resolve; }));
  const { rerender } = render(<SearchResultCard result={result} submittedSource={null} completedSearchFeedback={{ searchToken, feedback: null }} />);
  fireEvent.click(upButton());
  const nextToken = "B".repeat(43);
  rerender(<SearchResultCard result={result} submittedSource={null} completedSearchFeedback={{ searchToken: nextToken, feedback: savedDown }} />);
  await act(async () => resolveSave(savedUp));
  expect(downButton().getAttribute("aria-pressed")).toBe("true");
  expect(upButton().getAttribute("aria-pressed")).toBe("false");
  expect(saveSearchFeedback).toHaveBeenCalledTimes(1);
});
