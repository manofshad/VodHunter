import { fireEvent, render, screen } from "@testing-library/react";
import { createRef } from "react";
import { describe, expect, it, vi } from "vitest";

import { HistoryDrawer } from "./HistoryDrawer";
import { SearchHistoryEntry } from "./searchHistory";

function historyEntry(overrides: Partial<SearchHistoryEntry> = {}): SearchHistoryEntry {
  return {
    id: "jasontheween::tiktok.com/t/abc",
    searchedAt: new Date(2026, 8, 11, 18, 42, 0).toISOString(),
    tiktokUrl: "https://www.tiktok.com/t/abc?utm_source=share",
    streamer: "jasontheween",
    profileImageUrl: null,
    streamTitle: "Late Night Stream",
    twitchUrlAtTimestamp: "https://www.twitch.tv/videos/7?t=1h2m3s",
    timestampSeconds: 3723,
    additionalMatchCount: 3,
    ...overrides,
  };
}

describe("HistoryDrawer", () => {
  it("links only the title to Twitch and renders the history details outside the link", () => {
    render(
      <HistoryDrawer
        open
        entries={[historyEntry()]}
        returnFocusRef={createRef<HTMLButtonElement>()}
        onClose={vi.fn()}
        onClear={vi.fn()}
      />,
    );

    expect(screen.getByRole("dialog", { name: "History" })).toBeTruthy();
    expect(screen.getByRole("heading", { name: "Late Night Stream" })).toBeTruthy();
    expect(screen.getByText("tiktok.com/t/abc")).toBeTruthy();
    expect(screen.getByText("01:02:03")).toBeTruthy();
    expect(screen.getByText("+3 more")).toBeTruthy();

    const link = screen.getByRole("link", { name: "Late Night Stream" });
    expect(link.getAttribute("href")).toBe("https://www.twitch.tv/videos/7?t=1h2m3s");
    expect(link.getAttribute("target")).toBe("_blank");
    expect(link.contains(screen.getByText("tiktok.com/t/abc"))).toBe(false);
    expect(link.contains(screen.getByText("01:02:03"))).toBe(false);
  });

  it("copies the original TikTok URL", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });

    render(
      <HistoryDrawer
        open
        entries={[historyEntry()]}
        returnFocusRef={createRef<HTMLButtonElement>()}
        onClose={vi.fn()}
        onClear={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Copy TikTok URL" }));

    expect(writeText).toHaveBeenCalledWith("https://www.tiktok.com/t/abc?utm_source=share");
    expect(await screen.findByRole("button", { name: "TikTok URL copied" })).toBeTruthy();
  });

  it("closes from the close button, overlay, and Escape", () => {
    const onClose = vi.fn();
    render(
      <HistoryDrawer
        open
        entries={[]}
        returnFocusRef={createRef<HTMLButtonElement>()}
        onClose={onClose}
        onClear={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Close history" }));
    expect(onClose).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByRole("button", { name: "Close history overlay" }));
    expect(onClose).toHaveBeenCalledTimes(2);

    fireEvent.keyDown(window, { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(3);
  });

  it("clears entries and shows the empty state", () => {
    const onClear = vi.fn();
    const { rerender } = render(
      <HistoryDrawer
        open
        entries={[historyEntry()]}
        returnFocusRef={createRef<HTMLButtonElement>()}
        onClose={vi.fn()}
        onClear={onClear}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Clear all" }));
    expect(onClear).toHaveBeenCalledOnce();

    rerender(
      <HistoryDrawer
        open
        entries={[]}
        returnFocusRef={createRef<HTMLButtonElement>()}
        onClose={vi.fn()}
        onClear={onClear}
      />,
    );
    expect(screen.getByRole("heading", { name: "No search history" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Clear all" })).toBeNull();
  });
});
