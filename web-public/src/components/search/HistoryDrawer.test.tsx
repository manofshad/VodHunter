import { fireEvent, render, screen } from "@testing-library/react";
import { createRef } from "react";
import { describe, expect, it, vi } from "vitest";

import { HistoryDrawer } from "./HistoryDrawer";
import { SearchHistoryEntry } from "./searchHistory";

function historyEntry(overrides: Partial<SearchHistoryEntry> = {}): SearchHistoryEntry {
  return {
    id: "jasontheween::tiktok.com/t/abc",
    searchToken: "A".repeat(43),
    searchedAt: new Date(2026, 8, 11, 18, 42, 0).toISOString(),
    source: { kind: "link", url: "https://www.tiktok.com/t/abc?utm_source=share" },
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
  it("opens saved results through the title button while preserving the card details", () => {
    const entry = historyEntry();
    const onOpenEntry = vi.fn();
    render(
      <HistoryDrawer
        open
        entries={[entry]}
        returnFocusRef={createRef<HTMLButtonElement>()}
        onClose={vi.fn()}
        onClear={vi.fn()}
        onOpenEntry={onOpenEntry}
      />,
    );

    expect(screen.getByRole("dialog", { name: "History" })).toBeTruthy();
    expect(screen.getByRole("heading", { name: "Late Night Stream" })).toBeTruthy();
    expect(screen.getByText("tiktok.com/t/abc")).toBeTruthy();
    expect(screen.getByText("01:02:03")).toBeTruthy();
    expect(screen.getByText("+3 more")).toBeTruthy();

    const open = screen.getByRole("button", { name: "Open search results for Late Night Stream" });
    expect(open.tagName).toBe("BUTTON");
    expect(open.closest("h3")).toBeTruthy();
    fireEvent.click(screen.getByText("Late Night Stream"));
    expect(onOpenEntry).toHaveBeenCalledOnce();
    expect(onOpenEntry).toHaveBeenCalledWith(entry);
    expect(screen.queryByRole("link")).toBeNull();
  });

  it("copies the original TikTok URL", async () => {
    const onOpenEntry = vi.fn();
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
        onOpenEntry={onOpenEntry}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Copy clip link" }));

    expect(writeText).toHaveBeenCalledWith("https://www.tiktok.com/t/abc?utm_source=share");
    expect(await screen.findByRole("button", { name: "Clip link copied" })).toBeTruthy();
    expect(onOpenEntry).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Clip link copied" }).closest("button button")).toBeNull();
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
        onOpenEntry={vi.fn()}
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
        onOpenEntry={vi.fn()}
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
        onOpenEntry={vi.fn()}
      />,
    );
    expect(screen.getByRole("heading", { name: "No search history" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Clear all" })).toBeNull();
  });
});


it("opens an uploaded saved search without a copy-link action", () => {
  const onOpenEntry = vi.fn();
  render(<HistoryDrawer open entries={[historyEntry({ source: { kind: "upload", filename: "downloaded-moment.mp4" } })]}
    returnFocusRef={createRef<HTMLButtonElement>()} onClose={vi.fn()} onClear={vi.fn()} onOpenEntry={onOpenEntry} />);
  expect(screen.getByText("downloaded-moment.mp4")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Open search results for Late Night Stream" }));
  expect(onOpenEntry).toHaveBeenCalledOnce();
  expect(screen.queryByRole("link")).toBeNull();
  expect(screen.queryByRole("button", { name: /Copy/ })).toBeNull();
});

it("labels legacy searches as requiring another search", () => {
  render(<HistoryDrawer open entries={[historyEntry({ searchToken: null })]}
    returnFocusRef={createRef<HTMLButtonElement>()} onClose={vi.fn()} onClear={vi.fn()} onOpenEntry={vi.fn()} />);
  expect(screen.getByText("Search again to view results")).toBeTruthy();
  expect(screen.getByRole("button", { name: "Search again for Late Night Stream" })).toBeTruthy();
});
