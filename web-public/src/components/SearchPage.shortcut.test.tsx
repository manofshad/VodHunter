import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import { listSearchableStreamers } from "../api/client";
import SearchPage from "./SearchPage";

vi.mock("../api/client", () => ({
  createSearchJob: vi.fn(),
  getSearchJob: vi.fn(),
  listSearchableStreamers: vi.fn(),
}));

const originalShowModal = Object.getOwnPropertyDescriptor(HTMLDialogElement.prototype, "showModal");
const originalClose = Object.getOwnPropertyDescriptor(HTMLDialogElement.prototype, "close");

beforeAll(() => {
  Object.defineProperties(HTMLDialogElement.prototype, {
    showModal: {
      configurable: true,
      value(this: HTMLDialogElement) { this.setAttribute("open", ""); },
    },
    close: {
      configurable: true,
      value(this: HTMLDialogElement) {
        this.removeAttribute("open");
        this.dispatchEvent(new Event("close"));
      },
    },
  });
});

afterAll(() => {
  for (const [name, descriptor] of [["showModal", originalShowModal], ["close", originalClose]] satisfies [string, PropertyDescriptor | undefined][]) {
    if (descriptor) {
      Object.defineProperty(HTMLDialogElement.prototype, name, descriptor);
    } else {
      Reflect.deleteProperty(HTMLDialogElement.prototype, name);
    }
  }
});

describe("shortcut instructions", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    window.localStorage.clear();
    window.history.replaceState(null, "", "/");
    vi.mocked(listSearchableStreamers).mockResolvedValue([
      { name: "StableRonaldo", profile_image_url: null },
    ]);
  });

  it("opens from the navbar, offers the shortcut file, and preserves the search form", async () => {
    render(<SearchPage />);
    await waitFor(() => expect(screen.getByRole("combobox", { name: "Streamer" }).hasAttribute("disabled")).toBe(false));
    const urlInput = screen.getByPlaceholderText("Paste TikTok URL here");
    fireEvent.change(urlInput, { target: { value: "https://www.tiktok.com/@demo/video/123456789" } });
    fireEvent.click(screen.getByRole("combobox", { name: "Streamer" }));
    fireEvent.click(screen.getByRole("option", { name: "StableRonaldo" }));

    const shortcutButton = screen.getByRole("button", { name: "iPhone shortcut" });
    shortcutButton.focus();
    fireEvent.click(shortcutButton);

    const dialog = screen.getByRole("dialog", { name: "Search from TikTok" });
    const download = within(dialog).getByRole("link", { name: "Download shortcut" });
    expect(download.getAttribute("href")).toBe("/shortcuts/vodhunter-search.shortcut");
    expect(download.getAttribute("download")).toBe("vodhunter-search.shortcut");
    expect(dialog.getAttribute("aria-describedby")).toBe("vodhunter-shortcut-description");
    expect(document.activeElement).toBe(within(dialog).getByRole("button", { name: "Close shortcut instructions" }));
    expect(screen.queryByRole("dialog", { name: "History" })).toBeNull();

    fireEvent.click(within(dialog).getByRole("button", { name: "Close shortcut instructions" }));
    expect(screen.queryByRole("dialog", { name: "Search from TikTok" })).toBeNull();
    expect(shortcutButton.getAttribute("aria-expanded")).toBe("false");
    expect(document.activeElement).toBe(shortcutButton);
    expect(urlInput.getAttribute("value")).toBe("https://www.tiktok.com/@demo/video/123456789");
    expect(screen.getByRole("combobox", { name: "Streamer" }).textContent).toContain("StableRonaldo");
    expect(window.location.pathname).toBe("/");
  });

  it("synchronizes native Escape cancellation and can reopen", () => {
    render(<SearchPage />);
    const trigger = screen.getByRole("button", { name: "iPhone shortcut" });
    fireEvent.click(trigger);
    fireEvent(screen.getByRole("dialog", { name: "Search from TikTok" }), new Event("cancel", { cancelable: true }));
    expect(screen.queryByRole("dialog", { name: "Search from TikTok" })).toBeNull();
    expect(document.activeElement).toBe(trigger);
    fireEvent.click(trigger);
    expect(screen.getByRole("dialog", { name: "Search from TikTok" })).toBeTruthy();
  });

  it("dismisses a backdrop click while keeping clicks inside the instructions open", () => {
    render(<SearchPage />);
    fireEvent.click(screen.getByRole("button", { name: "iPhone shortcut" }));
    const dialog = screen.getByRole("dialog", { name: "Search from TikTok" });
    fireEvent.click(within(dialog).getByRole("heading", { name: "Search from TikTok" }));
    expect(dialog.hasAttribute("open")).toBe(true);
    fireEvent.click(dialog, { clientX: -1, clientY: -1 });
    expect(dialog.hasAttribute("open")).toBe(false);
    expect(document.activeElement).toBe(screen.getByRole("button", { name: "iPhone shortcut" }));
  });

  it("synchronizes a native close event with the navbar state", () => {
    render(<SearchPage />);
    const trigger = screen.getByRole("button", { name: "iPhone shortcut" });
    fireEvent.click(trigger);
    const dialog = screen.getByRole("dialog", { name: "Search from TikTok" });
    expect(dialog instanceof HTMLDialogElement).toBe(true);
    if (!(dialog instanceof HTMLDialogElement)) {
      throw new Error("Shortcut instructions must use a native dialog");
    }
    act(() => dialog.close());
    expect(trigger.getAttribute("aria-expanded")).toBe("false");
    expect(document.activeElement).toBe(trigger);
  });

  it("keeps history available after the shortcut instructions close", () => {
    render(<SearchPage />);
    fireEvent.click(screen.getByRole("button", { name: "iPhone shortcut" }));
    fireEvent.click(screen.getByRole("button", { name: "Close shortcut instructions" }));
    fireEvent.click(screen.getByRole("button", { name: "History" }));
    expect(screen.getByRole("dialog", { name: "History" })).toBeTruthy();
    expect(screen.queryByRole("dialog", { name: "Search from TikTok" })).toBeNull();
    expect(screen.getByRole("button", { name: "iPhone shortcut" }).getAttribute("aria-expanded")).toBe("false");
  });
});
