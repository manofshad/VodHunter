import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { getSearchJob, listSearchableStreamers } from "../api/client";
import SearchPage from "./SearchPage";
import { SEARCH_HISTORY_STORAGE_KEY } from "./search/searchHistory";

vi.mock("../api/client", () => ({
  createSearchJob: vi.fn(),
  getSearchJob: vi.fn(),
  listSearchableStreamers: vi.fn(),
}));

const searchToken = "A".repeat(43);
const tiktokUrl = "https://www.tiktok.com/@demo/video/123456789";

describe("VodHunter home link", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    window.localStorage.clear();
    vi.mocked(listSearchableStreamers).mockResolvedValue([
      { name: "jason", profile_image_url: null },
    ]);
    vi.mocked(getSearchJob).mockResolvedValue({
      search_token: searchToken,
      status: "running",
      stage: "validating",
      input_type: "tiktok_url",
      clip_filename: null,
      tiktok_url: tiktokUrl,
      streamer: "jason",
      created_at: "2026-09-14T12:00:00Z",
      started_at: null,
      finished_at: null,
      result: null,
      error: null,
      feedback: null,
    });
  });

  it.each(["/", `/share#${searchToken}`])("opens a blank homepage from %s without resuming the search", async (path) => {
    window.history.replaceState(null, "", path);
    window.localStorage.setItem(
      "vodhunter-public-active-search",
      JSON.stringify({ searchToken, tiktokUrl }),
    );
    window.localStorage.setItem(SEARCH_HISTORY_STORAGE_KEY, "[]");
    const page = render(<SearchPage />);
    await waitFor(() => expect(screen.getByPlaceholderText("Paste TikTok URL here").getAttribute("value")).toBe(tiktokUrl));

    const logo = screen.getByRole("link", { name: "VodHunter" });
    expect(logo.getAttribute("href")).toBe("/");
    // jsdom cannot navigate; follow the link by remounting at its destination.
    logo.addEventListener("click", (event) => event.preventDefault());
    fireEvent.click(logo);
    expect(window.localStorage.getItem("vodhunter-public-active-search")).toBeNull();
    expect(window.localStorage.getItem(SEARCH_HISTORY_STORAGE_KEY)).toBe("[]");

    page.unmount();
    window.history.replaceState(null, "", "/");
    vi.mocked(getSearchJob).mockClear();
    render(<SearchPage />);
    await waitFor(() => expect(screen.getByRole("combobox", { name: "Streamer" }).hasAttribute("disabled")).toBe(false));
    expect(screen.getByPlaceholderText("Paste TikTok URL here").getAttribute("value")).toBe("");
    expect(screen.getByRole("button", { name: "Search" })).toBeTruthy();
    expect(getSearchJob).not.toHaveBeenCalled();
  });
});
