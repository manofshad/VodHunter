import { fireEvent, render, screen } from "@testing-library/react";
import { createRef } from "react";
import { expect, it, vi } from "vitest";
import { SearchForm } from "./SearchForm";
import { ClipDraft } from "./clipSource";

function form(source: ClipDraft, onFileSelect = vi.fn()) {
  return <SearchForm source={source} streamer="jason" streamers={[{ name: "jason", profile_image_url: null }]}
    loadingStreamers={false} streamerLoadError={null} streamerError={null} submitting={false} canSubmit={source.kind === "upload-selected"}
    searchButtonLabel="Search" streamedFrom="" streamedTo="" streamerTriggerRef={createRef<HTMLButtonElement>()}
    onSubmit={vi.fn()} onSourceModeChange={vi.fn()} onFileSelect={onFileSelect} onUrlChange={vi.fn()} onPaste={vi.fn()}
    onSelectStreamer={vi.fn()} onDateRangeChange={vi.fn()}>{null}</SearchForm>;
}

it("accepts a picked file, resets the native picker, and supports selecting the same file again", () => {
  const onFileSelect = vi.fn();
  render(form({ kind: "upload-empty" }, onFileSelect));
  const input = screen.getByLabelText("Video file");
  const file = new File(["clip"], "moment.mp4");
  fireEvent.change(input, { target: { files: [file] } });
  expect(input).toBeInstanceOf(HTMLInputElement);
  if (!(input instanceof HTMLInputElement)) { throw new Error("Expected a native file picker"); }
  expect(input.value).toBe("");
  fireEvent.change(input, { target: { files: [file] } });
  expect(onFileSelect).toHaveBeenNthCalledWith(1, file);
  expect(onFileSelect).toHaveBeenNthCalledWith(2, file);
  expect(input.accept).toBe(".mp4,.mov,.m4v,.webm,.mkv");
});

it("accepts a dropped file in the input area and clears a selected file", () => {
  const onFileSelect = vi.fn();
  const file = new File(["clip"], "a-long-downloaded-moment.webm");
  const { rerender } = render(form({ kind: "upload-empty" }, onFileSelect));
  fireEvent.drop(screen.getByRole("button", { name: "Choose or drop a video file" }), { dataTransfer: { files: [file] } });
  expect(onFileSelect).toHaveBeenCalledWith(file);
  rerender(form({ kind: "upload-selected", file, durationSeconds: 12.5 }, onFileSelect));
  expect(screen.getByText(file.name)).toBeTruthy();
  expect(screen.getByText(/0.0 MB/).textContent).toBe("0.0 MB · 00:00:12.5");
  fireEvent.click(screen.getByRole("button", { name: "Remove video file" }));
  expect(onFileSelect).toHaveBeenLastCalledWith(null);
});

it("disables rerunning a restored upload and renders no idle limits banner", () => {
  render(form({ kind: "upload-restored", filename: "moment.mkv" }));
  expect(screen.getByRole("button", { name: "Search" }).hasAttribute("disabled")).toBe(true);
  expect(screen.getByRole("button", { name: "Replace video file moment.mkv" })).toBeTruthy();
  expect(screen.queryByText(/100 MB|3 minutes|MP4, MOV/)).toBeNull();
});
