import { FormEvent, ReactNode, RefObject, useRef } from "react";
import { AlertCircle, Clipboard, FileVideo, Link2, TriangleAlert, Upload, X } from "lucide-react";

import { StreamerListItem } from "../../api/types";
import { DateRangePicker } from "./DateRangePicker";
import { StreamerPicker } from "./StreamerPicker";
import { ClipDraft, VIDEO_FILE_ACCEPT } from "./clipSource";
import { formatTimelineTime } from "./searchUtils";

interface SearchFormProps {
  source: ClipDraft;
  streamer: string;
  streamers: StreamerListItem[];
  loadingStreamers: boolean;
  streamerLoadError: string | null;
  streamerError: string | null;
  submitting: boolean;
  canSubmit: boolean;
  searchButtonLabel: string;
  streamerTriggerRef: RefObject<HTMLButtonElement>;
  streamedFrom: string;
  streamedTo: string;
  children: ReactNode;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onSourceModeChange: (mode: "link" | "upload") => void;
  onFileSelect: (file: File | null) => void;
  onUrlChange: (value: string) => void;
  onPaste: () => void;
  onSelectStreamer: (value: string) => void;
  onDateRangeChange: (streamedFrom: string, streamedTo: string) => void;
}

export function SearchForm({
  source,
  streamer,
  streamers,
  loadingStreamers,
  streamerLoadError,
  streamerError,
  submitting,
  canSubmit,
  searchButtonLabel,
  streamerTriggerRef,
  streamedFrom,
  streamedTo,
  children,
  onSubmit,
  onSourceModeChange,
  onFileSelect,
  onUrlChange,
  onPaste,
  onSelectStreamer,
  onDateRangeChange,
}: SearchFormProps) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const isUpload = source.kind !== "link";
  const filename = source.kind === "upload-selected" ? source.file.name
    : source.kind === "upload-restored" ? source.filename : null;

  return (
    <>
      <div className="mb-5 flex justify-center">
        <div role="group" aria-label="Clip source" className="inline-flex rounded-lg border border-gray-700 bg-gray-900 p-0.5">
          {(["link", "upload"] satisfies Array<"link" | "upload">).map((mode) => {
            const selected = mode === "upload" ? isUpload : !isUpload;
            const Icon = mode === "link" ? Link2 : Upload;
            return (
              <button
                key={mode}
                type="button"
                aria-pressed={selected}
                disabled={submitting}
                onClick={() => onSourceModeChange(mode)}
                className={`inline-flex h-8 items-center gap-2 rounded-md px-4 py-1.5 text-xs font-medium transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white disabled:cursor-not-allowed sm:text-[13px] ${selected ? "bg-gray-700 text-white" : "text-gray-400 hover:text-white"}`}
              >
                <Icon aria-hidden="true" className="size-3.5" />
                {mode === "link" ? "Link" : "Upload"}
              </button>
            );
          })}
        </div>
      </div>
      <form onSubmit={onSubmit} noValidate className="flex flex-col items-stretch gap-3">
        <div className="relative rounded-xl bg-gray-800 ring-1 ring-inset ring-gray-700">
          <div className="grid grid-cols-1 items-stretch gap-2 p-1 md:grid-cols-[184px_1px_minmax(0,1fr)_auto] md:items-center md:gap-0 md:p-0">
            <StreamerPicker
              streamer={streamer}
              streamers={streamers}
              loading={loadingStreamers}
              disabled={submitting}
              error={streamerError}
              triggerRef={streamerTriggerRef}
              onSelect={onSelectStreamer}
            />

            <div className="hidden self-stretch bg-gray-700 md:block" aria-hidden="true" />

            <div
              className="relative min-w-0 flex-1"
              onDragOver={isUpload ? (event) => event.preventDefault() : undefined}
              onDrop={isUpload ? (event) => {
                event.preventDefault();
                if (!submitting && event.dataTransfer.files[0]) {
                  onFileSelect(event.dataTransfer.files[0]);
                }
              } : undefined}
            >
              {source.kind === "link" ? (
                <>
                  <input
                    key="clip-link"
                    type="url"
                    aria-label="Clip link"
                    placeholder="Paste TikTok URL here"
                    value={source.url}
                    disabled={submitting}
                    onChange={(event) => onUrlChange(event.target.value)}
                    className="h-10 w-full border-0 bg-transparent px-4 pr-12 text-sm text-gray-100 outline-none placeholder:text-gray-400 disabled:cursor-not-allowed disabled:text-gray-500 md:h-12"
                  />
                  <button
                    type="button"
                    onClick={onPaste}
                    disabled={submitting}
                    className="absolute top-1/2 right-1 flex size-8 -translate-y-1/2 items-center justify-center rounded-md text-pink-400 transition hover:bg-gray-700 hover:text-pink-300 disabled:cursor-not-allowed disabled:text-gray-500"
                    aria-label="Paste clip link from clipboard"
                  >
                    <Clipboard className="size-4" style={{ color: "#fb2844" }} />
                  </button>
                </>
              ) : (
                <>
                  <input
                    key="clip-file"
                    ref={fileInputRef}
                    type="file"
                    accept={VIDEO_FILE_ACCEPT}
                    aria-label="Video file"
                    className="sr-only"
                    disabled={submitting}
                    onChange={(event) => {
                      const file = event.target.files?.[0];
                      if (file) {
                        onFileSelect(file);
                      }
                      event.target.value = "";
                    }}
                  />
                  <div className="flex h-10 min-w-0 items-center gap-2 px-4 pr-2 md:h-12">
                    <button
                      type="button"
                      disabled={submitting}
                      onClick={() => fileInputRef.current?.click()}
                      className="flex h-full min-w-0 flex-1 items-center gap-2 text-left text-sm text-gray-100 outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[#fb2844] disabled:cursor-not-allowed disabled:text-gray-500"
                      aria-label={filename ? `Replace video file ${filename}` : "Choose or drop a video file"}
                    >
                      {filename ? <FileVideo aria-hidden="true" className="size-4 shrink-0 text-gray-400" /> : <Upload aria-hidden="true" className="size-4 shrink-0 text-gray-400" />}
                      <span className={`min-w-0 truncate ${filename ? "" : "text-gray-400"}`} title={filename ?? undefined}>
                        {filename ?? "Choose or drop a video file"}
                      </span>
                    </button>
                    {source.kind === "upload-selected" ? (
                      <span className="hidden shrink-0 whitespace-nowrap text-xs text-gray-400 sm:inline">
                        {(source.file.size / (1024 * 1024)).toFixed(1)} MB
                        {source.durationSeconds === undefined ? "" : ` · ${formatTimelineTime(source.durationSeconds)}`}
                      </span>
                    ) : source.kind === "upload-restored" ? (
                      <span className="hidden shrink-0 text-xs text-gray-400 sm:inline">Select file again</span>
                    ) : null}
                    {filename ? (
                      <button
                        type="button"
                        disabled={submitting}
                        aria-label="Remove video file"
                        onClick={() => onFileSelect(null)}
                        className="flex size-8 shrink-0 items-center justify-center rounded-md text-gray-400 hover:bg-gray-700 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#fb2844] disabled:cursor-not-allowed"
                      >
                        <X aria-hidden="true" className="size-4" />
                      </button>
                    ) : null}
                  </div>
                </>
              )}
            </div>

            <button
              type="submit"
              disabled={submitting || !canSubmit}
              className="inline-flex h-10 items-center justify-center rounded-xl border-2 border-[#fb2844] bg-[#fb2844] px-8 text-base font-semibold text-white transition hover:border-[#f55b70] hover:bg-[#f55b70] disabled:border-gray-600 disabled:bg-gray-700 disabled:text-gray-400 md:m-1"
            >
              {searchButtonLabel}
            </button>
          </div>

          <DateRangePicker
            streamedFrom={streamedFrom}
            streamedTo={streamedTo}
            disabled={submitting}
            onChange={onDateRangeChange}
          />
        </div>

        <div className="min-h-0 text-left">
          {streamerError ? (
            <p id="streamer-error" className="flex items-center gap-2 text-sm font-medium text-white">
              <TriangleAlert className="size-4" />
              {streamerError}
            </p>
          ) : null}
          {streamerLoadError ? (
            <p className="flex items-center gap-2 text-sm font-medium text-white">
              <AlertCircle className="size-4" />
              {streamerLoadError}
            </p>
          ) : null}
        </div>

        {children}
      </form>
    </>
  );
}
