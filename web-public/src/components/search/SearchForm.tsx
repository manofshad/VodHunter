import { FormEvent, ReactNode, RefObject } from "react";
import { AlertCircle, Clipboard, TriangleAlert } from "lucide-react";

import { StreamerListItem } from "../../api/types";
import { DateRangePicker } from "./DateRangePicker";
import { StreamerPicker } from "./StreamerPicker";

interface SearchFormProps {
  tiktokUrl: string;
  streamer: string;
  streamers: StreamerListItem[];
  loadingStreamers: boolean;
  streamerLoadError: string | null;
  streamerError: string | null;
  submitting: boolean;
  hasUrl: boolean;
  searchButtonLabel: string;
  streamerTriggerRef: RefObject<HTMLButtonElement>;
  streamedFrom: string;
  streamedTo: string;
  children: ReactNode;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onUrlChange: (value: string) => void;
  onPaste: () => void;
  onSelectStreamer: (value: string) => void;
  onDateRangeChange: (streamedFrom: string, streamedTo: string) => void;
}

export function SearchForm({
  tiktokUrl,
  streamer,
  streamers,
  loadingStreamers,
  streamerLoadError,
  streamerError,
  submitting,
  hasUrl,
  searchButtonLabel,
  streamerTriggerRef,
  streamedFrom,
  streamedTo,
  children,
  onSubmit,
  onUrlChange,
  onPaste,
  onSelectStreamer,
  onDateRangeChange,
}: SearchFormProps) {
  return (
    <form onSubmit={onSubmit} noValidate className="flex flex-col items-stretch gap-3">
      <div className="relative rounded-xl bg-gray-800">
        <div className="p-1 md:pl-0">
          <div className="flex flex-col items-stretch gap-2 md:flex-row md:items-center">
            <StreamerPicker
              streamer={streamer}
              streamers={streamers}
              loading={loadingStreamers}
              disabled={submitting}
              error={streamerError}
              triggerRef={streamerTriggerRef}
              onSelect={onSelectStreamer}
            />

            <div className="hidden h-10 w-px bg-gray-700 md:block" aria-hidden="true" />

            <div className="relative min-w-0 flex-1">
              <input
                type="url"
                placeholder="Paste TikTok URL here"
                value={tiktokUrl}
                disabled={submitting}
                onChange={(event) => onUrlChange(event.target.value)}
                className="h-10 w-full border-0 bg-gray-800 px-4 pr-12 text-sm text-gray-100 outline-none placeholder:text-gray-400 disabled:cursor-not-allowed disabled:text-gray-500"
              />
              <button
                type="button"
                onClick={onPaste}
                disabled={submitting}
                className="absolute top-1/2 right-1 flex size-8 -translate-y-1/2 items-center justify-center rounded-md text-pink-400 transition hover:bg-gray-700 hover:text-pink-300 disabled:cursor-not-allowed disabled:text-gray-500"
                aria-label="Paste TikTok URL from clipboard"
              >
                <Clipboard className="size-4" style={{ color: "#fb2844" }} />
              </button>
            </div>

            <button
              type="submit"
              disabled={submitting || !hasUrl}
              className="inline-flex h-10 items-center justify-center rounded-xl border-2 border-[#fb2844] bg-[#fb2844] px-8 text-base font-semibold text-white transition hover:border-[#f55b70] hover:bg-[#f55b70] disabled:border-gray-600 disabled:bg-gray-700 disabled:text-gray-400"
            >
              {searchButtonLabel}
            </button>
          </div>
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
  );
}
