import { RefObject, useEffect, useId, useRef, useState } from "react";
import { Check, ChevronDown } from "lucide-react";

import { StreamerListItem } from "../../api/types";
import { AvatarImage } from "./AvatarImage";

interface StreamerPickerProps {
  streamer: string;
  streamers: StreamerListItem[];
  loading: boolean;
  disabled: boolean;
  error: string | null;
  triggerRef: RefObject<HTMLButtonElement>;
  onSelect: (value: string) => void;
}

export function StreamerPicker({
  streamer,
  streamers,
  loading,
  disabled,
  error,
  triggerRef,
  onSelect,
}: StreamerPickerProps) {
  const [isOpen, setIsOpen] = useState(false);
  const streamerMenuRef = useRef<HTMLDivElement | null>(null);
  const streamerMenuId = useId();

  useEffect(() => {
    if (!isOpen) {
      return;
    }

    const onPointerDown = (event: MouseEvent) => {
      const target = event.target as Node;
      if (triggerRef.current?.contains(target) || streamerMenuRef.current?.contains(target)) {
        return;
      }
      setIsOpen(false);
    };

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setIsOpen(false);
        triggerRef.current?.focus();
      }
    };

    window.addEventListener("mousedown", onPointerDown);
    window.addEventListener("keydown", onKeyDown);

    return () => {
      window.removeEventListener("mousedown", onPointerDown);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [isOpen]);

  const selectStreamer = (value: string) => {
    onSelect(value);
    setIsOpen(false);
    triggerRef.current?.focus();
  };

  return (
    <div className="relative md:w-[184px] md:shrink-0">
      <button
        ref={triggerRef}
        type="button"
        disabled={disabled || loading || streamers.length === 0}
        aria-invalid={error ? "true" : "false"}
        aria-describedby={error ? "streamer-error" : undefined}
        aria-expanded={isOpen ? "true" : "false"}
        aria-controls={streamerMenuId}
        onClick={() => setIsOpen((open) => !open)}
        className="flex h-10 w-full items-center gap-2 border-0 bg-gray-800 px-4 text-sm font-medium text-gray-100 outline-none disabled:cursor-not-allowed disabled:text-gray-500 md:pl-5"
      >
        {streamer ? (
          <AvatarImage
            src={streamers.find((item) => item.name === streamer)?.profile_image_url}
            alt=""
            className="size-6 rounded-full object-cover"
            decorative
          />
        ) : null}
        <span className={streamer ? "text-gray-100" : "text-gray-400"}>
          {loading
            ? "Loading streamers..."
            : streamers.length === 0
              ? "No searchable streamers"
              : streamer || "Streamer"}
        </span>
        <ChevronDown className="ml-auto size-4 text-gray-500" />
      </button>

      {isOpen && !loading && streamers.length > 0 ? (
        <div
          ref={streamerMenuRef}
          id={streamerMenuId}
          role="listbox"
          className="absolute top-full left-0 z-20 w-full overflow-hidden border border-gray-700 bg-gray-800"
        >
          {streamers.map((item) => {
            const selected = item.name === streamer;
            return (
              <button
                key={item.name}
                type="button"
                role="option"
                aria-selected={selected}
                onClick={() => selectStreamer(item.name)}
                className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-gray-100 transition hover:bg-gray-700"
              >
                <AvatarImage
                  src={item.profile_image_url}
                  alt=""
                  className="size-6 rounded-full object-cover"
                  decorative
                />
                <span className="flex-1">{item.name}</span>
                {selected ? <Check className="size-4 text-[#fb2844]" /> : null}
              </button>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}
