import { RefObject } from "react";
import * as Select from "@radix-ui/react-select";
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
  const placeholder = loading
    ? "Loading streamers..."
    : streamers.length === 0
      ? "No searchable streamers"
      : "Streamer";

  return (
    <div className="min-w-0 md:self-stretch">
      <Select.Root
        value={streamer}
        disabled={disabled || loading || streamers.length === 0}
        onValueChange={onSelect}
      >
        <Select.Trigger
          ref={triggerRef}
          aria-label="Streamer"
          aria-invalid={error ? "true" : "false"}
          aria-describedby={error ? "streamer-error" : undefined}
          className="group flex h-10 w-full items-center gap-2 border-0 bg-transparent px-4 text-sm font-medium text-gray-100 outline-none disabled:cursor-not-allowed disabled:text-gray-500 md:h-12"
        >
          <Select.Value placeholder={placeholder} />
          <Select.Icon asChild>
            <ChevronDown className="ml-auto size-4 shrink-0 text-gray-500 transition-transform group-data-[state=open]:rotate-180" />
          </Select.Icon>
        </Select.Trigger>

        <Select.Portal>
          <Select.Content
            position="popper"
            side="bottom"
            sideOffset={0}
            align="start"
            collisionPadding={8}
            className="z-20 max-h-60 w-[var(--radix-select-trigger-width)] overflow-hidden rounded-b-xl border border-gray-700 bg-gray-800"
          >
            <Select.Viewport className="max-h-60 overflow-y-auto">
              {streamers.map((item) => (
                <Select.Item
                  key={item.name}
                  value={item.name}
                  className="relative flex min-h-10 w-full cursor-default select-none items-center gap-2 px-4 py-2 text-left text-sm text-gray-100 outline-none data-[highlighted]:bg-gray-700"
                >
                  <Select.ItemText>
                    <span className="flex min-w-0 items-center gap-2">
                      <AvatarImage
                        src={item.profile_image_url}
                        alt=""
                        className="size-6 shrink-0 rounded-full object-cover"
                        decorative
                      />
                      <span className="truncate">{item.name}</span>
                    </span>
                  </Select.ItemText>
                  <Select.ItemIndicator asChild>
                    <Check className="ml-auto size-4 shrink-0 text-[#fb2844]" />
                  </Select.ItemIndicator>
                </Select.Item>
              ))}
            </Select.Viewport>
          </Select.Content>
        </Select.Portal>
      </Select.Root>
    </div>
  );
}
