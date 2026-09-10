import { useEffect, useId, useRef, useState } from "react";
import { ChevronLeft, ChevronRight } from "lucide-react";

const DATE_RANGE_PLACEHOLDER = "mm/dd/yyyy – mm/dd/yyyy";
const WEEKDAYS = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"];

function parseDateInput(value: string): Date | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return null;
  }

  const [year, month, day] = value.split("-").map(Number);
  const date = new Date(year, month - 1, day);
  if (date.getFullYear() !== year || date.getMonth() !== month - 1 || date.getDate() !== day) {
    return null;
  }

  return date;
}

function formatDateInput(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function formatDateDisplay(value: string): string {
  const date = parseDateInput(value);
  if (date === null) {
    return "";
  }

  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${month}/${day}/${date.getFullYear()}`;
}

function formatDateRangeDisplay(streamedFrom: string, streamedTo: string): string {
  const from = formatDateDisplay(streamedFrom);
  const to = formatDateDisplay(streamedTo);

  if (!from && !to) {
    return DATE_RANGE_PLACEHOLDER;
  }
  if (!to) {
    return `${from} –`;
  }
  if (!from) {
    return `– ${to}`;
  }

  return `${from} – ${to}`;
}

function formatDateRangePill(streamedFrom: string, streamedTo: string): string {
  const from = parseDateInput(streamedFrom);
  const to = parseDateInput(streamedTo);
  if (from === null || to === null) {
    return "Custom";
  }

  const fromMonth = from.toLocaleDateString("en-US", { month: "short" });
  const toMonth = to.toLocaleDateString("en-US", { month: "short" });
  if (from.getFullYear() === to.getFullYear() && from.getMonth() === to.getMonth()) {
    return `${fromMonth} ${from.getDate()}–${to.getDate()}`;
  }
  if (from.getFullYear() === to.getFullYear()) {
    return `${fromMonth} ${from.getDate()}–${toMonth} ${to.getDate()}`;
  }

  return `${fromMonth} ${from.getDate()}, ${from.getFullYear()}–${toMonth} ${to.getDate()}, ${to.getFullYear()}`;
}

function recentDateRange(dayCount: number): [string, string] {
  const to = new Date();
  const from = new Date(to);
  from.setDate(from.getDate() - (dayCount - 1));
  return [formatDateInput(from), formatDateInput(to)];
}

function startOfMonth(date: Date): Date {
  return new Date(date.getFullYear(), date.getMonth(), 1);
}

function addMonths(date: Date, amount: number): Date {
  return new Date(date.getFullYear(), date.getMonth() + amount, 1);
}

function getCalendarDays(month: Date): Date[] {
  const firstDay = startOfMonth(month);
  const daysInMonth = new Date(month.getFullYear(), month.getMonth() + 1, 0).getDate();
  const leadingDays = firstDay.getDay();
  const totalCells = Math.ceil((leadingDays + daysInMonth) / 7) * 7;

  return Array.from({ length: totalCells }, (_, index) => {
    return new Date(month.getFullYear(), month.getMonth(), index - leadingDays + 1);
  });
}

function isDateInRange(value: string, streamedFrom: string, streamedTo: string): boolean {
  return Boolean(streamedFrom && streamedTo && value > streamedFrom && value < streamedTo);
}

interface CalendarMonthProps {
  month: Date;
  streamedFrom: string;
  streamedTo: string;
  onSelect: (value: string) => void;
  onPrevious: () => void;
  onNext: () => void;
}

function CalendarMonth({ month, streamedFrom, streamedTo, onSelect, onPrevious, onNext }: CalendarMonthProps) {
  const monthLabel = month.toLocaleDateString("en-US", { month: "short", year: "numeric" });

  return (
    <section aria-label={monthLabel}>
      <div className="mb-2 grid grid-cols-[2rem_1fr_2rem] items-center">
        <button
          type="button"
          aria-label="Previous month"
          onClick={onPrevious}
          className="flex size-8 items-center justify-center rounded-md text-gray-300 transition hover:bg-gray-800 hover:text-white"
        >
          <ChevronLeft className="size-4" />
        </button>
        <h3 className="text-center text-sm font-semibold text-gray-100">{monthLabel}</h3>
        <button
          type="button"
          aria-label="Next month"
          onClick={onNext}
          className="flex size-8 items-center justify-center rounded-md text-gray-300 transition hover:bg-gray-800 hover:text-white"
        >
          <ChevronRight className="size-4" />
        </button>
      </div>
      <div className="grid grid-cols-7 gap-0.5 text-center text-[0.64rem] font-semibold uppercase tracking-wide text-gray-500">
        {WEEKDAYS.map((weekday) => (
          <span key={weekday} aria-hidden="true" className="py-0.5">
            {weekday}
          </span>
        ))}
      </div>
      <div className="mt-0.5 grid grid-cols-7 gap-0.5 text-sm">
        {getCalendarDays(month).map((date) => {
          const value = formatDateInput(date);
          const isCurrentMonth = date.getMonth() === month.getMonth();
          const isStart = value === streamedFrom;
          const isEnd = value === streamedTo;
          const isSelected = isStart || isEnd;
          const isInRange = isDateInRange(value, streamedFrom, streamedTo);

          return (
            <button
              key={value}
              type="button"
              aria-label={date.toLocaleDateString("en-US", {
                month: "long",
                day: "numeric",
                year: "numeric",
              })}
              aria-pressed={isSelected}
              onClick={() => onSelect(value)}
              className={`flex h-8 items-center justify-center rounded-md transition ${
                isSelected
                  ? "bg-[#fb2844] font-semibold text-white"
                  : isInRange
                    ? "bg-[#fb2844]/20 text-gray-100"
                    : isCurrentMonth
                      ? "text-gray-200 hover:bg-gray-800"
                      : "text-gray-600 hover:bg-gray-800/60"
              }`}
            >
              {date.getDate()}
            </button>
          );
        })}
      </div>
    </section>
  );
}

interface DateRangePickerProps {
  streamedFrom: string;
  streamedTo: string;
  disabled: boolean;
  onChange: (streamedFrom: string, streamedTo: string) => void;
}

export function DateRangePicker({ streamedFrom, streamedTo, disabled, onChange }: DateRangePickerProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [draftFrom, setDraftFrom] = useState(streamedFrom);
  const [draftTo, setDraftTo] = useState(streamedTo);
  const [visibleMonth, setVisibleMonth] = useState(() =>
    startOfMonth(parseDateInput(streamedFrom) ?? parseDateInput(streamedTo) ?? new Date()),
  );
  const customTriggerRef = useRef<HTMLButtonElement | null>(null);
  const customPanelId = useId();

  useEffect(() => {
    if (!isOpen) {
      setDraftFrom(streamedFrom);
      setDraftTo(streamedTo);
    }
  }, [isOpen, streamedFrom, streamedTo]);

  useEffect(() => {
    if (!isOpen) {
      return;
    }
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setDraftFrom(streamedFrom);
        setDraftTo(streamedTo);
        setIsOpen(false);
        customTriggerRef.current?.focus();
      }
    };

    window.addEventListener("keydown", onKeyDown);

    return () => {
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [isOpen, streamedFrom, streamedTo]);

  const openCustomPicker = () => {
    setDraftFrom(streamedFrom);
    setDraftTo(streamedTo);
    setVisibleMonth(startOfMonth(parseDateInput(streamedFrom) ?? parseDateInput(streamedTo) ?? new Date()));
    setIsOpen(true);
  };

  const selectDate = (value: string) => {
    if (!draftFrom || draftTo) {
      setDraftFrom(value);
      setDraftTo("");
      return;
    }

    if (value < draftFrom) {
      setDraftTo(draftFrom);
      setDraftFrom(value);
      return;
    }

    setDraftTo(value);
  };

  const cancelPicker = () => {
    setDraftFrom(streamedFrom);
    setDraftTo(streamedTo);
    setIsOpen(false);
    customTriggerRef.current?.focus();
  };

  const savePicker = () => {
    if (!draftFrom || !draftTo) {
      return;
    }
    onChange(draftFrom, draftTo);
    setIsOpen(false);
    customTriggerRef.current?.focus();
  };

  const resetPicker = () => {
    setDraftFrom("");
    setDraftTo("");
  };

  const selectPreset = (dayCount: number | null) => {
    const range: [string, string] = dayCount === null ? ["", ""] : recentDateRange(dayCount);
    onChange(range[0], range[1]);
    setDraftFrom(range[0]);
    setDraftTo(range[1]);
    setIsOpen(false);
  };

  const [weekFrom, weekTo] = recentDateRange(7);
  const [monthFrom, monthTo] = recentDateRange(30);
  const appliedScope =
    !streamedFrom && !streamedTo
      ? "any"
      : streamedFrom === weekFrom && streamedTo === weekTo
        ? "week"
        : streamedFrom === monthFrom && streamedTo === monthTo
          ? "month"
          : "custom";
  const selectedScope = isOpen ? "custom" : appliedScope;
  const pillClass = (selected: boolean) =>
    `inline-flex h-5 items-center rounded-full border px-2 text-[0.65rem] font-semibold leading-none transition disabled:cursor-not-allowed disabled:opacity-50 ${
      selected
        ? "border-[#fb2844] bg-[#fb2844]/15 text-white"
        : "border-transparent bg-gray-700/75 text-gray-300 hover:border-gray-500 hover:text-white"
    }`;

  return (
    <div className="border-t border-gray-700/80 text-left">
      <div className="flex min-h-8 flex-wrap items-center gap-1.5 px-3 py-2">
        <span className="mr-1 text-[0.61rem] font-semibold uppercase tracking-[0.14em] text-gray-400">Streamed</span>
        <button
          type="button"
          disabled={disabled}
          aria-pressed={selectedScope === "any"}
          onClick={() => selectPreset(null)}
          className={pillClass(selectedScope === "any")}
        >
          Any time
        </button>
        <button
          type="button"
          disabled={disabled}
          aria-pressed={selectedScope === "week"}
          onClick={() => selectPreset(7)}
          className={pillClass(selectedScope === "week")}
        >
          Past 7 days
        </button>
        <button
          type="button"
          disabled={disabled}
          aria-pressed={selectedScope === "month"}
          onClick={() => selectPreset(30)}
          className={pillClass(selectedScope === "month")}
        >
          Past 30 days
        </button>
        <button
          ref={customTriggerRef}
          type="button"
          disabled={disabled}
          aria-pressed={selectedScope === "custom"}
          aria-expanded={isOpen}
          aria-controls={customPanelId}
          onClick={isOpen ? cancelPicker : openCustomPicker}
          className={pillClass(selectedScope === "custom")}
        >
          {appliedScope === "custom" ? formatDateRangePill(streamedFrom, streamedTo) : "Custom"}
        </button>
      </div>

      {isOpen ? (
        <section id={customPanelId} aria-label="Custom stream date range" className="border-t border-gray-700 bg-gray-900/35">
          <div className="flex flex-col gap-2 border-b border-gray-700 px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
            <p className="text-sm font-semibold text-white">Custom range</p>
            <p className="rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-xs font-medium text-gray-100">
              {formatDateRangeDisplay(draftFrom, draftTo)}
            </p>
          </div>

          <div className="mx-auto w-full max-w-[30rem] px-4 py-3">
            <CalendarMonth
              month={visibleMonth}
              streamedFrom={draftFrom}
              streamedTo={draftTo}
              onSelect={selectDate}
              onPrevious={() => setVisibleMonth((month) => addMonths(month, -1))}
              onNext={() => setVisibleMonth((month) => addMonths(month, 1))}
            />
          </div>

          <div className="flex items-center justify-end gap-2 border-t border-gray-700 px-3 py-2">
            <button
              type="button"
              onClick={resetPicker}
              className="mr-auto px-1 py-2 text-xs font-semibold text-gray-300 underline decoration-gray-500 underline-offset-4 transition hover:text-white"
            >
              Clear dates
            </button>
            <button
              type="button"
              onClick={cancelPicker}
              className="rounded-lg border border-gray-600 bg-gray-700 px-3 py-2 text-xs font-semibold text-gray-200 transition hover:bg-gray-600"
            >
              Cancel
            </button>
            <button
              type="button"
              disabled={!draftFrom || !draftTo}
              onClick={savePicker}
              className="rounded-lg bg-[#fb2844] px-3 py-2 text-xs font-semibold text-white transition hover:bg-[#f55b70] disabled:cursor-not-allowed disabled:bg-gray-700 disabled:text-gray-500"
            >
              Apply
            </button>
          </div>
        </section>
      ) : null}
    </div>
  );
}
