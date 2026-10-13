import { useState } from "react";
import { ChevronLeft, ChevronRight, ExternalLink } from "lucide-react";

import { SearchSegment } from "../../api/types";
import { formatTimelineTime } from "./searchUtils";
import { layoutMatchedArea } from "./matchedAreaLayout";

interface MatchedAreaTimelineProps {
  segments: readonly SearchSegment[];
  sourceHref: string | null;
  sourceTitle: string;
}

type Choice = {
  segment: SearchSegment;
  sourceHref: string | null;
} | null;

export function MatchedAreaTimeline({ segments, sourceHref, sourceTitle }: MatchedAreaTimelineProps) {
  const [choice, setChoice] = useState<Choice>(null);
  const orderedSegments = [...segments].sort((a, b) => a.query_start - b.query_start);
  const defaultSegment = orderedSegments.find((segment) => segment.video_url_at_timestamp === sourceHref)
    ?? orderedSegments[0];
  const activeSegment = choice && choice.sourceHref === sourceHref && segments.includes(choice.segment)
    ? choice.segment
    : defaultSegment;

  if (!activeSegment) return null;

  const layout = layoutMatchedArea({ segments: orderedSegments });
  const activeIndex = orderedSegments.indexOf(activeSegment);
  const clipRange = `${formatTimelineTime(activeSegment.query_start)}–${formatTimelineTime(activeSegment.query_end)}`;
  const vodRange = `${formatTimelineTime(activeSegment.vod_start)}–${formatTimelineTime(activeSegment.vod_end)}`;

  function chooseAdjacentMatch(direction: -1 | 1) {
    const segment = orderedSegments[activeIndex + direction];
    if (segment) setChoice({ segment, sourceHref });
  }

  return (
    <div className="mt-5">
      <div className="matched-area" role="group" aria-label={`Matched area in ${sourceTitle}`}>
        <ol className="matched-area-plot" aria-label="Matches in clip order">
          {layout.markers.map(({ segment, index, position, start, duration }) => (
            <li
              key={`${segment.query_start}-${segment.vod_start}-${segment.vod_end}`}
              className="matched-area-match"
              aria-current={segment === activeSegment ? "true" : undefined}
              aria-label={`Match ${index + 1}, clip ${formatTimelineTime(segment.query_start)}–${formatTimelineTime(segment.query_end)}, VOD ${formatTimelineTime(segment.vod_start)}–${formatTimelineTime(segment.vod_end)}`}
            >
              <span className="matched-area-bar" style={{ left: `${start}%`, width: `${duration}%` }} aria-hidden="true" />
              <span className="matched-area-marker" style={{ left: `${position}%` }} aria-hidden="true">
                <span className="matched-area-pin">{index + 1}</span>
                <span className="matched-area-stem" />
              </span>
              {segment === activeSegment ? (
                <span className="matched-area-point" style={{ left: `${position}%` }} aria-hidden="true" />
              ) : null}
            </li>
          ))}
        </ol>
        <div className="matched-area-track" aria-hidden="true" />
        <div className="matched-area-axis" aria-hidden="true">
          {layout.ticks.map((tick, index) => (
            <span key={tick} style={{ left: `${index * 25}%` }} className={index % 2 === 1 ? "matched-area-secondary-tick" : undefined}>
              {formatTimelineTime(tick)}
            </span>
          ))}
        </div>
      </div>
      <div className="matched-area-carousel rounded-xl border border-gray-700 bg-gray-800/30 p-3">
        <div aria-live="polite" aria-atomic="true">
          <p className="matched-area-count font-semibold text-white">Match {activeIndex + 1} of {orderedSegments.length}</p>
          <div className="grid gap-2 rounded-lg border border-gray-700 bg-gray-800/70 p-3">
            <p className="text-xs font-semibold uppercase tracking-[0.14em] text-gray-500">Clip {clipRange}</p>
            {activeSegment.video_url_at_timestamp ? (
              <a
                href={activeSegment.video_url_at_timestamp}
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center gap-2 break-all font-semibold text-white transition hover:text-[#fb2844]"
                aria-label={`Open matched segment ${activeIndex + 1} in ${sourceTitle} at ${formatTimelineTime(activeSegment.vod_start)}`}
              >
                {vodRange}<ExternalLink className="size-4 shrink-0" />
              </a>
            ) : (
              <p className="font-semibold text-white">{vodRange} <span className="font-normal text-gray-400">· Link unavailable</span></p>
            )}
            <span className="text-xs font-medium text-gray-400">Score {activeSegment.score.toFixed(3)}</span>
          </div>
        </div>
        <div className="matched-area-controls">
          <button
            type="button"
            className="matched-area-arrow"
            aria-label="Previous match"
            disabled={activeIndex === 0}
            onClick={() => chooseAdjacentMatch(-1)}
          >
            <ChevronLeft aria-hidden="true" className="size-5" />
          </button>
          <button
            type="button"
            className="matched-area-arrow"
            aria-label="Next match"
            disabled={activeIndex === orderedSegments.length - 1}
            onClick={() => chooseAdjacentMatch(1)}
          >
            <ChevronRight aria-hidden="true" className="size-5" />
          </button>
        </div>
      </div>
    </div>
  );
}
