import { SearchSegment } from "../../api/types";

export function layoutMatchedArea({ segments }: { segments: readonly SearchSegment[] }) {
  if (segments.length === 0) return { markers: [], ticks: [] };

  const firstTime = Math.min(...segments.map((segment) => segment.vod_start));
  const lastTime = Math.max(...segments.map((segment) => segment.vod_end));
  const padding = Math.max(10, (lastTime - firstTime) * 0.06);
  const targetStep = (lastTime - firstTime + padding * 2) / 4;
  const magnitude = 10 ** Math.floor(Math.log10(targetStep));
  const step = [1, 2, 3, 5, 6, 10].find((factor) => factor * magnitude >= targetStep) ?? 10;
  const rounding = Math.max(1, Math.round(step * magnitude / 4));
  const startTime = Math.max(0, Math.floor((firstTime - padding) / rounding) * rounding);
  const endTime = Math.ceil((lastTime + padding) / rounding) * rounding;
  const span = endTime - startTime;
  const markers = segments.map((segment, index) => ({
    segment,
    index,
    position: ((segment.vod_start + segment.vod_end) / 2 - startTime) / span * 100,
    start: (segment.vod_start - startTime) / span * 100,
    duration: (segment.vod_end - segment.vod_start) / span * 100,
  }));

  return {
    markers,
    ticks: Array.from({ length: 5 }, (_, index) => startTime + span * index / 4),
  };
}
