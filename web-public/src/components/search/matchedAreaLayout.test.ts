import { describe, expect, it } from "vitest";

import { SearchSegment } from "../../api/types";
import { layoutMatchedArea } from "./matchedAreaLayout";

function segment(queryStart: number, vodStart: number, vodEnd: number): SearchSegment {
  return {
    query_start: queryStart,
    query_end: queryStart + vodEnd - vodStart,
    video_id: 1,
    vod_start: vodStart,
    vod_end: vodEnd,
    video_url_at_timestamp: null,
    score: 0.8,
  };
}

describe("layoutMatchedArea", () => {
  it("projects true interval endpoints and midpoints without rearranging clip order", () => {
    const segments = [segment(0, 120, 124), segment(12, 100, 104), segment(30, 102, 106)];
    const layout = layoutMatchedArea({ segments });

    expect(layout.ticks).toEqual([90, 101.25, 112.5, 123.75, 135]);
    expect(layout.markers.map(({ segment: match, index }) => [match.query_start, index])).toEqual([[0, 0], [12, 1], [30, 2]]);
    const expectedTimes = [[120, 122, 124], [100, 102, 104], [102, 104, 106]];
    for (const [index, marker] of layout.markers.entries()) {
      const [start, midpoint, end] = expectedTimes[index];
      expect(marker.start).toBeCloseTo((start - 90) / 45 * 100);
      expect(marker.position).toBeCloseTo((midpoint - 90) / 45 * 100);
      expect(marker.start + marker.duration).toBeCloseTo((end - 90) / 45 * 100);
      expect(marker.position).toBeCloseTo(marker.start + marker.duration / 2);
    }
  });

  it("handles empty input and a match at the start of the VOD", () => {
    expect(layoutMatchedArea({ segments: [] })).toEqual({ markers: [], ticks: [] });
    const layout = layoutMatchedArea({ segments: [segment(0, 0, 4)] });
    expect(layout.ticks).toEqual([0, 3.5, 7, 10.5, 14]);
    expect(layout.markers[0].start).toBe(0);
    expect(layout.markers[0].position).toBeCloseTo(2 / 14 * 100);
  });
});
