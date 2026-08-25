import { FormEvent, useEffect, useState } from "react";

import { listSearchableStreamers, searchClip } from "../api/client";
import { LiveStatusResponse, SearchResponse, StreamerListItem } from "../api/types";

interface Props {
  liveStatus: LiveStatusResponse;
}

export function formatTimelineTime(value: number): string {
  if (!Number.isFinite(value)) {
    return "Unknown time";
  }

  const totalTenths = Math.max(0, Math.round(value * 10));
  const hours = Math.floor(totalTenths / 36_000);
  const minutes = Math.floor((totalTenths % 36_000) / 600);
  const secondsWithTenths = (totalTenths % 600) / 10;
  const seconds = Number.isInteger(secondsWithTenths)
    ? String(secondsWithTenths).padStart(2, "0")
    : secondsWithTenths.toFixed(1).padStart(4, "0");

  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}:${seconds}`;
}

export function SearchResultDetails({ result }: { result: SearchResponse }) {
  const segments = result.segments ?? [];
  const unmatchedRanges = result.unmatched_ranges ?? [];

  return (
    <div className="result-box">
      <div><strong>Found:</strong> {result.found ? "yes" : "no"}</div>
      <div><strong>Streamer:</strong> {result.streamer ?? "-"}</div>
      <div><strong>Title:</strong> {result.title ?? "-"}</div>
      <div><strong>Primary VOD:</strong> {result.video_id ?? "-"}</div>
      <div><strong>URL:</strong> {result.video_url ?? "-"}</div>
      <div>
        <strong>Primary timestamp URL:</strong>{" "}
        {result.video_url_at_timestamp ? (
          <a href={result.video_url_at_timestamp} target="_blank" rel="noreferrer">
            {result.video_url_at_timestamp}
          </a>
        ) : (
          "-"
        )}
      </div>
      <div><strong>Primary timestamp:</strong> {result.timestamp_seconds ?? "-"}</div>
      <div><strong>Score:</strong> {result.score ?? "-"}</div>
      <div><strong>Reason:</strong> {result.reason ?? "-"}</div>
      {typeof result.query_duration_seconds === "number" ? (
        <div><strong>Query duration:</strong> {formatTimelineTime(result.query_duration_seconds)}</div>
      ) : null}

      {segments.length > 0 ? (
        <section className="segment-results" aria-labelledby="admin-matched-segments-heading">
          <h3 id="admin-matched-segments-heading">Matched clip segments</h3>
          <ol className="segment-list">
            {segments.map((segment, index) => (
              <li key={`${segment.video_id}-${segment.query_start}-${segment.vod_start}`}>
                <div>
                  <strong>
                    Clip {formatTimelineTime(segment.query_start)}–{formatTimelineTime(segment.query_end)}
                  </strong>
                  <div className="segment-source">
                    {segment.video_url_at_timestamp ? (
                      <a
                        href={segment.video_url_at_timestamp}
                        target="_blank"
                        rel="noreferrer"
                        aria-label={`Open matched segment ${index + 1} in VOD ${segment.video_id} at ${formatTimelineTime(segment.vod_start)}`}
                      >
                        VOD {segment.video_id} · {formatTimelineTime(segment.vod_start)}–{formatTimelineTime(segment.vod_end)}
                      </a>
                    ) : (
                      <span>
                        VOD {segment.video_id} · {formatTimelineTime(segment.vod_start)}–{formatTimelineTime(segment.vod_end)} · Link unavailable
                      </span>
                    )}
                  </div>
                </div>
                <span className="segment-score">Score {segment.score.toFixed(3)}</span>
              </li>
            ))}
          </ol>
        </section>
      ) : null}

      {unmatchedRanges.length > 0 ? (
        <section className="segment-results unmatched-results" aria-labelledby="admin-unmatched-ranges-heading">
          <h3 id="admin-unmatched-ranges-heading">Unmatched clip ranges</h3>
          <p>NMFP did not have enough continuous fingerprint evidence to identify these portions.</p>
          <ul className="unmatched-range-list">
            {unmatchedRanges.map((range) => (
              <li key={`${range.query_start}-${range.query_end}`}>
                {formatTimelineTime(range.query_start)}–{formatTimelineTime(range.query_end)}
              </li>
            ))}
          </ul>
        </section>
      ) : null}
    </div>
  );
}

export default function SearchCard({ liveStatus }: Props) {
  const [file, setFile] = useState<File | null>(null);
  const [tiktokUrl, setTiktokUrl] = useState<string>("");
  const [streamedFrom, setStreamedFrom] = useState<string>("");
  const [streamedTo, setStreamedTo] = useState<string>("");
  const [streamer, setStreamer] = useState<string>("");
  const [streamers, setStreamers] = useState<StreamerListItem[]>([]);
  const [loadingStreamers, setLoadingStreamers] = useState<boolean>(true);
  const [result, setResult] = useState<SearchResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState<boolean>(false);

  const blocked = liveStatus.state !== "idle";
  const hasFile = file !== null;
  const hasUrl = tiktokUrl.trim().length > 0;
  const hasStreamer = streamer.trim().length > 0;

  useEffect(() => {
    let cancelled = false;

    const loadStreamers = async () => {
      try {
        setLoadingStreamers(true);
        const next = await listSearchableStreamers();
        if (cancelled) return;
        setStreamers(next);
        setStreamer((current) => {
          if (current && next.some((item) => item.name === current)) {
            return current;
          }
          return "";
        });
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Could not load streamers");
      } finally {
        if (!cancelled) {
          setLoadingStreamers(false);
        }
      }
    };

    void loadStreamers();

    return () => {
      cancelled = true;
    };
  }, []);

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!hasStreamer || hasFile === hasUrl) return;

    try {
      setSubmitting(true);
      setError(null);
      const dateRange = {
        streamedFrom: streamedFrom.trim() || undefined,
        streamedTo: streamedTo.trim() || undefined,
      };
      const next = file
        ? await searchClip({ type: "file", file, streamer, ...dateRange })
        : await searchClip({ type: "tiktok_url", tiktokUrl: tiktokUrl.trim(), streamer, ...dateRange });
      setResult(next);
    } catch (err) {
      setResult(null);
      setError(err instanceof Error ? err.message : "Search failed");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <section className="card">
      <h2>Clip Search</h2>
      <form onSubmit={onSubmit} className="row">
        <select
          value={streamer}
          onChange={(e) => {
            setStreamer(e.target.value);
            setError(null);
            setResult(null);
          }}
          disabled={submitting || blocked || loadingStreamers || streamers.length === 0}
        >
          <option value="" disabled>
            {loadingStreamers ? "Loading streamers..." : streamers.length === 0 ? "No searchable streamers" : "Select streamer"}
          </option>
          {streamers.map((item) => (
            <option key={item.name} value={item.name}>
              {item.name}
            </option>
          ))}
        </select>
        <input
          type="file"
          accept="audio/*,video/*"
          onChange={(e) => {
            const next = e.target.files?.[0] ?? null;
            setFile(next);
            if (next) {
              setTiktokUrl("");
            }
          }}
          disabled={submitting || blocked || hasUrl || !hasStreamer}
        />
        <input
          type="url"
          placeholder="https://www.tiktok.com/@user/video/..."
          value={tiktokUrl}
          onChange={(e) => {
            const next = e.target.value;
            setTiktokUrl(next);
            if (next.trim().length > 0 && file) {
              setFile(null);
            }
          }}
          disabled={submitting || blocked || hasFile || !hasStreamer}
        />
        <button type="submit" disabled={submitting || blocked || !hasStreamer || hasFile === hasUrl}>
          Search
        </button>
      </form>
      <div className="row date-filter-row" aria-label="Stream date range">
        <label>
          Streamed from
          <input
            type="date"
            value={streamedFrom}
            max={streamedTo || undefined}
            onChange={(e) => {
              setStreamedFrom(e.target.value);
              setError(null);
              setResult(null);
            }}
            disabled={submitting || blocked}
          />
        </label>
        <label>
          Streamed to
          <input
            type="date"
            value={streamedTo}
            min={streamedFrom || undefined}
            onChange={(e) => {
              setStreamedTo(e.target.value);
              setError(null);
              setResult(null);
            }}
            disabled={submitting || blocked}
          />
        </label>
      </div>

      {blocked && <p className="message">Stop live monitor before running search.</p>}
      {error && <p className="message">{error}</p>}

      {result ? <SearchResultDetails result={result} /> : null}
    </section>
  );
}
