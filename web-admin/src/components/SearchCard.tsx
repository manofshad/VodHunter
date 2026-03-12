import { FormEvent, useEffect, useState } from "react";

import { listSearchableStreamers, searchClip } from "../api/client";
import { LiveStatusResponse, SearchResponse, StreamerListItem } from "../api/types";

interface Props {
  liveStatus: LiveStatusResponse;
}

export default function SearchCard({ liveStatus }: Props) {
  const [tiktokUrl, setTiktokUrl] = useState<string>("");
  const [streamer, setStreamer] = useState<string>("");
  const [streamers, setStreamers] = useState<StreamerListItem[]>([]);
  const [loadingStreamers, setLoadingStreamers] = useState<boolean>(true);
  const [result, setResult] = useState<SearchResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState<boolean>(false);

  const blocked = liveStatus.state !== "idle";
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
    if (!hasStreamer || !hasUrl) return;

    try {
      setSubmitting(true);
      setError(null);
      const next = await searchClip({ tiktokUrl: tiktokUrl.trim(), streamer });
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
          type="url"
          placeholder="https://www.tiktok.com/@user/video/..."
          value={tiktokUrl}
          onChange={(e) => {
            setTiktokUrl(e.target.value);
          }}
          disabled={submitting || blocked || !hasStreamer}
        />
        <button type="submit" disabled={submitting || blocked || !hasStreamer || !hasUrl}>
          Search
        </button>
      </form>

      {blocked && <p className="message">Stop live monitor before running search.</p>}
      {error && <p className="message">{error}</p>}

      {result && (
        <div className="result-box">
          <div><strong>Found:</strong> {result.found ? "yes" : "no"}</div>
          <div><strong>Streamer:</strong> {result.streamer ?? "-"}</div>
          <div><strong>Title:</strong> {result.title ?? "-"}</div>
          <div><strong>URL:</strong> {result.video_url ?? "-"}</div>
          <div>
            <strong>Timestamp URL:</strong>{" "}
            {result.video_url_at_timestamp ? (
              <a href={result.video_url_at_timestamp} target="_blank" rel="noreferrer">
                {result.video_url_at_timestamp}
              </a>
            ) : (
              "-"
            )}
          </div>
          <div><strong>Timestamp:</strong> {result.timestamp_seconds ?? "-"}</div>
          <div><strong>Score:</strong> {result.score ?? "-"}</div>
          <div><strong>Reason:</strong> {result.reason ?? "-"}</div>
        </div>
      )}
    </section>
  );
}
