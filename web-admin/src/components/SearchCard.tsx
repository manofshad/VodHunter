import { FormEvent, useState } from "react";

import { searchClip } from "../api/client";
import { LiveStatusResponse, SearchResponse } from "../api/types";

interface Props {
  liveStatus: LiveStatusResponse;
}

export default function SearchCard({ liveStatus }: Props) {
  const [file, setFile] = useState<File | null>(null);
  const [tiktokUrl, setTiktokUrl] = useState<string>("");
  const [result, setResult] = useState<SearchResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState<boolean>(false);

  const blocked = liveStatus.state !== "idle";
  const hasUrl = tiktokUrl.trim().length > 0;

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!file && !hasUrl) return;

    try {
      setSubmitting(true);
      setError(null);
      const next = file
        ? await searchClip({ type: "file", file })
        : await searchClip({ type: "tiktok_url", tiktokUrl: tiktokUrl.trim() });
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
          disabled={submitting || blocked || hasUrl}
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
          disabled={submitting || blocked || !!file}
        />
        <button type="submit" disabled={submitting || blocked || (!file && !hasUrl)}>
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
