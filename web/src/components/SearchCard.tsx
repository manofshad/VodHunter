import { FormEvent, useState } from "react";

import { searchClip } from "../api/client";
import { LiveStatusResponse, SearchResponse } from "../api/types";

interface Props {
  liveStatus: LiveStatusResponse;
}

export default function SearchCard({ liveStatus }: Props) {
  const [file, setFile] = useState<File | null>(null);
  const [result, setResult] = useState<SearchResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState<boolean>(false);

  const blocked = liveStatus.state !== "idle";

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!file) return;

    try {
      setSubmitting(true);
      setError(null);
      const next = await searchClip(file);
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
          onChange={(e) => setFile(e.target.files?.[0] ?? null)}
          disabled={submitting || blocked}
        />
        <button type="submit" disabled={submitting || blocked || !file}>
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
          <div><strong>Timestamp:</strong> {result.timestamp_seconds ?? "-"}</div>
          <div><strong>Score:</strong> {result.score ?? "-"}</div>
          <div><strong>Reason:</strong> {result.reason ?? "-"}</div>
        </div>
      )}
    </section>
  );
}
