import { FormEvent, useState } from "react";

import { searchClip } from "../api/client";
import { SearchResponse } from "../api/types";

export default function SearchPage() {
  const [file, setFile] = useState<File | null>(null);
  const [tiktokUrl, setTiktokUrl] = useState<string>("");
  const [result, setResult] = useState<SearchResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState<boolean>(false);

  const hasUrl = tiktokUrl.trim().length > 0;

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!file && !hasUrl) return;

    try {
      setSubmitting(true);
      setError(null);
      setResult(null);

      const next = file
        ? await searchClip({ type: "file", file })
        : await searchClip({ type: "tiktok_url", tiktokUrl: tiktokUrl.trim() });
      setResult(next);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Search failed");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <main className="page-shell">
      <section className="hero">
        <h1>Find the source VOD in seconds</h1>
        <p>Upload a clip or paste a TikTok URL to locate the original Twitch VOD and exact timestamp.</p>
      </section>

      <section className="search-panel">
        <h2>Search Clip</h2>
        <p className="hint">Use one input at a time. Adding one source disables the other.</p>
        <form onSubmit={onSubmit} className="search-form">
          <div className="field">
            <label htmlFor="clip-file">Upload clip</label>
            <input
              id="clip-file"
              type="file"
              accept="audio/*,video/*"
              disabled={submitting || hasUrl}
              onChange={(e) => {
                const next = e.target.files?.[0] ?? null;
                setFile(next);
                if (next) {
                  setTiktokUrl("");
                }
              }}
            />
          </div>

          <div className="field">
            <label htmlFor="tiktok-url">TikTok URL</label>
            <input
              id="tiktok-url"
              type="url"
              placeholder="https://www.tiktok.com/@user/video/..."
              value={tiktokUrl}
              disabled={submitting || !!file}
              onChange={(e) => {
                const next = e.target.value;
                setTiktokUrl(next);
                if (next.trim().length > 0 && file) {
                  setFile(null);
                }
              }}
            />
          </div>

          <button type="submit" disabled={submitting || (!file && !hasUrl)}>
            {submitting ? "Searching..." : "Search"}
          </button>
        </form>
      </section>

      <section className="result-panel" aria-live="polite">
        <h2>Result</h2>
        {!result && !error && <p className="hint">Run a search to see match details here.</p>}

        {error && (
          <div className="alert-error" role="alert">
            {error}
          </div>
        )}

        {result && (
          <div className="result-box fade-in">
            <div className="result-header">
              <span className={`status-pill ${result.found ? "success" : "muted"}`}>
                {result.found ? "Match Found" : "No Match"}
              </span>
              <span className="score-text">
                Score: {result.score ?? "-"}
              </span>
            </div>

            <div className="meta-grid">
              <div className="meta-item">
                <span className="meta-label">Streamer</span>
                <span className="meta-value">{result.streamer ?? "-"}</span>
              </div>
              <div className="meta-item">
                <span className="meta-label">Title</span>
                <span className="meta-value">{result.title ?? "-"}</span>
              </div>
              <div className="meta-item">
                <span className="meta-label">Timestamp (seconds)</span>
                <span className="meta-value">{result.timestamp_seconds ?? "-"}</span>
              </div>
              <div className="meta-item">
                <span className="meta-label">Reason</span>
                <span className="meta-value">{result.reason ?? "-"}</span>
              </div>
            </div>

            <div className="link-group">
              <a
                className={`result-link ${result.video_url ? "" : "disabled"}`}
                href={result.video_url ?? undefined}
                target="_blank"
                rel="noreferrer"
                aria-disabled={!result.video_url}
                onClick={(e) => {
                  if (!result.video_url) e.preventDefault();
                }}
              >
                Open VOD
              </a>
              <a
                className={`result-link ${result.video_url_at_timestamp ? "" : "disabled"}`}
                href={result.video_url_at_timestamp ?? undefined}
                target="_blank"
                rel="noreferrer"
                aria-disabled={!result.video_url_at_timestamp}
                onClick={(e) => {
                  if (!result.video_url_at_timestamp) e.preventDefault();
                }}
              >
                Open at Timestamp
              </a>
            </div>
          </div>
        )}
      </section>
    </main>
  );
}
