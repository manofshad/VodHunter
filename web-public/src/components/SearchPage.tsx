import { FormEvent, useEffect, useMemo, useRef, useState } from "react";

import { listSearchableStreamers, searchClip } from "../api/client";
import { SearchResponse, StreamerListItem } from "../api/types";

function formatDuration(value: number | null): string | null {
  if (value === null || !Number.isFinite(value)) {
    return null;
  }

  const totalSeconds = Math.max(0, Math.floor(value));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  return [hours, minutes, seconds].map((part) => String(part).padStart(2, "0")).join(":");
}

function getResultHref(result: SearchResponse | null): string | null {
  if (!result?.found) {
    return null;
  }

  return result.video_url_at_timestamp ?? null;
}

export default function SearchPage() {
  const [tiktokUrl, setTiktokUrl] = useState("");
  const [streamer, setStreamer] = useState("");
  const [streamers, setStreamers] = useState<StreamerListItem[]>([]);
  const [loadingStreamers, setLoadingStreamers] = useState(true);
  const [streamerLoadError, setStreamerLoadError] = useState<string | null>(null);
  const [result, setResult] = useState<SearchResponse | null>(null);
  const [requestError, setRequestError] = useState<string | null>(null);
  const [streamerError, setStreamerError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [lastSubmittedUrl, setLastSubmittedUrl] = useState("");
  const streamerSelectRef = useRef<HTMLSelectElement | null>(null);

  const hasUrl = tiktokUrl.trim().length > 0;
  const resultHref = useMemo(() => getResultHref(result), [result]);
  const formattedTimestamp = useMemo(() => formatDuration(result?.timestamp_seconds ?? null), [result]);

  useEffect(() => {
    let cancelled = false;

    const loadStreamers = async () => {
      try {
        setLoadingStreamers(true);
        setStreamerLoadError(null);
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
        setStreamerLoadError(err instanceof Error ? err.message : "Could not load streamers");
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
    if (!hasUrl) return;

    if (!streamer.trim()) {
      setStreamerError("Select a streamer to run the search.");
      streamerSelectRef.current?.focus();
      return;
    }

    const submittedUrl = tiktokUrl.trim();

    try {
      setSubmitting(true);
      setStreamerError(null);
      setRequestError(null);
      setResult(null);
      setLastSubmittedUrl(submittedUrl);

      const next = await searchClip({ type: "tiktok_url", tiktokUrl: submittedUrl, streamer });
      setResult(next);
    } catch (err) {
      setRequestError(err instanceof Error ? err.message : "Search failed");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <main className="page-shell">
      <section className="search-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Public Search</p>
            <h1>Search Twitch VODs</h1>
          </div>
        </div>

        <form onSubmit={onSubmit} className="search-form" noValidate>
          <div className="search-grid">
            <div className={`field ${streamerError ? "field-error" : ""}`}>
              <label htmlFor="streamer-select">Streamer</label>
              <select
                ref={streamerSelectRef}
                id="streamer-select"
                value={streamer}
                disabled={submitting || loadingStreamers || streamers.length === 0}
                aria-invalid={streamerError ? "true" : "false"}
                aria-describedby={streamerError ? "streamer-error" : undefined}
                onChange={(e) => {
                  setStreamer(e.target.value);
                  setStreamerError(null);
                }}
              >
                <option value="">
                  {loadingStreamers
                    ? "Loading streamers..."
                    : streamers.length === 0
                      ? "No searchable streamers"
                      : "Select a streamer"}
                </option>
                {streamers.map((item) => (
                  <option key={item.name} value={item.name}>
                    {item.name}
                  </option>
                ))}
              </select>
              {streamerError ? (
                <p className="field-message error-text" id="streamer-error">
                  {streamerError}
                </p>
              ) : null}
            </div>

            <div className="field field-primary">
              <label htmlFor="tiktok-url">TikTok URL</label>
              <input
                id="tiktok-url"
                type="url"
                placeholder="https://www.tiktok.com/@user/video/..."
                value={tiktokUrl}
                disabled={submitting}
                onChange={(e) => {
                  setTiktokUrl(e.target.value);
                  setStreamerError(null);
                }}
              />
            </div>
          </div>

          <div className="search-actions">
            <button type="submit" disabled={submitting || !hasUrl}>
              {submitting ? "Searching..." : "Search"}
            </button>
          </div>
        </form>
      </section>

      <section className="result-panel" aria-live="polite">
        {requestError && (
          <div className="alert-error" role="alert">
            <strong>Search error</strong>
            <span>{requestError}</span>
          </div>
        )}

        {!submitting && result && (
          <div className={`result-card ${result.found ? "result-card-found" : "result-card-empty"} fade-in`}>
            <div className="result-status-row">
              <span className={`status-pill ${result.found ? "success" : "muted"}`}>
                {result.found ? "Match found" : "No match found"}
              </span>
              {result.score !== null && result.found ? <span className="score-text">Score {result.score}</span> : null}
            </div>

            <div className="result-card-top">
              {resultHref ? (
                <a
                  className="thumbnail-placeholder thumbnail-link"
                  href={resultHref}
                  target="_blank"
                  rel="noreferrer"
                  aria-label={`Open ${result.title ?? "matched VOD"} at timestamp`}
                >
                  <span>VOD</span>
                </a>
              ) : (
                <div className="thumbnail-placeholder" aria-hidden="true">
                  <span>VOD</span>
                </div>
              )}

              <div className="result-copy">
                <p className="result-streamer">{result.streamer ?? "Streamer unavailable"}</p>
                {resultHref ? (
                  <a className="result-title-link" href={resultHref} target="_blank" rel="noreferrer">
                    <h3>{result.title ?? "Matched Twitch VOD"}</h3>
                  </a>
                ) : (
                  <h3>{result.title ?? "No matching Twitch VOD found"}</h3>
                )}
              </div>
            </div>

            <div className="result-meta">
              {formattedTimestamp ? (
                <div className="result-meta-row">
                  <span className="meta-label">Timestamp</span>
                  <span className="meta-value">{formattedTimestamp}</span>
                </div>
              ) : null}
              {result.reason ? (
                <div className="result-meta-row">
                  <span className="meta-label">Match reason</span>
                  <span className="meta-value">{result.reason}</span>
                </div>
              ) : null}
              {lastSubmittedUrl ? (
                <div className="result-meta-row">
                  <span className="meta-label">TikTok URL</span>
                  <span className="meta-value">{lastSubmittedUrl}</span>
                </div>
              ) : null}
            </div>
          </div>
        )}
      </section>
    </main>
  );
}
