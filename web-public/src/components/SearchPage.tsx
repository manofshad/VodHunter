import { FormEvent, useEffect, useRef, useState } from "react";

import { listSearchableStreamers, searchClip } from "../api/client";
import { SearchResponse, StreamerListItem } from "../api/types";

export default function SearchPage() {
  const MAX_UPLOAD_DURATION_SECONDS = 180;
  const [file, setFile] = useState<File | null>(null);
  const [tiktokUrl, setTiktokUrl] = useState<string>("");
  const [streamer, setStreamer] = useState<string>("");
  const [streamers, setStreamers] = useState<StreamerListItem[]>([]);
  const [loadingStreamers, setLoadingStreamers] = useState<boolean>(true);
  const [result, setResult] = useState<SearchResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState<boolean>(false);
  const [validatingFile, setValidatingFile] = useState<boolean>(false);
  const fileSelectionTokenRef = useRef(0);

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

  const readMediaDurationSeconds = (inputFile: File): Promise<number> =>
    new Promise((resolve, reject) => {
      const media = document.createElement(inputFile.type.startsWith("audio/") ? "audio" : "video");
      const objectUrl = URL.createObjectURL(inputFile);
      const cleanup = () => {
        URL.revokeObjectURL(objectUrl);
        media.removeAttribute("src");
      };

      media.preload = "metadata";
      media.onloadedmetadata = () => {
        const duration = media.duration;
        cleanup();
        if (!Number.isFinite(duration) || duration <= 0) {
          reject(new Error("Could not read file duration"));
          return;
        }
        resolve(duration);
      };
      media.onerror = () => {
        cleanup();
        reject(new Error("Could not read file duration"));
      };
      media.src = objectUrl;
    });

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!hasStreamer || (!file && !hasUrl)) return;
    if (validatingFile) return;

    try {
      setSubmitting(true);
      setError(null);
      setResult(null);

      const next = file
        ? await searchClip({ type: "file", file, streamer })
        : await searchClip({ type: "tiktok_url", tiktokUrl: tiktokUrl.trim(), streamer });
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
        <p className="hint">Choose a streamer first, then use one input at a time. Max duration: 3 minutes.</p>
        <form onSubmit={onSubmit} className="search-form">
          <div className="field">
            <label htmlFor="streamer-select">Streamer</label>
            <select
              id="streamer-select"
              value={streamer}
              disabled={submitting || validatingFile || loadingStreamers || streamers.length === 0}
              onChange={(e) => {
                setStreamer(e.target.value);
                setError(null);
                setResult(null);
              }}
            >
              <option value="" disabled>
                {loadingStreamers ? "Loading streamers..." : streamers.length === 0 ? "No searchable streamers" : "Select a streamer"}
              </option>
              {streamers.map((item) => (
                <option key={item.name} value={item.name}>
                  {item.name}
                </option>
              ))}
            </select>
          </div>

          <div className="field">
            <label htmlFor="clip-file">Upload clip</label>
            <input
              id="clip-file"
              type="file"
              accept="audio/*,video/*"
              disabled={submitting || hasUrl || validatingFile || !hasStreamer}
              onChange={async (e) => {
                const next = e.target.files?.[0] ?? null;
                const nextSelectionToken = fileSelectionTokenRef.current + 1;
                fileSelectionTokenRef.current = nextSelectionToken;
                if (!next) {
                  setFile(null);
                  return;
                }
                setTiktokUrl("");
                setError(null);
                setResult(null);
                setValidatingFile(true);

                try {
                  const duration = await readMediaDurationSeconds(next);
                  if (fileSelectionTokenRef.current != nextSelectionToken) {
                    return;
                  }
                  if (duration > MAX_UPLOAD_DURATION_SECONDS) {
                    setFile(null);
                    setError(`Uploaded file is ${Math.ceil(duration)}s; maximum allowed is ${MAX_UPLOAD_DURATION_SECONDS}s`);
                    return;
                  }
                  setFile(next);
                } catch {
                  if (fileSelectionTokenRef.current != nextSelectionToken) {
                    return;
                  }
                  setFile(null);
                  setError("Could not read uploaded file duration");
                } finally {
                  if (fileSelectionTokenRef.current == nextSelectionToken) {
                    setValidatingFile(false);
                  }
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
              disabled={submitting || !!file || !hasStreamer}
              onChange={(e) => {
                const next = e.target.value;
                setTiktokUrl(next);
                if (next.trim().length > 0 && file) {
                  setFile(null);
                }
              }}
            />
          </div>

          <button type="submit" disabled={submitting || validatingFile || !hasStreamer || (!file && !hasUrl)}>
            {submitting ? "Searching..." : validatingFile ? "Validating file..." : "Search"}
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
