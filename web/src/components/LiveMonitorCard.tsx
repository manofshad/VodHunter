import { FormEvent, useState } from "react";

import { startLiveMonitor, stopLiveMonitor } from "../api/client";
import { LiveStatusResponse } from "../api/types";

interface Props {
  status: LiveStatusResponse;
  onRefresh: () => Promise<void> | void;
}

export default function LiveMonitorCard({ status, onRefresh }: Props) {
  const [streamer, setStreamer] = useState<string>(status.streamer ?? "");
  const [message, setMessage] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState<boolean>(false);

  const onStart = async (e: FormEvent) => {
    e.preventDefault();
    const trimmed = streamer.trim();
    if (!trimmed) return;

    try {
      setSubmitting(true);
      setMessage(null);
      await startLiveMonitor(trimmed);
      await onRefresh();
      setMessage(`Monitoring started for ${trimmed}`);
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Failed to start monitor");
    } finally {
      setSubmitting(false);
    }
  };

  const onStop = async () => {
    try {
      setSubmitting(true);
      setMessage(null);
      await stopLiveMonitor();
      await onRefresh();
      setMessage("Monitor stopped");
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Failed to stop monitor");
    } finally {
      setSubmitting(false);
    }
  };

  const running = status.state !== "idle";

  return (
    <section className="card">
      <h2>Live Monitor</h2>
      <form onSubmit={onStart} className="row">
        <input
          type="text"
          placeholder="streamer name"
          value={streamer}
          onChange={(e) => setStreamer(e.target.value)}
          disabled={submitting || running}
        />
        <button type="submit" disabled={submitting || running || streamer.trim().length === 0}>
          Start
        </button>
        <button type="button" onClick={onStop} disabled={submitting || !running}>
          Stop
        </button>
      </form>

      <div className="status-grid">
        <div><strong>State:</strong> {status.state}</div>
        <div><strong>Streamer:</strong> {status.streamer ?? "-"}</div>
        <div><strong>Live:</strong> {status.is_live === null ? "-" : status.is_live ? "yes" : "no"}</div>
        <div><strong>Last Check:</strong> {status.last_check_at ?? "-"}</div>
        <div><strong>Current Video ID:</strong> {status.current_video_id ?? "-"}</div>
      </div>

      {(status.last_error || message) && <p className="message">{status.last_error ?? message}</p>}
    </section>
  );
}
