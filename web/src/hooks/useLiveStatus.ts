import { useCallback, useEffect, useState } from "react";

import { getLiveStatus } from "../api/client";
import { LiveStatusResponse } from "../api/types";

const defaultStatus: LiveStatusResponse = {
  state: "idle",
  streamer: null,
  is_live: null,
  started_at: null,
  last_check_at: null,
  last_error: null,
  current_video_id: null,
};

export function useLiveStatus(pollMs: number = 2500) {
  const [status, setStatus] = useState<LiveStatusResponse>(defaultStatus);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const next = await getLiveStatus();
      setStatus(next);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load live status");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
    const id = window.setInterval(refresh, pollMs);
    return () => window.clearInterval(id);
  }, [pollMs, refresh]);

  return { status, loading, error, refresh };
}
