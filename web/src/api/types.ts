export type LiveState = "idle" | "polling" | "ingesting" | "error";

export interface LiveStatusResponse {
  state: LiveState;
  streamer: string | null;
  is_live: boolean | null;
  started_at: string | null;
  last_check_at: string | null;
  last_error: string | null;
  current_video_id: number | null;
  current_vod_url: string | null;
  ingest_cursor_seconds: number | null;
  lag_seconds: number | null;
}

export interface LiveStartResponse {
  status: LiveStatusResponse;
}

export interface LiveStopResponse {
  stopped: boolean;
  status: LiveStatusResponse;
}

export interface LiveSessionItem {
  video_id: number;
  creator_name: string;
  url: string;
  title: string;
  processed: boolean;
}

export interface SearchResponse {
  found: boolean;
  streamer: string | null;
  video_id: number | null;
  video_url: string | null;
  title: string | null;
  timestamp_seconds: number | null;
  score: number | null;
  reason: string | null;
}

export interface ApiError {
  detail?: {
    code?: string;
    message?: string;
  };
}
