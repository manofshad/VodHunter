export interface SearchSegment {
  query_start: number;
  query_end: number;
  video_id: number;
  vod_start: number;
  vod_end: number;
  video_url_at_timestamp: string | null;
  score: number;
}

export interface UnmatchedRange {
  query_start: number;
  query_end: number;
}

export interface SearchSource {
  video_id: number;
  video_url: string | null;
  video_url_at_timestamp: string | null;
  thumbnail_url: string | null;
  title: string | null;
  streamer: string | null;
  profile_image_url: string | null;
  segments: SearchSegment[];
}

export interface SearchResponse {
  found: boolean;
  streamer: string | null;
  profile_image_url: string | null;
  video_id: number | null;
  video_url: string | null;
  video_url_at_timestamp: string | null;
  thumbnail_url: string | null;
  title: string | null;
  timestamp_seconds: number | null;
  score: number | null;
  reason: string | null;
  sources: SearchSource[];
  segments: SearchSegment[];
  unmatched_ranges: UnmatchedRange[];
  query_duration_seconds: number | null;
}

export interface SearchJobCreatedResponse {
  search_token: string;
  status: "queued" | "running" | "completed" | "failed";
  stage: string | null;
}

export interface SearchJobError {
  code: string;
  message: string;
}

export interface SearchJobResponse {
  search_token: string;
  status: "queued" | "running" | "completed" | "failed";
  stage: string | null;
  input_type: "tiktok_url" | "upload";
  clip_filename: string | null;
  tiktok_url: string | null;
  streamer: string | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  result: SearchResponse | null;
  error: SearchJobError | null;
  feedback: SearchFeedbackResponse | null;
}

export type MatchRatingValue = "up" | "down";

export interface SearchFeedbackResponse {
  rating: MatchRatingValue;
  updated_at: string;
}

export interface CompletedSearchFeedback {
  searchToken: string;
  feedback: SearchFeedbackResponse | null;
}

export interface StreamerListItem {
  name: string;
  profile_image_url: string | null;
}
