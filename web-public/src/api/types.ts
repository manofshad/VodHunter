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
}

export interface StreamerListItem {
  name: string;
  profile_image_url: string | null;
}
