import { MatchRatingValue, SearchFeedbackResponse, SearchJobCreatedResponse, SearchJobResponse, StreamerListItem } from "./types";

const ENV_API_BASE = import.meta.env.VITE_API_BASE?.trim();
const DEV_API_BASE = `http://${window.location.hostname}:8001/api`;
const PROD_API_BASE = "/api";
const API_BASE = ENV_API_BASE || (import.meta.env.DEV ? DEV_API_BASE : PROD_API_BASE);

function getApiBase(): string {
  return API_BASE;
}

async function parseJson<T>(resp: Response): Promise<T> {
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const message = data?.detail?.message || `Request failed (${resp.status})`;
    throw new Error(message);
  }
  return data as T;
}

export type SearchClipInput = {
  streamer: string;
  streamedFrom?: string;
  streamedTo?: string;
} & (
  | { kind: "link"; url: string }
  | { kind: "upload"; file: File }
);

export async function listSearchableStreamers(): Promise<StreamerListItem[]> {
  const resp = await fetch(`${getApiBase()}/search/streamers`);
  return parseJson<StreamerListItem[]>(resp);
}

export async function createSearchJob(input: SearchClipInput): Promise<SearchJobCreatedResponse> {
  const form = new FormData();
  form.append("streamer", input.streamer);
  if (input.kind === "link") {
    form.append("tiktok_url", input.url);
  } else {
    form.append("clip_file", input.file);
  }
  if (input.streamedFrom) {
    form.append("streamed_from", input.streamedFrom);
  }
  if (input.streamedTo) {
    form.append("streamed_to", input.streamedTo);
  }

  const resp = await fetch(`${getApiBase()}/search/clip`, {
    method: "POST",
    body: form,
  });
  return parseJson<SearchJobCreatedResponse>(resp);
}

export async function getSearchJob(searchToken: string): Promise<SearchJobResponse> {
  const resp = await fetch(`${getApiBase()}/search/clip`, {
    headers: { Authorization: `Bearer ${searchToken}` },
    cache: "no-store",
  });
  return parseJson<SearchJobResponse>(resp);
}

export async function saveSearchFeedback(searchToken: string, rating: MatchRatingValue): Promise<SearchFeedbackResponse> {
  const resp = await fetch(`${getApiBase()}/search/clip/feedback`, {
    method: "PUT",
    headers: { Authorization: `Bearer ${searchToken}`, "Content-Type": "application/json" },
    body: JSON.stringify({ rating }),
    cache: "no-store",
  });
  return parseJson<SearchFeedbackResponse>(resp);
}
