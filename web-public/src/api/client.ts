import { SearchResponse, StreamerListItem } from "./types";

const ENV_API_BASE = import.meta.env.VITE_API_BASE?.trim();
const DEV_API_BASE = `http://${window.location.hostname}:8000/api`;
const API_BASE = ENV_API_BASE || (import.meta.env.DEV ? DEV_API_BASE : "");

function getApiBase(): string {
  if (!API_BASE) {
    throw new Error("VITE_API_BASE is required in production");
  }
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

export type SearchClipInput =
  | { type: "file"; file: File; streamer: string }
  | { type: "tiktok_url"; tiktokUrl: string; streamer: string };

export async function listSearchableStreamers(): Promise<StreamerListItem[]> {
  const resp = await fetch(`${getApiBase()}/search/streamers`);
  return parseJson<StreamerListItem[]>(resp);
}

export async function searchClip(input: SearchClipInput): Promise<SearchResponse> {
  const form = new FormData();
  form.append("streamer", input.streamer);
  if (input.type === "file") {
    form.append("file", input.file);
  } else {
    form.append("tiktok_url", input.tiktokUrl);
  }

  const resp = await fetch(`${getApiBase()}/search/clip`, {
    method: "POST",
    body: form,
  });
  return parseJson<SearchResponse>(resp);
}
