import { SearchJobCreatedResponse, SearchJobResponse, StreamerListItem } from "./types";

const ENV_API_BASE = import.meta.env.VITE_API_BASE?.trim();
const DEV_API_BASE = `http://${window.location.hostname}:8000/api`;
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

export interface SearchClipInput {
  streamer: string;
  tiktokUrl: string;
}

export async function listSearchableStreamers(): Promise<StreamerListItem[]> {
  const resp = await fetch(`${getApiBase()}/search/streamers`);
  return parseJson<StreamerListItem[]>(resp);
}

export async function createSearchJob(input: SearchClipInput): Promise<SearchJobCreatedResponse> {
  const form = new FormData();
  form.append("streamer", input.streamer);
  form.append("tiktok_url", input.tiktokUrl);

  const resp = await fetch(`${getApiBase()}/search/clip`, {
    method: "POST",
    body: form,
  });
  return parseJson<SearchJobCreatedResponse>(resp);
}

export async function getSearchJob(searchId: number): Promise<SearchJobResponse> {
  const resp = await fetch(`${getApiBase()}/search/clip/${searchId}`);
  return parseJson<SearchJobResponse>(resp);
}
