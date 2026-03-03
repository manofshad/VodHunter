import {
  LiveSessionItem,
  LiveStartResponse,
  LiveStatusResponse,
  LiveStopResponse,
  SearchResponse,
} from "./types";

const ENV_API_BASE = import.meta.env.VITE_API_BASE?.trim();
const DEV_API_BASE = `http://${window.location.hostname}:8001/api`;
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

export async function getLiveStatus(): Promise<LiveStatusResponse> {
  const resp = await fetch(`${getApiBase()}/live/status`);
  return parseJson<LiveStatusResponse>(resp);
}

export async function startLiveMonitor(streamer: string): Promise<LiveStartResponse> {
  const resp = await fetch(`${getApiBase()}/live/start`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ streamer }),
  });
  return parseJson<LiveStartResponse>(resp);
}

export async function stopLiveMonitor(): Promise<LiveStopResponse> {
  const resp = await fetch(`${getApiBase()}/live/stop`, { method: "POST" });
  return parseJson<LiveStopResponse>(resp);
}

export async function getLiveSessions(limit = 50, offset = 0): Promise<LiveSessionItem[]> {
  const resp = await fetch(`${getApiBase()}/live/sessions?limit=${limit}&offset=${offset}`);
  return parseJson<LiveSessionItem[]>(resp);
}

export type SearchClipInput =
  | { type: "file"; file: File }
  | { type: "tiktok_url"; tiktokUrl: string };

export async function searchClip(input: SearchClipInput): Promise<SearchResponse> {
  const form = new FormData();
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
