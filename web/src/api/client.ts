import {
  LiveSessionItem,
  LiveStartResponse,
  LiveStatusResponse,
  LiveStopResponse,
  SearchResponse,
} from "./types";

const API_BASE =
  import.meta.env.VITE_API_BASE ||
  `http://${window.location.hostname}:8000/api`;

async function parseJson<T>(resp: Response): Promise<T> {
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const message = data?.detail?.message || `Request failed (${resp.status})`;
    throw new Error(message);
  }
  return data as T;
}

export async function getLiveStatus(): Promise<LiveStatusResponse> {
  const resp = await fetch(`${API_BASE}/live/status`);
  return parseJson<LiveStatusResponse>(resp);
}

export async function startLiveMonitor(streamer: string): Promise<LiveStartResponse> {
  const resp = await fetch(`${API_BASE}/live/start`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ streamer }),
  });
  return parseJson<LiveStartResponse>(resp);
}

export async function stopLiveMonitor(): Promise<LiveStopResponse> {
  const resp = await fetch(`${API_BASE}/live/stop`, { method: "POST" });
  return parseJson<LiveStopResponse>(resp);
}

export async function getLiveSessions(limit = 50, offset = 0): Promise<LiveSessionItem[]> {
  const resp = await fetch(`${API_BASE}/live/sessions?limit=${limit}&offset=${offset}`);
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

  const resp = await fetch(`${API_BASE}/search/clip`, {
    method: "POST",
    body: form,
  });
  return parseJson<SearchResponse>(resp);
}
