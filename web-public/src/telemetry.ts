import {
  ErrorsInstrumentation, FetchTransport, initializeFaro, SessionInstrumentation,
  TransportItemType, WebVitalsInstrumentation,
  type Faro, type TransportItem,
} from "@grafana/faro-web-sdk";

const EVENT_NAMES = new Set([
  "search_page_ready", "streamer_list_failed", "search_attempted", "search_validation_blocked",
  "search_job_accepted", "search_submit_failed", "search_stage_changed", "search_result_visible",
  "search_poll_failed", "search_resumed", "search_view_hidden", "search_view_returned",
  "result_vod_clicked", "history_opened", "date_filter_used", "clipboard_failed",
]);
const ENUM_FIELDS: Record<string, Set<string>> = {
  entry_kind: new Set(["new", "resumed", "shared"]),
  outcome: new Set(["match", "no_match", "error"]),
  reason: new Set(["unsupported_url", "missing_streamer", "empty_input", "invalid_date_range", "clipboard_denied", "network", "http_4xx", "http_5xx", "poll_failed", "unknown"]),
  stage: new Set(["queued", "validating", "downloading", "preprocessing", "probing", "fingerprinting", "retrieving", "embedding", "searching", "aligning", "finalizing", "unknown"]),
  link_kind: new Set(["source", "segment"]),
  error_code: new Set(["DOWNLOAD_ERROR", "PROCESSING_ERROR", "INVALID_SEARCH_INPUT", "INVALID_TIKTOK_URL", "INVALID_STREAMER", "INVALID_DATE_RANGE", "INPUT_DURATION_EXCEEDED", "WORKER_RESTARTED", "SEARCH_NOT_FOUND", "RATE_LIMITED", "unknown"]),
};

export function safeAttributes(attributes: Record<string, unknown>): Record<string, string> {
  const safe: Record<string, string> = { schema_version: "1" };
  for (const [key, value] of Object.entries(attributes)) {
    const text = String(value);
    if (ENUM_FIELDS[key]) safe[key] = ENUM_FIELDS[key].has(text) ? text : "unknown";
    else if (key === "attempt_id" && /^[a-zA-Z0-9_-]{1,64}$/.test(text)) safe[key] = text;
    else if (key === "search_id" && /^\d{1,16}$/.test(text)) safe[key] = text;
    else if (key === "duration_ms" && Number.isFinite(Number(value)) && Number(value) >= 0) {
      safe[key] = String(Math.min(Math.round(Number(value)), 86400000));
    }
    else if (key === "has_date_filter" && typeof value === "boolean") safe[key] = String(value);
  }
  return safe;
}

function safeAssetUrl(raw: string): string {
  try {
    const url = new URL(raw, window.location.origin);
    return url.origin === window.location.origin && /^\/assets\/[a-zA-Z0-9_.-]+\.js$/.test(url.pathname)
      ? `${url.origin}${url.pathname}` : "[external]";
  } catch { return "[external]"; }
}

/** Rebuild payloads from allowed fields before they enter the SDK buffer. */
export function sanitizeTelemetry(item: TransportItem): TransportItem | null {
  const meta: TransportItem["meta"] = {
    app: item.meta.app,
    sdk: item.meta.sdk,
    page: { url: `${window.location.origin}${window.location.pathname === "/share" ? "/share" : "/"}` },
    session: { id: item.meta.session?.id, overrides: { geoLocationTrackingEnabled: false } },
    browser: item.meta.browser && {
      name: item.meta.browser.name, version: item.meta.browser.version,
      os: item.meta.browser.os, mobile: item.meta.browser.mobile,
    },
  };
  if (item.type === TransportItemType.EVENT) {
    const payload = item.payload as { name: string; timestamp: string; attributes?: Record<string, unknown> };
    if (!EVENT_NAMES.has(payload.name) && !/^session_(start|resume|extend)$/.test(payload.name)) return null;
    return { ...item, meta, payload: {
      name: payload.name, timestamp: payload.timestamp, domain: "vodhunter",
      attributes: EVENT_NAMES.has(payload.name) ? safeAttributes(payload.attributes ?? {}) : {},
    } };
  }
  if (item.type === TransportItemType.MEASUREMENT) {
    const payload = item.payload as { type: string; timestamp: string; values: Record<string, number>; context?: Record<string, string> };
    if (payload.type !== "web-vitals") return null;
    const values = Object.fromEntries(Object.entries(payload.values).filter(([key, value]) =>
      /^(lcp|inp|cls|fcp|ttfb|fid|delta)$/.test(key) && Number.isFinite(value),
    ));
    const context: Record<string, string> = {};
    if (["good", "needs-improvement", "poor"].includes(payload.context?.rating ?? "")) context.rating = payload.context!.rating;
    if (["navigate", "reload", "back-forward", "back-forward-cache", "prerender", "restore"].includes(payload.context?.navigation_type ?? "")) context.navigation_type = payload.context!.navigation_type;
    return { ...item, meta, payload: { type: payload.type, timestamp: payload.timestamp, values, context } };
  }
  if (item.type === TransportItemType.EXCEPTION) {
    const payload = item.payload as { type: string; timestamp: string; stacktrace?: { frames: { filename: string; function: string; lineno?: number; colno?: number }[] } };
    const errorType = /^(Error|TypeError|RangeError|ReferenceError|SyntaxError|URIError|EvalError|UnhandledRejection)$/.test(payload.type)
      ? payload.type : "Error";
    return { ...item, meta, payload: {
      type: errorType, value: `Unhandled ${errorType}`, timestamp: payload.timestamp,
      stacktrace: { frames: (payload.stacktrace?.frames ?? []).slice(0, 20).map((frame) => ({
        filename: safeAssetUrl(frame.filename), function: /^[a-zA-Z0-9_.$<> ]{1,100}$/.test(frame.function) ? frame.function : "unknown",
        lineno: frame.lineno, colno: frame.colno,
      })) },
    } };
  }
  // Never send arbitrary console logs, network URLs, resource timings or traces.
  return null;
}

let telemetry: Faro | undefined;
export function initializeTelemetry(): void {
  const url = import.meta.env.VITE_FARO_URL?.trim();
  const privacy = navigator as Navigator & { globalPrivacyControl?: boolean };
  if (!url || url === "disabled" || telemetry || import.meta.env.DEV || privacy.globalPrivacyControl || navigator.doNotTrack === "1") return;
  try {
    telemetry = initializeFaro({
      app: { name: "vodhunter-public", version: import.meta.env.VITE_RELEASE || "unversioned", environment: "production" },
      transports: [new FetchTransport({ url, bufferSize: 100, concurrency: 1, requestTimeoutMs: 5000, retry: { maxAttempts: 2 } })],
      instrumentations: [new ErrorsInstrumentation(), new WebVitalsInstrumentation(), new SessionInstrumentation()],
      sessionTracking: { enabled: true, persistent: false, samplingRate: 1 },
      batching: { enabled: true, itemLimit: 30, sendTimeout: 3000 },
      beforeSend: sanitizeTelemetry,
      preventGlobalExposure: true,
    });
  } catch { /* Monitoring must never prevent application startup. */ }
}

export function trackEvent(name: string, attributes: Record<string, unknown> = {}): void {
  if (!EVENT_NAMES.has(name)) return;
  try { telemetry?.api.pushEvent(name, safeAttributes(attributes), "vodhunter", { skipDedupe: true }); }
  catch { /* Search behavior does not depend on telemetry availability. */ }
}

export interface SearchJourney { attempt_id: string; entry_kind: "new" | "resumed" | "shared"; started: number; search_id?: number }
export function newJourney(entry_kind: SearchJourney["entry_kind"], search_id?: number): SearchJourney {
  return { attempt_id: `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 12)}`, entry_kind, started: performance.now(), search_id };
}
export function journeyAttributes(journey: SearchJourney): Record<string, unknown> {
  return { attempt_id: journey.attempt_id, entry_kind: journey.entry_kind, search_id: journey.search_id,
    duration_ms: Math.max(0, performance.now() - journey.started) };
}

export function requestFailureDetails(error: unknown): Record<string, unknown> {
  const failure = error as { status?: number; code?: string } | null;
  return { reason: failure?.status ? (failure.status >= 500 ? "http_5xx" : "http_4xx") : (error instanceof TypeError ? "network" : "unknown"), error_code: failure?.code ?? "unknown" };
}
