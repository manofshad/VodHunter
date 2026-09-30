import { describe, expect, it } from "vitest";
import { TransportItemType, type TransportItem } from "@grafana/faro-web-sdk";
import { safeAttributes, sanitizeTelemetry } from "./telemetry";

const secret = "https://www.tiktok.com/@someone/video/123?token=private";
function item(type: TransportItemType, payload: unknown): TransportItem {
  return { type, payload: payload as TransportItem["payload"], meta: {
    app: { name: "vodhunter-public" }, page: { url: `${location.origin}/share?search_id=42` },
    user: { email: "private@example.com" }, session: { id: "anonymous-session", attributes: { token: secret } },
    browser: { name: "Firefox", mobile: true, userAgent: "full user agent" },
  } };
}

describe("telemetry privacy boundary", () => {
  it("allowlists attributes and constrains values", () => {
    expect(safeAttributes({ attempt_id: "abc-123", search_id: 42, outcome: "match", tiktok_url: secret, clipboard: secret, stage: secret, duration_ms: Infinity }))
      .toEqual({ schema_version: "1", attempt_id: "abc-123", search_id: "42", outcome: "match", stage: "unknown" });
  });
  it("removes URL parameters, user metadata and session attributes", () => {
    const sanitized = sanitizeTelemetry(item(TransportItemType.EVENT, { name: "search_job_accepted", timestamp: "now", attributes: { search_id: 42, input: secret } }));
    const text = JSON.stringify(sanitized);
    expect(text).not.toContain(secret);
    expect(text).not.toContain("private@example.com");
    expect(text).not.toContain("search_id=42");
    expect(text).not.toContain("full user agent");
    expect(sanitized?.meta.session?.id).toBe("anonymous-session");
  });
  it("preserves SDK session lifecycle and actual lowercase web-vital fields", () => {
    expect(sanitizeTelemetry(item(TransportItemType.EVENT, { name: "session_start", timestamp: "now" }))).not.toBeNull();
    const vital = sanitizeTelemetry(item(TransportItemType.MEASUREMENT, { type: "web-vitals", timestamp: "now", values: { lcp: 2500, cls: 0.1, url: 1 }, context: { rating: "good", lcp_element: secret } }));
    expect(vital?.payload).toEqual({ type: "web-vitals", timestamp: "now", values: { lcp: 2500, cls: 0.1 }, context: { rating: "good" } });
  });
  it("strips arbitrary exception text, external filenames and context", () => {
    const value = sanitizeTelemetry(item(TransportItemType.EXCEPTION, { type: "TypeError", value: secret, timestamp: "now", context: { input: secret }, stacktrace: { frames: [{ filename: secret, function: "search", lineno: 2 }] } }));
    expect(JSON.stringify(value)).not.toContain(secret);
    expect(JSON.stringify(value)).toContain("Unhandled TypeError");
    expect(JSON.stringify(value)).toContain("[external]");
  });
  it("drops arbitrary logs, traces and unknown events", () => {
    expect(sanitizeTelemetry(item(TransportItemType.LOG, { message: secret }))).toBeNull();
    expect(sanitizeTelemetry(item(TransportItemType.TRACE, {}))).toBeNull();
    expect(sanitizeTelemetry(item(TransportItemType.EVENT, { name: "unknown", timestamp: "now" }))).toBeNull();
  });
});
