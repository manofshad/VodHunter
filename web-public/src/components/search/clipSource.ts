import { SearchJobResponse } from "../../api/types";

export type ClipDraft =
  | { kind: "link"; url: string }
  | { kind: "upload-empty" }
  | { kind: "upload-selected"; file: File; durationSeconds?: number }
  | { kind: "upload-restored"; filename: string };

export type ClipSourceMetadata =
  | { kind: "link"; url: string }
  | { kind: "upload"; filename: string };

export const VIDEO_FILE_ACCEPT = ".mp4,.mov,.m4v,.webm,.mkv";
const MAX_UPLOAD_BYTES = 100 * 1024 * 1024;
const MAX_UPLOAD_DURATION_SECONDS = 180;

export function validateClipFile(file: File, durationSeconds?: number): string | null {
  if (!/\.(mp4|mov|m4v|webm|mkv)$/i.test(file.name)) {
    return "Choose an MP4, MOV, M4V, WebM, or MKV video file.";
  }
  if (file.size === 0) {
    return "This file is empty. Choose a video file.";
  }
  if (file.size > MAX_UPLOAD_BYTES) {
    return "Choose a video file of 100 MB or smaller.";
  }
  if (durationSeconds !== undefined && durationSeconds > MAX_UPLOAD_DURATION_SECONDS) {
    return "Choose a video no longer than 3 minutes.";
  }
  return null;
}

export function parseClipSource(value: unknown): ClipSourceMetadata | null {
  if (!value || typeof value !== "object" || !("kind" in value)) {
    return null;
  }
  if (value.kind === "link" && "url" in value && typeof value.url === "string" && value.url.trim()) {
    return { kind: "link", url: value.url };
  }
  if (value.kind === "upload" && "filename" in value && typeof value.filename === "string" && value.filename.trim()) {
    return { kind: "upload", filename: value.filename };
  }
  return null;
}

export function sourceFromJob(job: SearchJobResponse): ClipSourceMetadata | null {
  switch (job.input_type) {
    case "upload":
      return job.clip_filename ? { kind: "upload", filename: job.clip_filename } : null;
    case "tiktok_url":
      return job.tiktok_url ? { kind: "link", url: job.tiktok_url } : null;
    default: {
      const exhaustive: never = job.input_type;
      return exhaustive;
    }
  }
}

export function restoredDraft(source: ClipSourceMetadata): ClipDraft {
  return source.kind === "link" ? source : { kind: "upload-restored", filename: source.filename };
}
