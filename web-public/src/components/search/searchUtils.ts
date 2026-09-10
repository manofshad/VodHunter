const DIRECT_TIKTOK_HOSTS = new Set(["tiktok.com", "www.tiktok.com", "www.tiktokv.com"]);
const SHORT_TIKTOK_HOSTS = new Set(["tiktok.com", "www.tiktok.com", "vm.tiktok.com", "vt.tiktok.com"]);
const DIRECT_VIDEO_PATHS = [
  /^\/@(?:[A-Za-z0-9_.-]+)?\/video\/[0-9]+\/?$/,
  /^\/share\/video\/[0-9]+\/?$/,
  /^\/embed\/[0-9]+\/?$/,
];

export function formatTimelineTime(value: number): string {
  if (!Number.isFinite(value)) {
    return "Unknown time";
  }

  const totalTenths = Math.max(0, Math.round(value * 10));
  const hours = Math.floor(totalTenths / 36_000);
  const minutes = Math.floor((totalTenths % 36_000) / 600);
  const secondsWithTenths = (totalTenths % 600) / 10;
  const seconds = Number.isInteger(secondsWithTenths)
    ? String(secondsWithTenths).padStart(2, "0")
    : secondsWithTenths.toFixed(1).padStart(4, "0");

  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}:${seconds}`;
}

export function isSupportedTikTokUrl(rawUrl: string): boolean {
  const url = rawUrl.trim();
  if (!url) {
    return false;
  }

  try {
    const parsed = new URL(url);
    const host = parsed.hostname.toLowerCase();
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return false;
    }
    if (parsed.username || parsed.password || parsed.port) {
      return false;
    }

    const isDirectVideo = DIRECT_TIKTOK_HOSTS.has(host) && DIRECT_VIDEO_PATHS.some((pattern) => pattern.test(parsed.pathname));
    const isShortShare =
      (host === "tiktok.com" || host === "www.tiktok.com") && /^\/t\/[A-Za-z0-9_]+\/?$/.test(parsed.pathname);
    const isVmShare =
      (host === "vm.tiktok.com" || host === "vt.tiktok.com") && /^\/[A-Za-z0-9_]+\/?$/.test(parsed.pathname);

    return isDirectVideo || (SHORT_TIKTOK_HOSTS.has(host) && (isShortShare || isVmShare));
  } catch {
    return false;
  }
}

export function getStageMessage(stage: string | null): string {
  switch (stage) {
    case "validating":
      return "Validating your TikTok URL.";
    case "downloading":
      return "Downloading the TikTok clip.";
    case "probing":
      return "Checking clip duration.";
    case "preprocessing":
      return "Preparing the clip audio for search.";
    case "fingerprinting":
      return "Generating neural audio fingerprints.";
    case "retrieving":
      return "Retrieving candidate VOD fingerprints.";
    case "aligning":
      return "Building supported clip-to-VOD timeline segments.";
    case "finalizing":
      return "Finalizing the search result.";
    default:
      return "We are matching your TikTok clip against indexed streamer audio.";
  }
}
