export type SharedSearchLocation =
  | { kind: "none" }
  | { kind: "invalid" }
  | { kind: "search"; searchToken: string };

export function isSearchToken(value: unknown): value is string {
  return typeof value === "string" && value.length === 43 && /^[A-Za-z0-9_-]{43}$/.test(value);
}

export function parseSharedSearchLocation(
  location: Pick<Location, "pathname" | "search" | "hash">,
): SharedSearchLocation {
  if (location.pathname !== "/share" && location.pathname !== "/share/") {
    return { kind: "none" };
  }

  // A fragment never reaches web servers or travels in a Referer header.
  // Reject numbered links rather than upgrading them into access capabilities.
  const searchToken = location.hash.slice(1);
  if (location.search || !isSearchToken(searchToken)) {
    return { kind: "invalid" };
  }

  return { kind: "search", searchToken };
}

export function sharedSearchPath(searchToken: string): string {
  return `/share#${searchToken}`;
}
