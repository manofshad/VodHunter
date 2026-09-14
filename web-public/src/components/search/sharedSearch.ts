export type SharedSearchLocation =
  | { kind: "none" }
  | { kind: "invalid" }
  | { kind: "search"; searchId: number };

export function parseSharedSearchLocation(
  location: Pick<Location, "pathname" | "search">,
): SharedSearchLocation {
  if (location.pathname !== "/share" && location.pathname !== "/share/") {
    return { kind: "none" };
  }

  const values = new URLSearchParams(location.search).getAll("search_id");
  if (values.length !== 1 || !/^[1-9]\d*$/.test(values[0])) {
    return { kind: "invalid" };
  }

  const searchId = Number(values[0]);
  if (!Number.isSafeInteger(searchId)) {
    return { kind: "invalid" };
  }

  return { kind: "search", searchId };
}

export function sharedSearchPath(searchId: number): string {
  return `/share?search_id=${searchId}`;
}
