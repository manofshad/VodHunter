import { describe, expect, it } from "vitest";

import { parseSharedSearchLocation, sharedSearchPath } from "./sharedSearch";

const searchToken = "Ab_-" + "x".repeat(39);

describe("parseSharedSearchLocation", () => {
  it("reads a capability from the fragment", () => {
    expect(parseSharedSearchLocation({ pathname: "/share", search: "", hash: `#${searchToken}` })).toEqual({
      kind: "search",
      searchToken,
    });
  });

  it.each(["", "#123", "#" + "x".repeat(42), "#" + "x".repeat(44), "#" + "!".repeat(43),
           `#${searchToken}&extra`, `#${searchToken}%20`, `#${searchToken}\n`])("rejects an invalid share fragment %s", (hash) => {
    expect(parseSharedSearchLocation({ pathname: "/share", search: "", hash })).toEqual({ kind: "invalid" });
  });

  it.each(["?search_id=123", `?search_token=${searchToken}`])("rejects query-based access %s", (search) => {
    expect(parseSharedSearchLocation({ pathname: "/share", search, hash: `#${searchToken}` })).toEqual({ kind: "invalid" });
  });

  it("ignores tokens outside the share page", () => {
    expect(parseSharedSearchLocation({ pathname: "/", search: "", hash: `#${searchToken}` })).toEqual({ kind: "none" });
  });
});

describe("sharedSearchPath", () => {
  it("keeps the capability out of the request URL and query string", () => {
    expect(sharedSearchPath(searchToken)).toBe(`/share#${searchToken}`);
  });
});
