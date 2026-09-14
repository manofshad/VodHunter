import { describe, expect, it } from "vitest";

import { parseSharedSearchLocation, sharedSearchPath } from "./sharedSearch";

describe("parseSharedSearchLocation", () => {
  it("reads a positive integer job id from the share page", () => {
    expect(parseSharedSearchLocation({ pathname: "/share", search: "?search_id=123" })).toEqual({
      kind: "search",
      searchId: 123,
    });
  });

  it.each([
    "",
    "?search_id=",
    "?search_id=0",
    "?search_id=-1",
    "?search_id=1.5",
    "?search_id=abc",
    "?search_id=1&search_id=2",
    "?search_id=9007199254740992",
  ])("rejects an invalid share query %s", (search) => {
    expect(parseSharedSearchLocation({ pathname: "/share", search })).toEqual({ kind: "invalid" });
  });

  it("ignores search_id outside the share page", () => {
    expect(parseSharedSearchLocation({ pathname: "/", search: "?search_id=123" })).toEqual({ kind: "none" });
  });
});

describe("sharedSearchPath", () => {
  it("builds the canonical frontend job URL", () => {
    expect(sharedSearchPath(123)).toBe("/share?search_id=123");
  });
});
