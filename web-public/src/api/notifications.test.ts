import { describe, expect, it } from "vitest";

import { urlBase64ToArrayBuffer } from "./notifications";


describe("urlBase64ToArrayBuffer", () => {
  it("decodes URL-safe VAPID public keys", () => {
    const value = new Uint8Array(urlBase64ToArrayBuffer("AQID-v8"));
    expect([...value]).toEqual([1, 2, 3, 250, 255]);
  });
});
