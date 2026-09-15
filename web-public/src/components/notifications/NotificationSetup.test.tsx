import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { NOTIFICATION_INSTALLATION_TOKEN_KEY } from "../../api/notifications";
import { NotificationSetup } from "./NotificationSetup";


function jsonResponse(value: unknown, ok = true): Response {
  return { ok, status: ok ? 200 : 400, json: async () => value } as Response;
}

function installWebPushGlobals({ ios = false, standalone = true } = {}) {
  Object.defineProperty(window, "isSecureContext", { configurable: true, value: true });
  Object.defineProperty(navigator, "userAgent", {
    configurable: true,
    value: ios ? "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X)" : "Mozilla/5.0 (Macintosh)",
  });
  Object.defineProperty(window, "matchMedia", {
    configurable: true,
    value: vi.fn().mockReturnValue({ matches: standalone }),
  });
  Object.defineProperty(window, "PushManager", { configurable: true, value: class PushManager {} });
  Object.defineProperty(globalThis, "Notification", {
    configurable: true,
    value: { permission: "default", requestPermission: vi.fn().mockResolvedValue("granted") },
  });
  const subscription = {
    toJSON: () => ({
      endpoint: "https://push.example/device",
      expirationTime: null,
      keys: { p256dh: "public-key", auth: "auth-secret" },
    }),
  };
  const registration = {
    pushManager: {
      getSubscription: vi.fn().mockResolvedValue(null),
      subscribe: vi.fn().mockResolvedValue(subscription),
    },
  };
  Object.defineProperty(navigator, "serviceWorker", {
    configurable: true,
    value: { register: vi.fn().mockResolvedValue(registration) },
  });
  return registration;
}


describe("NotificationSetup", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.history.replaceState(null, "", "/");
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("explains that iPhone users must open the Home Screen app", () => {
    installWebPushGlobals({ ios: true, standalone: false });

    render(<NotificationSetup open onClose={vi.fn()} />);

    expect(
      screen.getByText((_, element) =>
        element?.tagName === "P" && /tap\s+Share, then\s+Add to Home Screen/i.test(element.textContent ?? ""),
      ),
    ).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Enable" })).toBeNull();
  });

  it("requests permission from a click and stores the browser installation token", async () => {
    const registration = installWebPushGlobals();
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(jsonResponse({ enabled: true, vapid_public_key: "AQID-v8" }))
      .mockResolvedValueOnce(jsonResponse({ enabled: true, installation_token: "browser-token" }));

    render(<NotificationSetup open onClose={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: "Enable" }));

    await screen.findByText(/Connect the Shortcut once/i);
    expect(Notification.requestPermission).toHaveBeenCalledTimes(1);
    expect(navigator.serviceWorker.register).toHaveBeenCalledWith("/service-worker.js", { scope: "/" });
    expect(registration.pushManager.subscribe).toHaveBeenCalledWith({
      userVisibleOnly: true,
      applicationServerKey: expect.any(ArrayBuffer),
    });
    expect(window.localStorage.getItem(NOTIFICATION_INSTALLATION_TOKEN_KEY)).toBe("browser-token");
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("confirms a claimed pairing after the Shortcut callback", async () => {
    installWebPushGlobals();
    Object.defineProperty(globalThis, "Notification", {
      configurable: true,
      value: { permission: "granted", requestPermission: vi.fn() },
    });
    window.localStorage.setItem(NOTIFICATION_INSTALLATION_TOKEN_KEY, "browser-token");
    window.history.replaceState(
      null,
      "",
      "/notifications/connected?pairing_id=9f7cf239-5b35-4e56-96e8-1f61a4f7c3c0",
    );
    vi.spyOn(globalThis, "fetch").mockResolvedValue(jsonResponse({ status: "connected" }));

    render(<NotificationSetup open onClose={vi.fn()} />);

    await waitFor(() => {
      expect(screen.getByText(/Future searches can notify you/i)).toBeTruthy();
    });
  });
});
