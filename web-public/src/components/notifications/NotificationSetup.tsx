import { useEffect, useMemo, useState } from "react";
import { Bell, CheckCircle2, ExternalLink, X } from "lucide-react";

import {
  createNotificationPairing,
  getNotificationConfig,
  getNotificationPairingStatus,
  NOTIFICATION_INSTALLATION_TOKEN_KEY,
  NOTIFICATION_PAIRING_ID_KEY,
  registerPushSubscription,
  urlBase64ToArrayBuffer,
} from "../../api/notifications";

type SetupState = "idle" | "enabling" | "enabled" | "connecting" | "connected" | "error";

function isIosDevice(): boolean {
  return /iPad|iPhone|iPod/.test(navigator.userAgent);
}

function isStandalone(): boolean {
  return (
    window.matchMedia?.("(display-mode: standalone)").matches === true ||
    (navigator as Navigator & { standalone?: boolean }).standalone === true
  );
}

function supportsWebPush(): boolean {
  return (
    "serviceWorker" in navigator &&
    "PushManager" in window &&
    "Notification" in window &&
    window.isSecureContext
  );
}

async function enablePushNotifications(): Promise<void> {
  const permission = await Notification.requestPermission();
  if (permission !== "granted") {
    throw new Error("Notifications were not allowed. You can enable them later in iPhone Settings.");
  }

  const config = await getNotificationConfig();
  if (!config.enabled || !config.vapid_public_key) {
    throw new Error("VodHunter notifications are not available yet.");
  }

  const registration = await navigator.serviceWorker.register("/service-worker.js", { scope: "/" });
  const existingSubscription = await registration.pushManager.getSubscription();
  const subscription =
    existingSubscription ??
    (await registration.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToArrayBuffer(config.vapid_public_key),
    }));

  const existingToken = window.localStorage.getItem(NOTIFICATION_INSTALLATION_TOKEN_KEY);
  let result;
  try {
    result = await registerPushSubscription(subscription.toJSON(), existingToken);
  } catch (error) {
    if (!existingToken) {
      throw error;
    }
    window.localStorage.removeItem(NOTIFICATION_INSTALLATION_TOKEN_KEY);
    result = await registerPushSubscription(subscription.toJSON(), null);
  }
  const token = result.installation_token ?? existingToken;
  if (!token) {
    throw new Error("VodHunter could not save this notification installation.");
  }
  window.localStorage.setItem(NOTIFICATION_INSTALLATION_TOKEN_KEY, token);
}

interface NotificationSetupProps {
  open: boolean;
  onClose: () => void;
}

export function NotificationSetup({ open, onClose }: NotificationSetupProps) {
  const [state, setState] = useState<SetupState>("idle");
  const [message, setMessage] = useState<string | null>(null);
  const supported = useMemo(() => supportsWebPush(), []);
  const iosNeedsInstall = useMemo(() => isIosDevice() && !isStandalone(), []);

  useEffect(() => {
    if (!open) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose, open]);

  useEffect(() => {
    if (!open) return;
    if (!supported) return;
    const callback = window.location.pathname.match(/^\/notifications\/(connected|cancelled|error)$/);
    if (!callback) {
      if (Notification.permission === "granted" && window.localStorage.getItem(NOTIFICATION_INSTALLATION_TOKEN_KEY)) {
        setState("enabled");
      }
      return;
    }

    if (callback[1] !== "connected") {
      setState("error");
      setMessage(callback[1] === "cancelled" ? "Shortcut connection was cancelled." : "The Shortcut could not be connected.");
      return;
    }

    const token = window.localStorage.getItem(NOTIFICATION_INSTALLATION_TOKEN_KEY);
    const pairingId =
      new URLSearchParams(window.location.search).get("pairing_id") ??
      window.localStorage.getItem(NOTIFICATION_PAIRING_ID_KEY);
    if (!token || !pairingId) {
      setState("connected");
      setMessage("The Shortcut finished connecting. Return to VodHunter from its Home Screen icon.");
      return;
    }

    void getNotificationPairingStatus(pairingId, token)
      .then((result) => {
        if (result.status === "connected") {
          window.localStorage.removeItem(NOTIFICATION_PAIRING_ID_KEY);
          setState("connected");
          setMessage("VodHunter Shortcut connected. Future searches can notify you when they finish.");
        } else {
          setState("error");
          setMessage(result.status === "expired" ? "This pairing expired. Connect the Shortcut again." : "The Shortcut has not finished connecting yet.");
        }
      })
      .catch((error: Error) => {
        setState("error");
        setMessage(error.message);
      });
  }, [open, supported]);

  const onEnable = async () => {
    setState("enabling");
    setMessage(null);
    try {
      await enablePushNotifications();
      setState("enabled");
      setMessage("Notifications are enabled. Connect the Shortcut once to finish setup.");
    } catch (error) {
      setState("error");
      setMessage(error instanceof Error ? error.message : "Notifications could not be enabled.");
    }
  };

  const onConnect = async () => {
    const token = window.localStorage.getItem(NOTIFICATION_INSTALLATION_TOKEN_KEY);
    if (!token) {
      setState("error");
      setMessage("Enable notifications before connecting the Shortcut.");
      return;
    }

    setState("connecting");
    setMessage(null);
    try {
      const pairing = await createNotificationPairing(token);
      window.localStorage.setItem(NOTIFICATION_PAIRING_ID_KEY, pairing.pairing_id);
      window.location.assign(pairing.shortcut_url);
    } catch (error) {
      setState("error");
      setMessage(error instanceof Error ? error.message : "The Shortcut could not be opened.");
    }
  };

  if (!open) return null;

  const notificationsEnabled =
    state === "enabled" || state === "connecting" || state === "connected";

  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center bg-black/70 p-0 sm:items-center sm:p-6" role="presentation">
      <section
        role="dialog"
        aria-modal="true"
        aria-labelledby="notification-setup-title"
        className="w-full max-w-lg rounded-t-2xl border border-gray-700 bg-gray-900 p-6 shadow-2xl sm:rounded-2xl"
      >
        <div className="mb-5 flex items-start justify-between gap-4">
          <div className="flex gap-3">
            <span className="rounded-full bg-[#fb2844]/15 p-2 text-[#fb2844]"><Bell aria-hidden="true" className="size-5" /></span>
            <div>
              <h2 id="notification-setup-title" className="text-xl font-bold text-white">Get search results while you keep scrolling</h2>
              <p className="mt-1 text-sm leading-6 text-gray-300">VodHunter can notify your Home Screen app when a Shortcut search finishes.</p>
            </div>
          </div>
          <button type="button" aria-label="Close notification setup" onClick={onClose} className="rounded-lg p-2 text-gray-400 hover:bg-gray-800 hover:text-white"><X aria-hidden="true" className="size-5" /></button>
        </div>

        {!supported ? (
          <p className="rounded-xl border border-amber-500/40 bg-amber-500/10 p-4 text-sm text-amber-100">Web Push is not supported in this browser. On iPhone, use iOS 16.4 or later and open VodHunter from its Home Screen icon.</p>
        ) : iosNeedsInstall ? (
          <div className="space-y-3 text-sm leading-6 text-gray-200">
            <p>First install VodHunter: in Safari tap <strong>Share</strong>, then <strong>Add to Home Screen</strong>. Open the new VodHunter icon and return here.</p>
            <p className="text-gray-400">iPhone only offers website push permission to a web app opened from the Home Screen.</p>
          </div>
        ) : (
          <div className="space-y-4">
            <div className="flex items-center gap-3 rounded-xl border border-gray-700 bg-gray-800/70 p-4">
              {notificationsEnabled ? <CheckCircle2 aria-hidden="true" className="size-5 text-emerald-400" /> : <span className="flex size-5 items-center justify-center rounded-full border border-gray-500 text-xs text-gray-300">1</span>}
              <div className="min-w-0 flex-1"><p className="font-semibold text-white">Enable notifications</p><p className="text-sm text-gray-400">Allow VodHunter to send a visible result alert.</p></div>
              {!notificationsEnabled && <button type="button" onClick={onEnable} disabled={state === "enabling"} className="rounded-lg bg-[#fb2844] px-3 py-2 text-sm font-bold text-white disabled:opacity-60">{state === "enabling" ? "Enabling…" : "Enable"}</button>}
            </div>

            <div className="flex items-center gap-3 rounded-xl border border-gray-700 bg-gray-800/70 p-4">
              {state === "connected" ? <CheckCircle2 aria-hidden="true" className="size-5 text-emerald-400" /> : <span className="flex size-5 items-center justify-center rounded-full border border-gray-500 text-xs text-gray-300">2</span>}
              <div className="min-w-0 flex-1"><p className="font-semibold text-white">Connect Shortcut</p><p className="text-sm text-gray-400">One tap opens VodHunter Search and pairs it privately.</p></div>
              {notificationsEnabled && state !== "connected" && <button type="button" onClick={onConnect} disabled={state === "connecting"} className="inline-flex items-center gap-1 rounded-lg border border-gray-600 px-3 py-2 text-sm font-bold text-white hover:bg-gray-700 disabled:opacity-60">{state === "connecting" ? "Opening…" : "Connect"}<ExternalLink aria-hidden="true" className="size-4" /></button>}
            </div>
          </div>
        )}

        {message && <p role={state === "error" ? "alert" : "status"} className={`mt-4 rounded-lg p-3 text-sm ${state === "error" ? "bg-red-500/10 text-red-200" : "bg-emerald-500/10 text-emerald-200"}`}>{message}</p>}
      </section>
    </div>
  );
}
