import { getApiBase, parseJson } from "./client";

export const NOTIFICATION_INSTALLATION_TOKEN_KEY = "vodhunter.notification-installation-token.v1";
export const NOTIFICATION_PAIRING_ID_KEY = "vodhunter.notification-pairing-id.v1";

export interface NotificationConfig {
  enabled: boolean;
  vapid_public_key: string | null;
}

export interface PushSubscriptionRegistration {
  enabled: boolean;
  installation_token: string | null;
}

export interface NotificationPairing {
  pairing_id: string;
  expires_at: string;
  shortcut_url: string;
}

export interface NotificationPairingStatus {
  status: "pending" | "connected" | "expired";
}

export async function getNotificationConfig(): Promise<NotificationConfig> {
  const response = await fetch(`${getApiBase()}/notifications/config`);
  return parseJson<NotificationConfig>(response);
}

export async function registerPushSubscription(
  subscription: PushSubscriptionJSON,
  installationToken: string | null,
): Promise<PushSubscriptionRegistration> {
  const response = await fetch(`${getApiBase()}/notifications/subscriptions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(installationToken ? { Authorization: `Bearer ${installationToken}` } : {}),
    },
    body: JSON.stringify(subscription),
  });
  return parseJson<PushSubscriptionRegistration>(response);
}

export async function createNotificationPairing(installationToken: string): Promise<NotificationPairing> {
  const response = await fetch(`${getApiBase()}/notifications/pairings`, {
    method: "POST",
    headers: { Authorization: `Bearer ${installationToken}` },
  });
  return parseJson<NotificationPairing>(response);
}

export async function getNotificationPairingStatus(
  pairingId: string,
  installationToken: string,
): Promise<NotificationPairingStatus> {
  const response = await fetch(`${getApiBase()}/notifications/pairings/${encodeURIComponent(pairingId)}`, {
    headers: { Authorization: `Bearer ${installationToken}` },
  });
  return parseJson<NotificationPairingStatus>(response);
}

export function urlBase64ToArrayBuffer(value: string): ArrayBuffer {
  const padding = "=".repeat((4 - (value.length % 4)) % 4);
  const base64 = (value + padding).replace(/-/g, "+").replace(/_/g, "/");
  const raw = window.atob(base64);
  const bytes = new Uint8Array(new ArrayBuffer(raw.length));
  for (let index = 0; index < raw.length; index += 1) {
    bytes[index] = raw.charCodeAt(index);
  }
  return bytes.buffer;
}
