# Web Push notification deployment

VodHunter uses standards-based Web Push. On iPhone and iPad, notifications are available to Home Screen web apps on iOS/iPadOS 16.4 or later. No Apple Developer Program membership or APNs certificate is required.

This first release accepts only HTTPS subscription endpoints under Apple's `*.push.apple.com` domain. That both matches the iOS scope and prevents arbitrary endpoints from turning delivery into a server-side request primitive.

## Runtime configuration

Generate one P-256 VAPID key pair and keep it stable. Rotating it invalidates existing browser subscriptions.

With `py-vapid` installed by `pywebpush`:

```bash
mkdir vodhunter-vapid
cd vodhunter-vapid
vapid --gen
```

Configure this Coolify secret for the API service:

- `WEB_PUSH_VAPID_PRIVATE_KEY`: the generated `private_key.pem` contents (multiline PEM and escaped-newline PEM are both accepted)

The API derives the browser-facing VAPID public key from that private key. The
VodHunter site URL, Shortcut name, and VAPID contact URI are public product
constants and are kept in the codebase instead of deployment configuration.

Do not commit the generated private key or any browser/Shortcut token. The
checked-in `.env.example` files contain the secret's name only.

The deployment migration creates:

- `notification_installations`: hashed browser and Shortcut credentials
- `push_subscriptions`: browser-generated endpoint and encryption material
- `notification_pairings`: short-lived, single-use pairing codes
- a nullable installation link and delivery timestamp on `search_requests`

## Delivery behavior

The search result is persisted before delivery is attempted. A Push provider failure cannot change a completed search into a failed search. HTTP 404 and 410 responses revoke stale subscriptions. A notification is marked sent after at least one active subscription accepts it.

Apple devices must be able to reach their Push service. If an outbound firewall is added later, allow HTTPS access to Apple's Web Push service endpoints.

## Deployment smoke test

1. Confirm `GET /api/notifications/config` returns `enabled: true` and the public key only.
2. Install VodHunter from Safari to an iPhone Home Screen.
3. Enable notifications and connect the Shortcut.
4. Start a TikTok search and verify the Shortcut returns to TikTok.
5. Verify exactly one notification arrives and tapping it opens the matching shared-search page.
6. Repeat with a no-match result and a deliberately failed input.

If configuration is absent, `/api/notifications/config` reports `enabled: false`, notification setup explains that the feature is unavailable, and existing Shortcut searches continue through the immediate `/share` fallback.
