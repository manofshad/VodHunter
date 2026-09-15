# VodHunter Search iOS Shortcut

The `VodHunter Search` Shortcut starts a search from TikTok's Share Sheet. Once notifications are paired, it finishes without opening a browser so the user can keep scrolling. VodHunter sends a Web Push notification when the job reaches a terminal state; tapping it opens `/share?search_id=...` with the stored result.

If the Shortcut is not paired, its token has expired, or server-side notifications are disabled, the existing behavior remains intact: it opens the VodHunter result page immediately and the page polls the job.

## Requirements

- iOS or iPadOS 16.4 or later for Home Screen Web Push
- VodHunter added to the Home Screen from Safari
- The web app opened from its Home Screen icon
- Notifications allowed after tapping **Enable Notifications**
- Shortcut name exactly `VodHunter Search` (the pairing URL launches it by name)
- **Show in Share Sheet** enabled, receiving **URLs** and **Text**

Apple references:

- [Web Push for Home Screen web apps](https://webkit.org/blog/13878/web-push-for-web-apps-on-ios-and-ipados/)
- [Launch a shortcut from a URL](https://support.apple.com/guide/shortcuts/apd624386f42/ios)
- [Use x-callback-url with Shortcuts](https://support.apple.com/guide/shortcuts/apdcd7f20a6f/ios)
- [Make API requests from Shortcuts](https://support.apple.com/guide/shortcuts/apd58d46713f/ios)

## Shortcut v2 action sequence

The entire existing TikTok flow moves into the **Otherwise** branch described below.

1. Configure the Shortcut to receive **URLs** and **Text** from the Share Sheet. For no input, use **Stop and Respond** with `Share a TikTok video with this shortcut.`
2. Add **Get Text from Shortcut Input**. Rename its output `Input Text`.
3. Add **If** `Input Text` begins with `vodhunter-setup:`.
4. In the pairing branch:
   1. Add **Replace Text**. Replace `vodhunter-setup:` with nothing in `Input Text`. Rename the result `Pairing Code`.
   2. Add **URL**: `https://vodhunter.com/api/notifications/pairings/claim`.
   3. Add **Get Contents of URL** with method `POST`, request body `Form`, and field `pairing_code` set to `Pairing Code`.
   4. Add **Get Dictionary Value** for `shortcut_token` from the response. Rename it `Notification Token`.
   5. Add **Text** containing the `Notification Token` magic variable.
   6. Add **Save File** to `iCloud Drive/Shortcuts/VodHunter/notification-token.txt`. Turn off **Ask Where to Save** and turn on **Overwrite If File Exists**.
   7. Add **Show Alert**: `VodHunter Shortcut connected`.
5. Add **Otherwise**. Put every normal TikTok-search action below inside this branch.
6. Add **Get URLs from Shortcut Input**.
7. Add **Get Item from List**, configured for **First Item**. Rename it `TikTok URL`.
8. Add **If** `TikTok URL` does not have a value. Inside it, show `No TikTok URL was found. Share a TikTok video and try again.`, then **Stop This Shortcut**.
9. Add **Get File from Folder** for `Shortcuts/VodHunter/notification-token.txt`. Turn off **Error If Not Found**. Rename the result `Notification Token File`.
10. Add **If** `Notification Token File` has a value. Inside it, add **Get Text from Input** using that file and rename the result `Notification Token`. In **Otherwise**, add an empty **Text** action and also rename its result `Notification Token`.
11. Add **URL**: `https://vodhunter.com/api/search/streamers`.
12. Add **Get Contents of URL** using `GET`.
13. Add **Repeat with Each** over the response. Inside it, get dictionary value `name` from `Repeat Item`, then **Add to Variable** named `Streamers`.
14. Add **Choose from List** using `Streamers`, with multiple selection disabled. Rename the result `Streamer`.
15. Add **Choose from Menu** with prompt `Search this TikTok clip with [Streamer]?`.
    - **Cancel** contains **Stop This Shortcut**.
    - **Search** contains the remaining API actions.
16. Under **Search**, add **URL**: `https://vodhunter.com/api/search/clip`.
17. Add **Get Contents of URL** with method `POST`, request body `Form`, and fields:
    - `tiktok_url`: `TikTok URL`
    - `streamer`: `Streamer`
    - `notification_token`: `Notification Token`
18. Add **Get Dictionary Value** for `notifications_enabled` from the response.
19. Add **If** `notifications_enabled` is true:
    - Add **Show Notification**: `VodHunter is searching. You can keep scrolling.`
    - Add **Stop This Shortcut**. Because no browser is opened, iOS returns to TikTok.
20. In **Otherwise** (notifications unavailable):
    1. Get dictionary value `search_id` from the same POST response. Rename it `Search ID`.
    2. Add **Text**: `https://vodhunter.com/share?search_id=` followed by `Search ID`.
    3. Add **Open URLs** using that Text result.
21. Close the notification fallback **If**, the **Search** menu item, the menu, and the top-level pairing **If**.

The pairing branch must complete normally rather than using **Stop This Shortcut**. This allows the `x-success` callback to return to VodHunter, where the web app confirms that the one-time code was claimed.

## One-tap onboarding

1. Install or update the `VodHunter Search` Shortcut.
2. In Safari, open VodHunter, tap **Share**, then **Add to Home Screen**.
3. Open VodHunter from its Home Screen icon.
4. Tap **Notifications** in the header, then **Enable** and approve the iOS prompt.
5. Tap **Connect**. VodHunter creates a five-minute, single-use pairing code and opens the installed Shortcut automatically.
6. Approve any first-time Shortcuts file/network prompts. The Shortcut saves its scoped token in iCloud Drive and returns to VodHunter.

Depending on the iOS version, the callback may open VodHunter in Safari instead of the standalone Home Screen window. The pairing is still complete; close Safari and keep using VodHunter from its Home Screen icon.

The long-lived Shortcut token is never placed in the custom-scheme URL. The server stores only SHA-256 token hashes. Creating a new pairing rotates the Shortcut token for that installation.

## Distribution

Build and test the Shortcut on a physical iPhone, then publish an updated **Copy iCloud Link** from the Shortcut sharing menu. Apple documents [sharing a Shortcut through iCloud](https://support.apple.com/guide/shortcuts/apdf01f8c054/ios).

The Shortcut is maintained in Apple's Shortcuts app, not generated by this repository. Update this document and the public iCloud link whenever its action sequence or API contract changes.

## Device acceptance checks

- Add to Home Screen uses the VodHunter icon and opens in standalone mode
- Notification permission is requested only after tapping **Enable**
- **Connect** opens `VodHunter Search`, saves the token file, and returns with a connected confirmation
- A pairing code cannot be claimed twice and expires after five minutes
- TikTok full and short share URLs still work
- A paired search returns to TikTok without opening Safari/Chrome
- Match, no-match, and failed searches each deliver one visible notification
- Tapping a notification opens the correct `/share?search_id=...` result
- Missing, invalid, or revoked Shortcut tokens use the immediate browser fallback
- Reconnecting rotates the Shortcut token and future searches still notify
