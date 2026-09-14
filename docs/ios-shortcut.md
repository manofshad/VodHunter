# Search with VodHunter iOS Shortcut

This Shortcut starts a VodHunter search directly from TikTok's Share Sheet. The user chooses a streamer, confirms with **Search**, and is sent to the VodHunter frontend while the server-side job is running.

## Shortcut settings

- Name: `Search with VodHunter`
- Enable **Show in Share Sheet**.
- Receive **URLs** and **Text** from the Share Sheet.
- When there is no input, choose **Stop and Respond** and use: `Share a TikTok video with this shortcut.`

Apple references:

- [Launch a shortcut from another app](https://support.apple.com/guide/shortcuts/launch-a-shortcut-from-another-app-apd163eb9f95/ios)
- [Limit the input for a shortcut](https://support.apple.com/guide/shortcuts/limit-the-input-for-a-shortcut-apd8195f96d6/ios)
- [Make API requests from Shortcuts](https://support.apple.com/guide/shortcuts/apd58d46713f/ios)

## Actions

1. Add **Get URLs from Shortcut Input**.
2. Add **Get Item from List**, configured to get the **First Item**. Rename its output variable to `TikTok URL`.
3. Add **If** `TikTok URL` does not have a value. Inside it, add **Show Alert** with `No TikTok URL was found`, followed by **Stop This Shortcut**.
4. Add **URL** with `https://vodhunter.com/api/search/streamers`.
5. Add **Get Contents of URL** using `GET`.
6. Add **Repeat with Each** over the returned list.
   - Inside the repeat, add **Get Dictionary Value** for the `name` key from the Repeat Item.
   - Add **Add to Variable** and name the variable `Streamers`.
7. Add **Choose from List** using `Streamers`, with multiple selection disabled. Rename its output variable to `Streamer`.
8. Add **Choose from Menu** with the prompt `Search this TikTok clip with Streamer?` Insert the `Streamer` magic variable in place of the final word.
   - Menu item **Search** continues with the API actions below.
   - Menu item **Cancel** contains **Stop This Shortcut**.
9. Under **Search**, add **URL** with `https://vodhunter.com/api/search/clip`.
10. Add **Get Contents of URL** and configure:
   - Method: `POST`
   - Request Body: `Form`
   - `tiktok_url`: the `TikTok URL` magic variable
   - `streamer`: the `Streamer` magic variable
11. Add **Get Dictionary Value** for `search_id` from the POST response. Rename the output to `Search ID`.
12. Add **Text** containing `https://vodhunter.com/share?search_id=` followed by the `Search ID` magic variable.
13. Add **Open URLs** using that Text result.

The Shortcut must not poll the job. Opening the frontend immediately lets the normal VodHunter polling UI take over and avoids depending on a long-running Shortcut process.

## Distribution

Create and test the Shortcut on a physical iPhone, then use **Copy iCloud Link** from the Shortcut's sharing menu. Publish that link as the install action on VodHunter. Apple documents [sharing a Shortcut through iCloud](https://support.apple.com/guide/shortcuts/apdf01f8c054/ios).

The iCloud Shortcut itself is maintained in Apple's Shortcuts app. Update this document whenever its action sequence or API contract changes.

## Device acceptance checks

- TikTok full video URL and TikTok short share URL
- Shortcut visible under TikTok **Share → More**
- No-input error
- Streamer list loads and only one streamer can be selected
- Cancel does not create a search
- Search creates exactly one job and opens `/share?search_id=...`
- Queued/running progress, completed match, no match, failed job, and unknown job
- Refresh during processing and opening a completed URL on another device
