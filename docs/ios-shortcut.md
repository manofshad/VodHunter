# Maintain the VodHunter iPhone shortcut

The public navbar opens a setup dialog and downloads
`web-public/public/shortcuts/vodhunter-search.shortcut`.
The shortcut source is `shortcuts/vodhunter-search.cherri`.

## Update the download

1. Edit the Cherri source.
2. Open [Cherri Playground](https://playground.cherrilang.org/) and paste the source into its editor.
3. Click the hammer to build the shortcut. Resolve compiler errors before you export.
4. Open Export, click **Sign Shortcut**, and then click **Download Shortcut**.
5. Replace `web-public/public/shortcuts/vodhunter-search.shortcut` with the signed download.
6. Run the frontend tests and build. Complete the iPhone checks below before you release the update.

The current artifact was compiled with Playground v2.3.0 and signed through
[RoutineHub's HubSign service](https://cherrilang.org/compiler/signing.html).
Signing sends the public workflow definition to that service. The download
contains no account credentials or saved search tokens. The website serves the
committed file and does not contact the compiler or signing service at runtime.

Apple also supports signing exported shortcuts on a Mac and sharing them as
files for **Anyone**. See [Apple's shortcut sharing guide](https://support.apple.com/guide/shortcuts-mac/share-shortcuts-apdf01f8c054/mac).
Distribute the signed file, not an unsigned plist renamed to `.shortcut`.

## Keep the API handoff current

The shortcut receives URLs and text from the share sheet. It extracts the first
URL, fetches the current streamer list, and asks the user to select one streamer.
Confirmation occurs before the shortcut creates a search.

The shortcut sends form fields `tiktok_url` and `streamer` to
`POST https://vodhunter.com/api/search/clip`. It reads `search_token` from the
response and opens `https://vodhunter.com/share#<search_token>`. The website
polls the job and displays its result. The shortcut does not poll.

The older `search_id` response field and `/share?search_id=...` links no longer
work. Replace the older shortcut with this version. Do not add numbered search
access to recover those links.

## Check on an iPhone

- Download the file in Safari, open the downloaded file, and tap **Add Shortcut**.
- Confirm **Search with VodHunter** appears under TikTok's **Share**, then **More**. If needed, enable **Show in Share Sheet** in the shortcut details.
- Share both a full TikTok video URL and a TikTok short link.
- Confirm the picker includes each streamer returned by the API, including Stable Ronaldo.
- Cancel before confirmation and confirm that no search is created.
- Confirm a search opens `/share#` followed by its token and shows progress or a completed result.
- Refresh during a search and confirm the page restores it.
- Run the shortcut without input and confirm that it asks you to share a TikTok video.
- Replace an older shortcut and repeat the TikTok share flow.

Browser layout and download checks do not verify import or execution in Apple's
Shortcuts app. Those checks require a physical iPhone or iPad.
