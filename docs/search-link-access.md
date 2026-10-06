# Search link access

Anonymous searches use a bearer capability: possession of the complete link grants read access to that one search. This preserves cross-device sharing and the iOS Shortcut handoff without allowing searches to be enumerated. It does not restrict reads to the originating browser or a signed-in account. Treat a shared link as permission to see the submitted TikTok URL, streamer, processing timestamps, result, and any search error.

## API and frontend

- `POST /api/search/clip` returns `search_token`, status, and stage. Tokens contain 32 cryptographically random bytes encoded as 43 URL-safe characters.
- `GET /api/search/clip` requires `Authorization: Bearer <search_token>`. Missing, malformed, and unknown tokens return the same 404 response. Tokens in query strings do not grant access.
- The numbered `GET /api/search/clip/{search_id}` route is removed. Internal database IDs still identify jobs in processing and operational logs, but are not exposed as public search handles.
- New links use `https://vodhunter.com/share#<search_token>`. Fragments are read by the frontend and never included in HTTP request targets or referrer headers. The API token travels only in the authorization header. Do not configure infrastructure to log authorization headers or response bodies.
- Search creation and read responses use `Cache-Control: no-store`. The frontend also sets a no-referrer policy.
- Only the SHA-256 token hash is stored, with a unique index. Existing records retain a NULL hash and cannot be recovered through their old IDs. Public reads also require `source_app = 'public'`.
- Active searches saved in browser storage use tokens. Old numeric active-search records cannot resume; saved result history remains local to the browser.
- Nginx limits search creation by POST only. GET polling shares the endpoint and must not consume the six-per-minute creation limit.

## Rollout

1. Apply Alembic revision `20261005_0016` before starting the updated API. The startup schema check requires its token-hash column.
2. Deploy the updated API and frontend together from merged `main` through Coolify. Do not leave an old API replica serving the numeric lookup route.
3. Update and re-share the physical-device iOS Shortcut using [the revised actions](ios-shortcut.md). Actions 11–12 must read `search_token` and open `https://vodhunter.com/share#` followed by that value.
4. Confirm a new search polls, completes, and opens after refresh and on another device with its full link. Confirm the old numbered API URL returns 404 and a request without a token cannot retrieve a result.

This intentionally breaks old numbered links, old numeric active-search state, and installed Shortcuts using the previous response field. Run a new search to obtain a secure link. No tokens are backfilled or recoverable from an old ID. Existing database records and local result history are not deleted.

Merging this change does not by itself prove the production deployment has finished. Verify the public endpoint and resolve the live Coolify containers at inspection time as described in `AGENTS.md`. Do not roll back to the numeric lookup API to restore old links; that reopens enumeration access.
