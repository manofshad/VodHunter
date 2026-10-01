# TikTok MP4 migration to private Cobalt

Public search can select `TIKTOK_DOWNLOADER=yt-dlp` (initial default) or
`TIKTOK_DOWNLOADER=cobalt`. The Cobalt path resolves a validated TikTok URL
with `downloadMode=auto` and `alwaysProxy=true`, streams the complete MP4 into
the existing temporary download directory, and passes that MP4 to the existing
duration probe and FFmpeg query preprocessor. The preprocessor continues to
produce 8 kHz mono PCM. TikTok audio-only modes are intentionally unsupported:
they may return a separate music asset or an empty body instead of the mixed
video soundtrack.

The downloader accepts Cobalt `tunnel` and `redirect` responses. It rejects
other statuses, empty or oversized media, invalid MIME, declared-length
truncation, excess redirects, and transfers that exceed the 90-second download
deadline. Partial `.part` files are removed on failure. Prometheus records
provider, total duration, Cobalt resolution duration, media transfer duration,
size, and bounded failure categories. Completed search diagnostics also include
the selected provider and download timings.
No automatic fallback occurs: a Cobalt failure remains visible as a failed
search. An operator can restore the previous provider with the environment
setting below.

## Deploy and validate

1. Merge a release containing this code with `TIKTOK_DOWNLOADER=yt-dlp`.
   The Compose application starts the digest-pinned Cobalt 11.7.1 container
   on its private network. It publishes no host port or Traefik route.
2. Resolve the currently running API and Cobalt containers with `docker ps`.
   From the API container, run `curl -fsS http://cobalt:9000/` and verify that
   the response advertises version 11.7.1 and includes `tiktok` in its services.
   Verify the Cobalt container is healthy, has the intended CPU/memory limits,
   and has no public port binding or proxy label.
3. For several currently public TikTok video URLs, run the same search through
   both provider settings. Include canonical and short share links, short and
   long clips, and varied MP4 sizes. For each provider and URL, run at least
   five downloads. Record median, p90, and failure rate, and compare MP4 and
   normalized PCM duration, PCM hash or waveform, matched VOD, score, and
   timestamp. A result is acceptable only if it uses the complete mixed audio,
   matches the same VOD, and has no unexplained timestamp drift beyond the
   0.5-second NMFP hop. Target at least a 2x median download improvement with
   no worse failure rate. Keep the fixture URL list outside source control if
   the clips are not stable public test material.
4. Set `TIKTOK_DOWNLOADER=cobalt` in Coolify and redeploy the API. Do not change
   `COBALT_API_URL` unless the private service hostname changes; it and Cobalt's
   `API_URL` must both be `http://cobalt:9000/` so tunnel URLs resolve from the
   API container. Watch the Search Performance dashboard for provider traffic,
   p50/p90 download latency, Cobalt resolution and transfer p90, failure
   categories, FFmpeg processing errors, and search match rate.
5. Observe for at least seven days and 100 production downloads before removing
   the TikTok yt-dlp rollback path. These are rollout gates, not claims that
   production validation has already happened.

To roll back during the observation window, set
`TIKTOK_DOWNLOADER=yt-dlp` in Coolify and redeploy the API. The worker's
Twitch VOD resolver continues to use yt-dlp regardless of this flag.

## yt-dlp dependency audit

| Location | Current purpose | Initial migration decision |
| --- | --- | --- |
| `backend/services/remote_clip_downloader.py` | TikTok MP4 download | Keep only through the observation window as explicit rollback. |
| `sources/live_archive_vod_source.py` | Resolve a growing Twitch VOD to an HLS media URL for FFmpeg chunk extraction | Retain. |
| `sources/historical_archive_vod_source.py` | Resolve archived Twitch VOD HLS, including refreshed playlists | Retain. |
| `experiments/fingerprint_benchmark/audio.py` and `preflight.py` | Resolve Twitch VODs and acquire TikTok benchmark fixtures | Retain until benchmark tooling is separately migrated. |
| `notebooks/colab_backfill_ingest.ipynb` and `colab_live_ingest.ipynb` | Install yt-dlp for notebook ingestion | Retain until those notebooks are revised. |
| `backend/requirements-nmfp.txt` | Install yt-dlp into the common API/worker runtime image | Retain for the worker initially; split API and worker requirements after TikTok cutover. |
| `README.md`, benchmark README, and downloader tests | Documentation and regression coverage | Update with each removal step. |

No direct `curl_cffi` dependency, yt-dlp environment variable, update job, or
dedicated Dockerfile install step was found in the repository. The current
Dockerfile installs the common requirements, so yt-dlp remains in the API,
worker, and migration images even after TikTok switches to Cobalt. The first
post-observation cleanup should remove the TikTok implementation and flag,
move the two Twitch call sites behind a shared resolver interface, and split
the API/worker images so only the worker installs yt-dlp. Full removal needs a
tested replacement for Twitch VOD HLS resolution and the remaining benchmark
and notebook uses. Keep rollback through the preceding release after removing
the TikTok flag.
