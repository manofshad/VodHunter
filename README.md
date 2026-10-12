# VodHunter

## [vodhunter.com](https://vodhunter.com/)

Find the original Twitch moments from a TikTok clip. VodHunter matches the clip's
audio to a streamer's VODs and links you to the timestamps. It also works with
edited clips that combine moments from multiple VODs.

![VodHunter search result matching a TikTok clip to 07:32:17 in a jasontheween Twitch VOD](docs/images/matched-search.png)

## Find a moment

1. Paste a TikTok link, choose a streamer, and click **Search**.
2. Open a match to jump to that moment in the Twitch VOD.

On iPhone, use the site's **iPhone shortcut** button to add VodHunter to your
share sheet and search directly from TikTok.

## How it works

VodHunter saves audio fingerprints from Twitch VODs along with their timestamps.
It compares your TikTok clip's audio fingerprints against them, then checks the
matches to find where each part of the clip appears in the VODs.

```mermaid
flowchart LR
    Clip["TikTok clip"] --> Audio["Match the audio"]
    VODs["Indexed Twitch VODs"] --> Audio
    Audio --> Moments["Matched moments"]
    Moments --> Watch["Open Twitch at the timestamp"]

    classDef source fill:#1e2939,stroke:#64748b,color:#ffffff
    classDef match fill:#ff2446,stroke:#ff2446,color:#101827
    classDef result fill:#101827,stroke:#ff2446,color:#ffffff
    class Clip,VODs source
    class Audio match
    class Moments,Watch result
    linkStyle default stroke:#ff2446
```

*Search covers the VODs VodHunter has indexed for the selected streamer. Very
short clips, heavy music overlays, or heavily altered audio can leave some or
all of a clip unmatched.*

## License

VodHunter's own source is licensed under the MIT License.
