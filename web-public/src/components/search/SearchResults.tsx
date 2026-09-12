import { useEffect, useState } from "react";
import { ExternalLink } from "lucide-react";

import { SearchResponse, SearchSegment, SearchSource } from "../../api/types";
import { AvatarImage } from "./AvatarImage";
import { formatTimelineTime } from "./searchUtils";

interface SearchResultCardProps {
  result: SearchResponse;
  lastSubmittedUrl: string;
}

function getSearchSources(result: SearchResponse): SearchSource[] {
  if (result.sources?.length) {
    return result.sources;
  }

  if (!result.found) {
    return [];
  }

  const segmentsByVideoId = new Map<number, SearchSegment[]>();
  for (const segment of result.segments ?? []) {
    const sourceSegments = segmentsByVideoId.get(segment.video_id) ?? [];
    sourceSegments.push(segment);
    segmentsByVideoId.set(segment.video_id, sourceSegments);
  }

  if (result.video_id !== null && !segmentsByVideoId.has(result.video_id)) {
    segmentsByVideoId.set(result.video_id, []);
  }

  return Array.from(segmentsByVideoId.entries()).map(([videoId, sourceSegments]) => {
    const isPrimary = videoId === result.video_id;
    return {
      video_id: videoId,
      video_url: isPrimary ? result.video_url : null,
      video_url_at_timestamp: isPrimary ? result.video_url_at_timestamp : sourceSegments[0]?.video_url_at_timestamp ?? null,
      thumbnail_url: isPrimary ? result.thumbnail_url : null,
      title: isPrimary ? result.title : null,
      streamer: isPrimary ? result.streamer : null,
      profile_image_url: isPrimary ? result.profile_image_url : null,
      segments: sourceSegments,
    };
  });
}

interface SourceThumbnailProps {
  source: SearchSource;
  href: string | null;
}

function SourceThumbnail({ source, href }: SourceThumbnailProps) {
  const [thumbnailLoadFailed, setThumbnailLoadFailed] = useState(false);

  useEffect(() => {
    setThumbnailLoadFailed(false);
  }, [source.video_id, source.thumbnail_url]);

  const content = source.thumbnail_url && !thumbnailLoadFailed ? (
    <img
      src={source.thumbnail_url}
      alt={source.title ?? "Matched Twitch VOD thumbnail"}
      loading="lazy"
      className="h-full w-full object-cover"
      onError={() => setThumbnailLoadFailed(true)}
    />
  ) : (
    <div className="flex aspect-video items-center justify-center bg-gray-800">
      <span className="rounded-full border border-gray-700 bg-gray-900 px-4 py-1.5 text-xs font-semibold uppercase tracking-[0.16em] text-gray-300">
        Twitch VOD
      </span>
    </div>
  );

  return (
    <div className="self-start overflow-hidden rounded-xl border border-gray-700 bg-gray-800">
      {href ? (
        <a
          href={href}
          target="_blank"
          rel="noreferrer"
          className="block aspect-video"
          aria-label={`Open ${source.title ?? "matched VOD"} at its strongest match`}
        >
          {content}
        </a>
      ) : (
        content
      )}
    </div>
  );
}

interface SearchSourceBlockProps {
  source: SearchSource;
  fallbackStreamer: string | null;
  fallbackProfileImageUrl: string | null;
}

function SearchSourceBlock({ source, fallbackStreamer, fallbackProfileImageUrl }: SearchSourceBlockProps) {
  const sourceHref = source.video_url_at_timestamp ?? source.segments[0]?.video_url_at_timestamp ?? null;
  const sourceTitle = source.title ?? "Matched Twitch VOD";
  const streamer = source.streamer ?? fallbackStreamer ?? "Streamer unavailable";
  const profileImageUrl = source.profile_image_url ?? fallbackProfileImageUrl;

  return (
    <article className="rounded-xl border border-gray-700 bg-gray-800/30 p-4">
      <div className="grid items-start gap-5 md:grid-cols-[minmax(0,240px)_minmax(0,1fr)]">
        <SourceThumbnail source={source} href={sourceHref} />

        <div className="min-w-0">
          <div className="mb-3 flex items-center gap-3">
            <AvatarImage
              src={profileImageUrl}
              alt={streamer}
              className="size-11 rounded-full border border-gray-700 object-cover"
            />
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-[#fb2844]">{streamer}</p>
          </div>
          {sourceHref ? (
            <a href={sourceHref} target="_blank" rel="noreferrer" className="group flex w-full items-start gap-3">
              <h3 className="min-w-0 flex-1 break-words [overflow-wrap:anywhere] text-lg font-bold leading-tight text-white transition group-hover:text-gray-100 md:text-[1.5rem]">
                {sourceTitle}
              </h3>
              <ExternalLink className="mt-1 size-4 shrink-0 text-gray-400 transition group-hover:text-[#fb2844]" />
            </a>
          ) : (
            <h3 className="text-lg font-bold leading-tight text-white md:text-[1.5rem]">{sourceTitle}</h3>
          )}
        </div>
      </div>

      {source.segments.length > 0 ? (
        <div className="mt-5 border-t border-gray-700 pt-5">
          <h4 className="text-sm font-semibold uppercase tracking-[0.16em] text-gray-300">Matched clip segments</h4>
          <ol className="mt-3 grid gap-3">
            {source.segments.map((segment, index) => {
              const clipRange = `${formatTimelineTime(segment.query_start)}–${formatTimelineTime(segment.query_end)}`;
              const vodRange = `${formatTimelineTime(segment.vod_start)}–${formatTimelineTime(segment.vod_end)}`;
              return (
                <li
                  key={`${source.video_id}-${segment.query_start}-${segment.vod_start}`}
                  className="grid gap-2 rounded-lg border border-gray-700 bg-gray-800/70 p-3 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-center"
                >
                  <div className="min-w-0">
                    <p className="text-xs font-semibold uppercase tracking-[0.14em] text-gray-500">Clip {clipRange}</p>
                    {segment.video_url_at_timestamp ? (
                      <a
                        href={segment.video_url_at_timestamp}
                        target="_blank"
                        rel="noreferrer"
                        className="mt-1 inline-flex items-center gap-2 break-all font-semibold text-white transition hover:text-[#fb2844]"
                        aria-label={`Open matched segment ${index + 1} in ${sourceTitle} at ${formatTimelineTime(segment.vod_start)}`}
                      >
                        {vodRange}
                        <ExternalLink className="size-4 shrink-0" />
                      </a>
                    ) : (
                      <p className="mt-1 font-semibold text-white">
                        {vodRange} <span className="font-normal text-gray-400">· Link unavailable</span>
                      </p>
                    )}
                  </div>
                  <span className="text-xs font-medium text-gray-400">Score {segment.score.toFixed(3)}</span>
                </li>
              );
            })}
          </ol>
        </div>
      ) : null}
    </article>
  );
}

export function SearchResultCard({ result, lastSubmittedUrl }: SearchResultCardProps) {
  const sources = getSearchSources(result);

  return (
    <section className="mx-auto max-w-4xl rounded-xl border border-gray-700 bg-gray-900 p-5 text-left shadow-lg">
      <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
        <span
          className={`inline-flex items-center rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-[0.14em] ${
            result.found ? "bg-emerald-500/15 text-emerald-200" : "bg-gray-700 text-gray-200"
          }`}
        >
          {result.found ? "Match found" : "No match found"}
        </span>
      </div>

      {sources.length > 0 ? (
        <div className="grid gap-5">
          {sources.map((source) => (
            <SearchSourceBlock
              key={source.video_id}
              source={source}
              fallbackStreamer={result.streamer}
              fallbackProfileImageUrl={result.profile_image_url}
            />
          ))}
        </div>
      ) : (
        <h3 className="text-lg font-bold leading-tight text-white md:text-[1.5rem]">No matching Twitch VOD found</h3>
      )}

      {lastSubmittedUrl ? (
        <div className="mt-5 border-t border-gray-700 pt-5">
          <p className="text-xs font-semibold uppercase tracking-[0.16em] text-gray-500">TikTok URL</p>
          <a
            href={lastSubmittedUrl}
            target="_blank"
            rel="noreferrer"
            className="mt-1 inline-flex max-w-full items-start gap-2 break-all text-sm text-gray-300 transition hover:text-[#fb2844]"
            aria-label="Open original TikTok clip"
          >
            <span>{lastSubmittedUrl}</span>
            <ExternalLink className="mt-0.5 size-4 shrink-0" />
          </a>
        </div>
      ) : null}
    </section>
  );
}
