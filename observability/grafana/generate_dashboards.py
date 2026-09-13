"""Generate the versioned VodHunter Grafana dashboard suite."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


OUTPUT_DIR = Path(__file__).resolve().parent

INPUT_DEFINITIONS = {
    "DS_METRICS": {
        "name": "DS_METRICS",
        "label": "Metrics datasource",
        "type": "datasource",
        "pluginId": "prometheus",
        "pluginName": "Prometheus",
    },
    "DS_LOGS": {
        "name": "DS_LOGS",
        "label": "Logs datasource",
        "type": "datasource",
        "pluginId": "loki",
        "pluginName": "Loki",
    },
    "DS_POSTGRES": {
        "name": "DS_POSTGRES",
        "label": "Reporting datasource",
        "type": "datasource",
        "pluginId": "grafana-postgresql-datasource",
        "pluginName": "PostgreSQL",
    },
}

DATASOURCES = {
    "metrics": {"type": "prometheus", "uid": "${DS_METRICS}"},
    "logs": {"type": "loki", "uid": "${DS_LOGS}"},
    "postgres": {"type": "grafana-postgresql-datasource", "uid": "${DS_POSTGRES}"},
}


def prom(expr: str, legend: str = "", ref_id: str = "A") -> dict[str, Any]:
    return {"expr": expr, "legendFormat": legend, "refId": ref_id}


def loki(expr: str, ref_id: str = "A", *, instant: bool = False) -> dict[str, Any]:
    target: dict[str, Any] = {"expr": expr, "refId": ref_id}
    if instant:
        target.update({"queryType": "instant", "instant": True})
    else:
        target["queryType"] = "range"
    return target


def sql(raw_sql: str, ref_id: str = "A", *, time_series: bool = False) -> dict[str, Any]:
    return {
        "editorMode": "code",
        "format": "time_series" if time_series else "table",
        "rawQuery": True,
        "rawSql": raw_sql.strip(),
        "refId": ref_id,
        "sql": {"columns": [], "groupBy": []},
    }


def panel(
    panel_type: str,
    title: str,
    datasource: str,
    targets: list[dict[str, Any]],
    *,
    x: int,
    y: int,
    w: int,
    h: int,
    unit: str = "short",
    description: str = "",
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "type": panel_type,
        "title": title,
        "datasource": DATASOURCES[datasource],
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": targets,
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
    }
    if description:
        result["description"] = description
    if panel_type == "stat":
        result["options"] = {
            "colorMode": "value",
            "graphMode": "area",
            "justifyMode": "auto",
            "orientation": "auto",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "textMode": "auto",
        }
    elif panel_type == "timeseries":
        result["fieldConfig"]["defaults"]["custom"] = {
            "drawStyle": "line",
            "fillOpacity": 12,
            "lineInterpolation": "linear",
            "showPoints": "never",
            "spanNulls": False,
        }
        result["options"] = {
            "legend": {"displayMode": "list", "placement": "bottom"},
            "tooltip": {"mode": "multi"},
        }
    elif panel_type == "logs":
        result["options"] = {
            "enableLogDetails": True,
            "prettifyLogMessage": True,
            "showCommonLabels": False,
            "showLabels": False,
            "showTime": True,
            "sortOrder": "Descending",
            "wrapLines": False,
        }
    elif panel_type == "table":
        result["options"] = {"cellHeight": "sm", "showHeader": True}
    elif panel_type == "piechart":
        result["options"] = {
            "displayLabels": ["name", "percent"],
            "legend": {"displayMode": "table", "placement": "right", "showLegend": True},
            "pieType": "donut",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": True},
        }
    elif panel_type == "bargauge":
        result["options"] = {
            "displayMode": "gradient",
            "minVizHeight": 10,
            "minVizWidth": 0,
            "orientation": "horizontal",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": True},
            "showUnfilled": True,
        }
    elif panel_type == "barchart":
        result["options"] = {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "orientation": "auto",
            "showValue": "auto",
            "stacking": "normal",
            "tooltip": {"mode": "multi"},
        }
    return result


def streamer_variable() -> dict[str, Any]:
    return {
        "allValue": ".*",
        "current": {"selected": True, "text": "All", "value": "$__all"},
        "datasource": DATASOURCES["postgres"],
        "definition": "SELECT streamer AS __text, streamer AS __value FROM grafana_streamer_summary ORDER BY streamer",
        "hide": 0,
        "includeAll": True,
        "label": "Streamer",
        "multi": False,
        "name": "streamer",
        "options": [],
        "query": "SELECT streamer AS __text, streamer AS __value FROM grafana_streamer_summary ORDER BY streamer",
        "refresh": 1,
        "regex": "",
        "skipUrlSync": False,
        "sort": 1,
        "type": "query",
    }


def status_variable() -> dict[str, Any]:
    return {
        "allValue": "%",
        "current": {"selected": True, "text": "All", "value": "$__all"},
        "hide": 0,
        "includeAll": True,
        "label": "VOD status",
        "multi": False,
        "name": "vod_status",
        "options": [],
        "query": "searchable,indexing,reindex_requested,deleted",
        "skipUrlSync": False,
        "type": "custom",
    }


def dashboard(
    *,
    title: str,
    uid: str,
    inputs: list[str],
    panels: list[dict[str, Any]],
    tags: list[str],
    variables: list[dict[str, Any]] | None = None,
    time_from: str = "now-24h",
) -> dict[str, Any]:
    for panel_id, item in enumerate(panels, start=1):
        item["id"] = panel_id
    return {
        "__inputs": [INPUT_DEFINITIONS[name] for name in inputs],
        "annotations": {"list": []},
        "editable": True,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 1,
        "links": [
            {
                "asDropdown": True,
                "icon": "external link",
                "includeVars": True,
                "keepTime": True,
                "tags": ["vodhunter"],
                "targetBlank": False,
                "title": "VodHunter dashboards",
                "type": "dashboards",
            }
        ],
        "panels": panels,
        "refresh": "30s",
        "schemaVersion": 39,
        "tags": ["vodhunter", *tags],
        "templating": {"list": variables or []},
        "time": {"from": time_from, "to": "now"},
        "timepicker": {},
        "timezone": "browser",
        "title": title,
        "uid": uid,
        "version": 1,
    }


def build_overview() -> dict[str, Any]:
    panels = [
        panel("stat", "Searches", "metrics", [prom("sum(increase(vodhunter_searches_total[$__range]))")], x=0, y=0, w=4, h=5),
        panel("stat", "Match rate", "metrics", [prom('sum(increase(vodhunter_searches_total{outcome="match"}[$__range])) / clamp_min(sum(increase(vodhunter_searches_total{outcome=~"match|no_match"}[$__range])), 1)')], x=4, y=0, w=4, h=5, unit="percentunit"),
        panel("stat", "Search latency p95", "metrics", [prom("histogram_quantile(0.95, sum by (le) (rate(vodhunter_search_duration_seconds_bucket[$__rate_interval])))")], x=8, y=0, w=4, h=5, unit="s"),
        panel("stat", "Searchable streamers", "postgres", [sql("SELECT COUNT(*)::double precision AS value FROM grafana_streamer_summary WHERE searchable_vods > 0")], x=12, y=0, w=4, h=5),
        panel("stat", "Active ingests", "postgres", [sql("SELECT COALESCE(SUM(active_vods), 0)::double precision AS value FROM grafana_streamer_summary")], x=16, y=0, w=4, h=5),
        panel("stat", "Stalled ingests", "postgres", [sql("SELECT COALESCE(SUM(stalled_vods), 0)::double precision AS value FROM grafana_streamer_summary")], x=20, y=0, w=4, h=5, description="Indexing VODs whose cursor has not updated for ten minutes, or which have no cursor."),
        panel("timeseries", "Search outcomes", "metrics", [prom("sum by (outcome) (rate(vodhunter_searches_total[$__rate_interval]))", "{{outcome}}")], x=0, y=5, w=12, h=8, unit="reqps"),
        panel("table", "Streamer inventory", "postgres", [sql("""
            SELECT streamer AS "Streamer", total_vods AS "VODs",
                   searchable_vods AS "Complete", active_vods AS "Active",
                   stalled_vods AS "Stalled", reindex_requested_vods AS "Reindex",
                   newest_vod_at AS "Newest VOD", last_ingest_update AS "Last ingest activity"
            FROM grafana_streamer_summary
            ORDER BY stalled_vods DESC, active_vods DESC, streamer
        """)], x=12, y=5, w=12, h=8),
        panel("logs", "Recent errors across VodHunter", "logs", [loki('{service_name=~"vodhunter-api|vodhunter-worker|vodhunter-retention"} |~ "(?i)error|failed|exception"')], x=0, y=13, w=24, h=9),
    ]
    return dashboard(title="00 - VodHunter Overview", uid="vodhunter-overview", inputs=["DS_METRICS", "DS_LOGS", "DS_POSTGRES"], panels=panels, tags=["overview"])


def build_search_performance() -> dict[str, Any]:
    panels = [
        panel("stat", "Searches", "metrics", [prom("sum(increase(vodhunter_searches_total[$__range]))")], x=0, y=0, w=6, h=5),
        panel("stat", "Application errors", "metrics", [prom('sum(increase(vodhunter_searches_total{outcome="error"}[$__range]))')], x=6, y=0, w=6, h=5),
        panel("stat", "Latency p50", "metrics", [prom("histogram_quantile(0.50, sum by (le) (rate(vodhunter_search_duration_seconds_bucket[$__rate_interval])))")], x=12, y=0, w=6, h=5, unit="s"),
        panel("stat", "Latency p95", "metrics", [prom("histogram_quantile(0.95, sum by (le) (rate(vodhunter_search_duration_seconds_bucket[$__rate_interval])))")], x=18, y=0, w=6, h=5, unit="s"),
        panel("timeseries", "Search throughput by outcome", "metrics", [prom("sum by (outcome) (rate(vodhunter_searches_total[$__rate_interval]))", "{{outcome}}")], x=0, y=5, w=12, h=8, unit="reqps"),
        panel("timeseries", "Total latency p50 / p95", "metrics", [
            prom("histogram_quantile(0.50, sum by (le) (rate(vodhunter_search_duration_seconds_bucket[$__rate_interval])))", "p50", "A"),
            prom("histogram_quantile(0.95, sum by (le) (rate(vodhunter_search_duration_seconds_bucket[$__rate_interval])))", "p95", "B"),
        ], x=12, y=5, w=12, h=8, unit="s"),
        panel("timeseries", "Pipeline stage latency p95", "metrics", [prom("histogram_quantile(0.95, sum by (le, stage) (rate(vodhunter_search_stage_duration_seconds_bucket[$__rate_interval])))", "{{stage}}")], x=0, y=13, w=12, h=8, unit="s"),
        panel("bargauge", "Failures by code", "metrics", [prom("sum by (error_code) (increase(vodhunter_search_failures_total[$__range]))", "{{error_code}}")], x=12, y=13, w=12, h=8),
        panel("logs", "Recent completed searches", "logs", [loki('{service_name="vodhunter-api", event="search_finished"} | json')], x=0, y=21, w=16, h=10),
        panel("logs", "HTTP and application errors", "logs", [loki('{service_name="vodhunter-api"} |~ "\\\"status_code\\\":[45][0-9][0-9]|\\\"level\\\":\\\"error\\\""')], x=16, y=21, w=8, h=10),
    ]
    return dashboard(title="10 - Search Performance", uid="vodhunter-search-performance", inputs=["DS_METRICS", "DS_LOGS"], panels=panels, tags=["search", "performance"])


def build_search_quality() -> dict[str, Any]:
    where = "$__timeFilter(created_at) AND ('${streamer}' = '.*' OR streamer = '${streamer}')"
    panels = [
        panel("stat", "Completed searches", "postgres", [sql(f"SELECT COUNT(*)::double precision AS value FROM grafana_search_quality WHERE {where} AND job_status = 'completed'")], x=0, y=0, w=6, h=5),
        panel("stat", "Match rate", "postgres", [sql(f"SELECT COALESCE(COUNT(*) FILTER (WHERE outcome = 'match')::double precision / NULLIF(COUNT(*) FILTER (WHERE outcome IN ('match', 'no_match')), 0), 0) AS value FROM grafana_search_quality WHERE {where}")], x=6, y=0, w=6, h=5, unit="percentunit"),
        panel("stat", "Average match score", "postgres", [sql(f"SELECT COALESCE(AVG(score), 0)::double precision AS value FROM grafana_search_quality WHERE {where} AND outcome = 'match'")], x=12, y=0, w=6, h=5),
        panel("stat", "Average candidates", "postgres", [sql(f"SELECT COALESCE(AVG(candidate_count), 0)::double precision AS value FROM grafana_search_quality WHERE {where}")], x=18, y=0, w=6, h=5),
        panel("timeseries", "Outcomes over time", "postgres", [sql(f"SELECT $__timeGroupAlias(created_at, $__interval), outcome AS metric, COUNT(*)::double precision AS value FROM grafana_search_quality WHERE {where} GROUP BY 1, outcome ORDER BY 1", time_series=True)], x=0, y=5, w=12, h=8),
        panel("table", "Quality by streamer", "postgres", [sql(f"""
            SELECT streamer AS "Streamer", COUNT(*) AS "Searches",
                   COUNT(*) FILTER (WHERE outcome = 'match') AS "Matches",
                   ROUND(100.0 * COUNT(*) FILTER (WHERE outcome = 'match') / NULLIF(COUNT(*) FILTER (WHERE outcome IN ('match', 'no_match')), 0), 1) AS "Match %",
                   ROUND(AVG(score) FILTER (WHERE outcome = 'match')::numeric, 3) AS "Avg score"
            FROM grafana_search_quality WHERE {where}
            GROUP BY streamer ORDER BY "Searches" DESC
        """)], x=12, y=5, w=12, h=8),
        panel("barchart", "Match score distribution", "postgres", [sql(f"SELECT FLOOR(score * 10) / 10.0 AS bucket, COUNT(*)::double precision AS searches FROM grafana_search_quality WHERE {where} AND outcome = 'match' AND score IS NOT NULL GROUP BY 1 ORDER BY 1")], x=0, y=13, w=10, h=8),
        panel("table", "No-match and failure reasons", "postgres", [sql(f"""
            SELECT outcome AS "Outcome",
                   COALESCE(error_code, result_reason, 'unspecified') AS "Reason",
                   COUNT(*) AS "Count"
            FROM grafana_search_quality
            WHERE {where} AND outcome <> 'match'
            GROUP BY outcome, COALESCE(error_code, result_reason, 'unspecified')
            ORDER BY "Count" DESC
        """)], x=10, y=13, w=14, h=8),
        panel("table", "Recent match decisions", "postgres", [sql(f"""
            SELECT created_at AS "Time", search_id AS "Search ID", streamer AS "Streamer",
                   outcome AS "Outcome", ROUND(score::numeric, 3) AS "Score",
                   candidate_count AS "Candidates", segment_count AS "Segments",
                   matched_video_title AS "Matched VOD", matched_video_url AS "VOD URL",
                   total_duration_ms AS "Total ms", model_cold_start AS "Cold start"
            FROM grafana_search_quality WHERE {where}
            ORDER BY created_at DESC LIMIT 200
        """)], x=0, y=21, w=24, h=10),
    ]
    return dashboard(title="10 - Search Quality", uid="vodhunter-search-quality", inputs=["DS_POSTGRES"], panels=panels, tags=["search", "quality"], variables=[streamer_variable()], time_from="now-7d")


def build_streamers_vods() -> dict[str, Any]:
    streamer_filter = "('${streamer}' = '.*' OR streamer = '${streamer}')"
    vod_filter = f"{streamer_filter} AND status LIKE '${{vod_status}}'"
    panels = [
        panel("stat", "Streamers", "postgres", [sql(f"SELECT COUNT(*)::double precision AS value FROM grafana_streamer_summary WHERE {streamer_filter}")], x=0, y=0, w=4, h=5),
        panel("stat", "Retained VODs", "postgres", [sql(f"SELECT COUNT(*)::double precision AS value FROM grafana_vod_inventory WHERE {vod_filter}")], x=4, y=0, w=4, h=5),
        panel("stat", "Complete", "postgres", [sql(f"SELECT COUNT(*)::double precision AS value FROM grafana_vod_inventory WHERE {streamer_filter} AND status = 'searchable'")], x=8, y=0, w=4, h=5),
        panel("stat", "In progress", "postgres", [sql(f"SELECT COUNT(*)::double precision AS value FROM grafana_vod_inventory WHERE {streamer_filter} AND operational_status = 'in_progress'")], x=12, y=0, w=4, h=5),
        panel("stat", "Stalled", "postgres", [sql(f"SELECT COUNT(*)::double precision AS value FROM grafana_vod_inventory WHERE {streamer_filter} AND operational_status = 'stalled'")], x=16, y=0, w=4, h=5),
        panel("stat", "Reindex requested", "postgres", [sql(f"SELECT COUNT(*)::double precision AS value FROM grafana_vod_inventory WHERE {streamer_filter} AND status = 'reindex_requested'")], x=20, y=0, w=4, h=5),
        panel("table", "Streamer summary", "postgres", [sql(f"""
            SELECT profile_image_url AS "Avatar", streamer AS "Streamer", streamer_url AS "Twitch",
                   total_vods AS "VODs", searchable_vods AS "Complete", active_vods AS "Active",
                   stalled_vods AS "Stalled", reindex_requested_vods AS "Reindex",
                   newest_vod_at AS "Newest VOD", last_ingest_update AS "Last activity"
            FROM grafana_streamer_summary WHERE {streamer_filter}
            ORDER BY stalled_vods DESC, active_vods DESC, streamer
        """)], x=0, y=5, w=16, h=9),
        panel("piechart", "VOD lifecycle", "postgres", [sql(f"SELECT status AS metric, COUNT(*)::double precision AS value FROM grafana_vod_inventory WHERE {vod_filter} GROUP BY status ORDER BY status")], x=16, y=5, w=8, h=9),
        panel("table", "VOD inventory", "postgres", [sql(f"""
            SELECT thumbnail_url AS "Thumbnail", streamer AS "Streamer", title AS "Title",
                   vod_platform_id AS "Twitch VOD", vod_url AS "VOD URL", streamed_at AS "Streamed at",
                   status AS "Lifecycle", operational_status AS "Operational health",
                   ROUND(progress_percent::numeric, 1) AS "Progress %",
                   last_ingested_seconds AS "Ingested seconds",
                   last_seen_duration_seconds AS "Observed seconds",
                   last_ingest_update AS "Last progress", cursor_age_seconds AS "Cursor age seconds"
            FROM grafana_vod_inventory
            WHERE {vod_filter}
              AND (streamed_at IS NULL OR $__timeFilter(streamed_at))
            ORDER BY streamed_at DESC NULLS LAST, video_id DESC LIMIT 500
        """)], x=0, y=14, w=24, h=13, description="Complete VODs have no cursor row because finalization deletes resumable ingest state; their lifecycle status is authoritative."),
    ]
    return dashboard(title="20 - Streamers & VODs", uid="vodhunter-streamers-vods", inputs=["DS_POSTGRES"], panels=panels, tags=["content", "inventory"], variables=[streamer_variable(), status_variable()], time_from="now-30d")


def build_ingestion_operations() -> dict[str, Any]:
    streamer_filter = "('${streamer}' = '.*' OR streamer = '${streamer}')"
    log_streamer = ' | regexp "streamer=(?P<streamer>[^ ]+)" | streamer=~"${streamer}"'
    panels = [
        panel("stat", "Worker log lines", "logs", [loki('sum(count_over_time({service_name="vodhunter-worker"}[$__range]))', instant=True)], x=0, y=0, w=4, h=5, description="Activity in the selected range; this is not a heartbeat because watch mode intentionally logs infrequently."),
        panel("stat", "Active VODs", "postgres", [sql(f"SELECT COUNT(*)::double precision AS value FROM grafana_vod_inventory WHERE {streamer_filter} AND operational_status = 'in_progress'")], x=4, y=0, w=4, h=5),
        panel("stat", "Stalled VODs", "postgres", [sql(f"SELECT COUNT(*)::double precision AS value FROM grafana_vod_inventory WHERE {streamer_filter} AND operational_status = 'stalled'")], x=8, y=0, w=4, h=5),
        panel("stat", "Reindex queue", "postgres", [sql(f"SELECT COUNT(*)::double precision AS value FROM grafana_vod_inventory WHERE {streamer_filter} AND status = 'reindex_requested'")], x=12, y=0, w=4, h=5),
        panel("stat", "Failures", "logs", [loki(f'sum(count_over_time({{service_name="vodhunter-worker", action="failed"}}{log_streamer} [$__range]))', instant=True)], x=16, y=0, w=4, h=5),
        panel("stat", "Handoffs", "logs", [loki(f'sum(count_over_time({{service_name="vodhunter-worker", action="handoff"}}{log_streamer} [$__range]))', instant=True)], x=20, y=0, w=4, h=5),
        panel("table", "Current ingest cursors", "postgres", [sql(f"""
            SELECT streamer AS "Streamer", title AS "VOD", vod_url AS "VOD URL",
                   operational_status AS "Health", ROUND(progress_percent::numeric, 1) AS "Progress %",
                   last_ingested_seconds AS "Ingested seconds",
                   last_seen_duration_seconds AS "Observed seconds",
                   last_ingest_update AS "Last update", cursor_age_seconds AS "Cursor age seconds"
            FROM grafana_vod_inventory
            WHERE {streamer_filter} AND status = 'indexing'
            ORDER BY operational_status DESC, last_ingest_update DESC NULLS LAST
        """)], x=0, y=5, w=14, h=9),
        panel("logs", "Worker mode and handoff events", "logs", [loki(f'{{service_name="vodhunter-worker"}} |~ "mode=|handoff"{log_streamer}')], x=14, y=5, w=10, h=9),
        panel("timeseries", "Chunks started per second", "logs", [loki('sum(rate({service_name="vodhunter-worker", action="processing"}[$__rate_interval]))')], x=0, y=14, w=8, h=8, unit="ops"),
        panel("timeseries", "Worker failures", "logs", [loki('sum by (mode) (count_over_time({service_name="vodhunter-worker", action="failed"}[$__interval]))')], x=8, y=14, w=8, h=8),
        panel("timeseries", "Live/backlog handoffs", "logs", [loki('sum by (event) (count_over_time({service_name="vodhunter-worker", action="handoff"}[$__interval]))')], x=16, y=14, w=8, h=8),
        panel("logs", "Recent ingestion logs", "logs", [loki(f'{{service_name="vodhunter-worker"}}{log_streamer}')], x=0, y=22, w=24, h=11),
    ]
    return dashboard(title="20 - Ingestion Operations", uid="vodhunter-ingestion-operations", inputs=["DS_LOGS", "DS_POSTGRES"], panels=panels, tags=["content", "ingestion"], variables=[streamer_variable()], time_from="now-6h")


def build_index_retention() -> dict[str, Any]:
    streamer_filter = "('${streamer}' = '.*' OR streamer = '${streamer}')"
    panels = [
        panel("stat", "Estimated embeddings", "postgres", [sql(f"SELECT COALESCE(SUM(estimated_embeddings), 0)::double precision AS value FROM grafana_index_partitions WHERE {streamer_filter}")], x=0, y=0, w=5, h=5),
        panel("stat", "Index storage", "postgres", [sql(f"SELECT COALESCE(SUM(total_bytes), 0)::double precision AS value FROM grafana_index_partitions WHERE {streamer_filter}")], x=5, y=0, w=5, h=5, unit="bytes"),
        panel("stat", "Missing HNSW indexes", "postgres", [sql(f"SELECT COUNT(*)::double precision AS value FROM grafana_index_partitions WHERE {streamer_filter} AND NOT has_hnsw_index")], x=10, y=0, w=5, h=5),
        panel("stat", "Expiring in 7 days", "postgres", [sql(f"SELECT COUNT(*)::double precision AS value FROM grafana_retention_inventory WHERE {streamer_filter} AND age_days >= $retention_days - 7 AND age_days < $retention_days AND status <> 'indexing' AND NOT has_active_cursor")], x=15, y=0, w=5, h=5),
        panel("stat", "Expired but protected", "postgres", [sql(f"SELECT COUNT(*)::double precision AS value FROM grafana_retention_inventory WHERE {streamer_filter} AND age_days >= $retention_days AND (status = 'indexing' OR has_active_cursor)")], x=20, y=0, w=4, h=5),
        panel("table", "Creator index partitions", "postgres", [sql(f"""
            SELECT streamer AS "Streamer", partition_name AS "Partition",
                   estimated_embeddings AS "Estimated embeddings", total_bytes AS "Storage bytes",
                   has_hnsw_index AS "HNSW ready", embedding_dim AS "Dimensions",
                   model_version AS "Model", preprocessing_version AS "Preprocessing",
                   metadata_updated_at AS "Metadata updated"
            FROM grafana_index_partitions WHERE {streamer_filter}
            ORDER BY streamer
        """)], x=0, y=5, w=15, h=10),
        panel("piechart", "VOD lifecycle", "postgres", [sql(f"SELECT status AS metric, COUNT(*)::double precision AS value FROM grafana_vod_inventory WHERE {streamer_filter} GROUP BY status ORDER BY status")], x=15, y=5, w=9, h=10),
        panel("table", "Index consistency checks", "postgres", [sql(f"""
            SELECT check_name AS "Check", affected AS "Affected"
            FROM (
                SELECT 'Indexing VOD without cursor' AS check_name, COUNT(*)::BIGINT AS affected
                FROM grafana_vod_inventory WHERE {streamer_filter} AND status = 'indexing' AND last_ingest_update IS NULL
                UNION ALL
                SELECT 'Stalled ingest cursor', COUNT(*)::BIGINT
                FROM grafana_vod_inventory WHERE {streamer_filter} AND operational_status = 'stalled'
                UNION ALL
                SELECT 'Creator partition missing HNSW', COUNT(*)::BIGINT
                FROM grafana_index_partitions WHERE {streamer_filter} AND NOT has_hnsw_index
            ) AS checks ORDER BY check_name
        """)], x=0, y=15, w=9, h=8),
        panel("table", "Upcoming retention candidates", "postgres", [sql(f"""
            SELECT streamer AS "Streamer", title AS "VOD", vod_url AS "VOD URL",
                   streamed_at AS "Streamed at", ROUND(age_days::numeric, 1) AS "Age days",
                   ROUND(($retention_days - age_days)::numeric, 1) AS "Days remaining", status AS "Status"
            FROM grafana_retention_inventory
            WHERE {streamer_filter} AND age_days >= $retention_days - 7
              AND status <> 'indexing' AND NOT has_active_cursor
            ORDER BY age_days DESC LIMIT 200
        """)], x=9, y=15, w=15, h=8),
        panel("logs", "Retention service activity", "logs", [loki('{service_name="vodhunter-retention"}')], x=0, y=23, w=24, h=9),
    ]
    retention_variable = {
        "current": {"selected": True, "text": "30", "value": "30"},
        "hide": 0,
        "label": "Retention days",
        "name": "retention_days",
        "options": [{"selected": True, "text": "30", "value": "30"}],
        "query": "30",
        "skipUrlSync": False,
        "type": "constant",
    }
    return dashboard(title="30 - Index & Retention", uid="vodhunter-index-retention", inputs=["DS_POSTGRES", "DS_LOGS"], panels=panels, tags=["index", "retention"], variables=[streamer_variable(), retention_variable], time_from="now-30d")


def dashboards() -> dict[str, dict[str, Any]]:
    return {
        "00-vodhunter-overview.json": build_overview(),
        "10-search-performance.json": build_search_performance(),
        "10-search-quality.json": build_search_quality(),
        "20-streamers-vods.json": build_streamers_vods(),
        "20-ingestion-operations.json": build_ingestion_operations(),
        "30-index-retention.json": build_index_retention(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail when generated dashboards are stale")
    args = parser.parse_args()

    stale: list[str] = []
    for filename, payload in dashboards().items():
        path = OUTPUT_DIR / filename
        rendered = json.dumps(payload, indent=2, sort_keys=False) + "\n"
        if args.check:
            if not path.exists() or path.read_text() != rendered:
                stale.append(filename)
        else:
            path.write_text(rendered)

    if stale:
        raise SystemExit(f"stale generated dashboards: {', '.join(stale)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
