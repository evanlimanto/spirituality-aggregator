import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, render_template, request

from config import CATEGORY_COLORS, SOURCES
from scraper import scrape_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)

CACHE_FILE = "/tmp/event-aggregator-cache.json"
CACHE_TTL = 60 * 60  # 1 hour
MIN_EVENTS_FOR_VALID_SCRAPE = 5  # don't overwrite cache with an empty/broken result


def load_cache():
    if not os.path.exists(CACHE_FILE):
        return None, None
    try:
        with open(CACHE_FILE) as f:
            data = json.load(f)
        if time.time() - data.get("ts", 0) < CACHE_TTL:
            return data["events"], data.get("ts")
    except Exception:
        pass
    return None, None


def save_cache(events):
    with open(CACHE_FILE, "w") as f:
        json.dump({"ts": time.time(), "events": events}, f)


def build_week_days():
    today = datetime.now(tz=ZoneInfo("America/New_York")).date()
    return [(today + timedelta(days=i)).isoformat() for i in range(7)]


@app.route("/")
def index():
    return render_template("index.html", category_colors=CATEGORY_COLORS)


@app.route("/api/events")
def get_events():
    cached, ts = load_cache()
    if cached is not None:
        return jsonify({"events": cached, "cached": True, "scraped_at": ts, "week": build_week_days()})
    ts = time.time()
    events = asyncio.run(scrape_all(SOURCES))
    save_cache(events)
    return jsonify({"events": events, "cached": False, "scraped_at": ts, "week": build_week_days()})


@app.route("/api/refresh", methods=["POST"])
def refresh_events():
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
    return get_events()


@app.route("/api/sources")
def get_sources():
    return jsonify(SOURCES)


@app.route("/api/sources/query", methods=["GET", "POST"])
def query_sources():
    """LLM-friendly sources endpoint. Returns all scraped sources with metadata.

    Optional POST body filters:
      { "category": "Yoga", "status": "active" }

    Response:
      {
        "sources": [ { name, url, category, status, note } ],
        "meta": { "total": 15, "categories": [...] }
      }
    """
    body = request.get_json(silent=True) or {}
    category_filter = (body.get("category") or request.args.get("category") or "").strip().lower()
    status_filter = (body.get("status") or request.args.get("status") or "").strip().lower()

    sources = SOURCES
    if category_filter:
        sources = [s for s in sources if s["category"].lower() == category_filter]
    if status_filter:
        sources = [s for s in sources if s["status"].lower() == status_filter]

    return jsonify({
        "sources": sources,
        "meta": {
            "total": len(sources),
            "categories": sorted({s["category"] for s in SOURCES}),
        },
    })


@app.route("/api/query", methods=["POST"])
def query_events():
    """LLM-friendly endpoint. Accepts optional JSON filters and returns a
    structured payload suitable for embedding in a tool-call response.

    Request body (all fields optional):
      {
        "category": "Yoga",          // filter by category name (case-insensitive)
        "date": "2026-03-25",        // filter to a specific ISO date
        "include_sources": true      // also return source metadata (default false)
      }

    Response:
      {
        "events": [...],
        "sources": [...],            // only if include_sources=true
        "meta": { "total": 42, "cached": true, "week": ["2026-03-25", ...] }
      }
    """
    body = request.get_json(silent=True) or {}
    category_filter = (body.get("category") or "").strip().lower()
    date_filter = (body.get("date") or "").strip()
    include_sources = bool(body.get("include_sources", False))

    cached, ts = load_cache()
    if cached is not None:
        events, is_cached = cached, True
    else:
        ts = time.time()
        events = asyncio.run(scrape_all(SOURCES))
        save_cache(events)
        is_cached = False

    filtered = events
    if category_filter:
        filtered = [e for e in filtered if e.get("category", "").lower() == category_filter]
    if date_filter:
        filtered = [e for e in filtered if e.get("date") == date_filter]

    payload = {
        "events": filtered,
        "meta": {
            "total": len(filtered),
            "cached": is_cached,
            "week": build_week_days(),
        },
    }
    if include_sources:
        payload["sources"] = SOURCES

    return jsonify(payload)


if __name__ == "__main__":
    print("Starting Event Aggregator on http://localhost:5050")
    app.run(debug=False, port=5050)
