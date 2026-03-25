import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
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
        return None
    try:
        with open(CACHE_FILE) as f:
            data = json.load(f)
        if time.time() - data.get("ts", 0) < CACHE_TTL:
            return data["events"]
    except Exception:
        pass
    return None


def save_cache(events):
    with open(CACHE_FILE, "w") as f:
        json.dump({"ts": time.time(), "events": events}, f)


def build_week_days():
    today = datetime.now().date()
    return [(today + timedelta(days=i)).isoformat() for i in range(7)]


@app.route("/")
def index():
    return render_template("index.html", category_colors=CATEGORY_COLORS)


@app.route("/api/events")
def get_events():
    cached = load_cache()
    if cached is not None:
        return jsonify({"events": cached, "cached": True, "week": build_week_days()})
    events = asyncio.run(scrape_all(SOURCES))
    save_cache(events)
    return jsonify({"events": events, "cached": False, "week": build_week_days()})


@app.route("/api/refresh", methods=["POST"])
def refresh_events():
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
    return get_events()


@app.route("/api/sources")
def get_sources():
    return jsonify(SOURCES)


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

    cached = load_cache()
    if cached is not None:
        events, is_cached = cached, True
    else:
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


def nightly_cache_refresh():
    """Scrape all sources and update the cache only if the result looks valid."""
    log.info("Nightly cache refresh starting...")
    try:
        events = asyncio.run(scrape_all(SOURCES))
        if len(events) >= MIN_EVENTS_FOR_VALID_SCRAPE:
            save_cache(events)
            log.info(f"Nightly cache refresh succeeded: {len(events)} events cached.")
        else:
            log.warning(
                f"Nightly refresh returned only {len(events)} events — "
                "cache NOT updated to avoid overwriting good data."
            )
    except Exception as e:
        log.error(f"Nightly cache refresh failed: {e}")


def start_scheduler():
    scheduler = BackgroundScheduler(timezone="America/New_York")
    # Run at 3:00 AM NYC time every night
    scheduler.add_job(
        nightly_cache_refresh,
        trigger=CronTrigger(hour=3, minute=0, timezone="America/New_York"),
        id="nightly_refresh",
        name="Nightly event cache refresh",
        replace_existing=True,
    )
    scheduler.start()
    log.info("Scheduler started — nightly refresh at 3:00 AM ET.")
    return scheduler


# Start scheduler when the app loads (gunicorn imports this module once per worker)
_scheduler = start_scheduler()


if __name__ == "__main__":
    print("Starting Event Aggregator on http://localhost:5050")
    app.run(debug=False, port=5050)
