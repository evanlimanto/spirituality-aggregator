import asyncio
import json
import os
import time
from datetime import datetime, timedelta

from flask import Flask, jsonify, render_template

from config import CATEGORY_COLORS, SOURCES
from scraper import scrape_all

app = Flask(__name__)

CACHE_FILE = "/tmp/event-aggregator-cache.json"
CACHE_TTL = 60 * 60  # 1 hour


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


if __name__ == "__main__":
    print("Starting Event Aggregator on http://localhost:5050")
    app.run(debug=False, port=5050)
