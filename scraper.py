"""
Rule-based event extractor.

Generic strategies (tried for all sites):
  1. JSON-LD Schema.org Event markup
  2. HTML microdata Schema.org Event
  3. WordPress The Events Calendar plugin
  4. Squarespace event lists
  5. <time datetime="..."> elements near headings

Site-specific extractors (keyed by domain substring):
  - yogamaya.com        → .event-wrapper CSS classes
  - kinlia.com          → EventCard_container DOM
  - thus.org            → Shopify product-item with pipe/bullet date
  - kulayoga.com        → text-block: title / teacher / "Sat, 3/28/26"
  - ohmcenter.com       → prose lines: "Month D, Day time: Title"
  - eventbrite.com      → handled by JSON-LD fix (ListItem.item)
  - satsangnyc.com      → JSON API at /api/events (UTC datetimes)
  - bhaktischoolnyc.com → Wix data-hook="events-card" attributes
  - groupmuse.com       → .card-content divs; NYC filtered by EDT/EST timezone
  - premabrooklyn.com   → Squarespace fluid-engine text-block parsing
  - 113spring.com       → Shopify products.json; dates from "Offered on..." body
  - lifeshopny.com      → Wix data-hook="event-title"/"event-full-date" (SSR single-event)
  - ishtayoga.com       → Squarespace eventlist-event; start time from event-time-localized-start
"""

import asyncio
import json
import re
from datetime import datetime, timedelta, date as Date
from html import unescape
from typing import Optional
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateutil_parser

NYC_TZ = ZoneInfo("America/New_York")
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}

CONCURRENCY = 15  # all sources in parallel

# ── Week helpers ─────────────────────────────────────────────────────────────

def _today() -> Date:
    return datetime.now(tz=NYC_TZ).date()

def _week_end() -> Date:
    return _today() + timedelta(days=14)

def in_week(d: Date) -> bool:
    return _today() <= d <= _week_end()

def next_weekday(dow: int) -> Date:
    """Next occurrence of weekday (0=Mon…6=Sun), including today."""
    t = _today()
    return t + timedelta(days=(dow - t.weekday()) % 7)


# ── Lookup tables ─────────────────────────────────────────────────────────────

MONTH_MAP = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}

DOW_MAP = {
    "monday": 0, "mon": 0, "tuesday": 1, "tue": 1, "tues": 1,
    "wednesday": 2, "wed": 2, "thursday": 3, "thu": 3, "thur": 3, "thurs": 3,
    "friday": 4, "fri": 4, "saturday": 5, "sat": 5, "sunday": 6, "sun": 6,
}


# ── Regex helpers ─────────────────────────────────────────────────────────────

TIME_RE = re.compile(
    r"(?<!\d)(\d{1,2})(?::(\d{2}))?\s*(am|pm)"
    r"(?:\s*[-–—to/]+\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm))?",
    re.IGNORECASE,
)

# Handles "7-8:00pm" or "7-8pm" ranges where only the end has am/pm
TIME_RANGE_NO_AMPM_RE = re.compile(
    r"(?<!\d)(\d{1,2})(?::(\d{2}))?\s*[-–—]\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)",
    re.IGNORECASE,
)

DATE_RE = re.compile(
    r"(?:"
    r"(\d{4})-(\d{2})-(\d{2})"                               # ISO YYYY-MM-DD
    r"|(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?"                   # M/D or M/D/Y
    r"|(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?"
    r"|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?"
    r"|nov(?:ember)?|dec(?:ember)?)\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s*(\d{4}))?"
    r"|(\d{1,2})(?:st|nd|rd|th)?\s+"
    r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?"
    r"|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?"
    r"|nov(?:ember)?|dec(?:ember)?)(?:,?\s*(\d{4}))?(?!\s*\w)"
    r")",
    re.IGNORECASE,
)

DOW_ONLY_RE = re.compile(
    r"^\s*(monday|tuesday|wednesday|thursday|friday|saturday|sunday|"
    r"mon|tue|tues|wed|thu|thur|thurs|fri|sat|sun)s?\s*$",
    re.IGNORECASE,
)


def parse_time(text: str) -> Optional[str]:
    # Try "7-8:00pm" range first (only end has am/pm)
    mr = TIME_RANGE_NO_AMPM_RE.search(text)
    if mr:
        h1, m1, h2, m2, ap = mr.groups()
        t1 = f"{int(h1)}:{m1 or '00'} {ap.upper()}"
        t2 = f"{int(h2)}:{m2 or '00'} {ap.upper()}"
        return f"{t1} – {t2}"
    m = TIME_RE.search(text)
    if not m:
        return None
    h1, m1, ap1, h2, m2, ap2 = m.groups()
    t1 = f"{int(h1)}:{m1 or '00'} {ap1.upper()}"
    if h2:
        t2 = f"{int(h2)}:{m2 or '00'} {ap2.upper()}"
        return f"{t1} – {t2}"
    return t1


def parse_date(text: str) -> Optional[Date]:
    if not text:
        return None
    m = DATE_RE.search(text)
    if not m:
        return None
    g = m.groups()
    yr = datetime.now().year

    if g[0]:   # ISO
        try:
            return Date(int(g[0]), int(g[1]), int(g[2]))
        except ValueError:
            return None

    if g[3]:   # M/D(/Y)
        month, day = int(g[3]), int(g[4])
        year = int(g[5]) if g[5] else yr
        if year < 100:
            year += 2000
        try:
            return Date(year, month, day)
        except ValueError:
            return None

    if g[6]:   # Month D(, YYYY)
        month = MONTH_MAP.get(g[6].lower()[:3])
        if not month:
            return None
        try:
            year = int(g[8]) if g[8] else yr
            d = Date(year, month, int(g[7]))
            if not g[8] and d < _today():
                d = Date(yr + 1, month, int(g[7]))
            return d
        except ValueError:
            return None

    if g[9]:   # D Month(, YYYY)
        month = MONTH_MAP.get(g[10].lower()[:3]) if g[10] else None
        if not month:
            return None
        try:
            year = int(g[11]) if g[11] else yr
            d = Date(year, month, int(g[9]))
            if not g[11] and d < _today():
                d = Date(yr + 1, month, int(g[9]))
            return d
        except ValueError:
            return None

    return None


def parse_datetime_attr(s: str):
    try:
        dt = dateutil_parser.isoparse(s)
        t = dt.strftime("%-I:%M %p") if (dt.hour or dt.minute) else None
        return dt.date(), t
    except Exception:
        return None, None


_TITLE_BLACKLIST = re.compile(
    r"^(our most popular|follow a manual|view event|view schedule|book|sign in|"
    r"more info|as well as|join us|skip to|click here|open menu|close menu)",
    re.IGNORECASE,
)


def clean_description(text: str, max_chars: int = 300) -> Optional[str]:
    """Strip HTML tags, decode entities, and truncate at a sentence boundary."""
    if not text:
        return None
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None
    if len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    last_end = max(truncated.rfind("."), truncated.rfind("!"), truncated.rfind("?"))
    if last_end > max_chars // 2:
        return text[: last_end + 1]
    return truncated.rstrip() + "\u2026"


def make_event(title, date_obj, time_str=None, description=None,
               url=None, location=None, source_url=None):
    if not title or not date_obj or not in_week(date_obj):
        return None
    title = title.strip()
    if len(title) < 4:
        return None
    # Filter obviously noisy titles
    if _TITLE_BLACKLIST.match(title):
        return None
    # Filter sentence fragments that start lowercase (prose continuation)
    if title[0].islower():
        return None
    return {
        "title": title[:120],
        "date": date_obj.isoformat(),
        "time": time_str,
        "description": clean_description(description) if description else None,
        "event_url": url or source_url,
        "location": location or None,
    }


# ── Generic: JSON-LD ──────────────────────────────────────────────────────────

def _parse_schema_event(item: dict, source_url: str) -> Optional[dict]:
    name = item.get("name") or ""
    start = item.get("startDate") or ""
    description = item.get("description") or ""
    url = item.get("url") or item.get("@id") or source_url

    loc = item.get("location") or {}
    if isinstance(loc, dict):
        location = (loc.get("name") or
                    (loc.get("address") or {}).get("streetAddress") or None)
    elif isinstance(loc, str):
        location = loc
    else:
        location = None

    if not start:
        return None
    d, t = parse_datetime_attr(start)
    if not d:
        d, t = parse_date(start), parse_time(start)
    if not d:
        return None

    return make_event(title=name, date_obj=d, time_str=t,
                      description=description, url=url,
                      location=location, source_url=source_url)


def extract_jsonld(soup: BeautifulSoup, source_url: str) -> list[dict]:
    events = []

    def process(item):
        if not isinstance(item, dict):
            return
        t = item.get("@type", "")
        types = t if isinstance(t, list) else [t]

        if "Event" in types:
            evt = _parse_schema_event(item, source_url)
            if evt:
                events.append(evt)

        # Recurse into @graph
        for sub in item.get("@graph", []):
            process(sub)

        # Recurse into itemListElement (handles Eventbrite's ListItem.item pattern)
        for elem in item.get("itemListElement", []):
            if isinstance(elem, dict):
                # ListItem wrapping an Event
                inner = elem.get("item", elem)
                process(inner)

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except Exception:
            continue
        if isinstance(data, list):
            for item in data:
                process(item)
        else:
            process(data)

    return events


# ── Generic: microdata ────────────────────────────────────────────────────────

def extract_microdata(soup: BeautifulSoup, source_url: str) -> list[dict]:
    events = []
    for el in soup.find_all(attrs={"itemtype": re.compile(r"schema\.org/Event", re.I)}):
        def prop(name):
            f = el.find(attrs={"itemprop": name})
            if not f:
                return None
            return f.get("content") or f.get("datetime") or f.get_text(strip=True)

        start, name = prop("startDate"), prop("name")
        if not start or not name:
            continue
        d, t = parse_datetime_attr(start)
        if not d:
            d, t = parse_date(start), parse_time(start)
        if not d:
            continue
        evt = make_event(title=name, date_obj=d, time_str=t,
                         description=prop("description"),
                         url=prop("url") or source_url,
                         location=prop("location"), source_url=source_url)
        if evt:
            events.append(evt)
    return events


# ── Generic: WordPress The Events Calendar ────────────────────────────────────

def extract_tribe_events(soup: BeautifulSoup, source_url: str) -> list[dict]:
    events = []
    for article in soup.find_all("article", class_=re.compile(r"tribe.event", re.I)):
        title_el = (article.find(class_=re.compile(r"tribe-event.*title|tribe-events.*title", re.I))
                    or article.find(["h1", "h2", "h3", "h4"]))
        start_el = (article.find("abbr", class_=re.compile(r"tribe-events-abbr", re.I))
                    or article.find(["time", "abbr"]))
        dt_str = ((start_el.get("title") or start_el.get("datetime") or
                   start_el.get_text(strip=True)) if start_el else "")
        title = title_el.get_text(strip=True) if title_el else ""
        d = parse_date(dt_str)
        t = parse_time(dt_str)
        url_el = article.find("a", href=True)
        evt = make_event(title=title, date_obj=d, time_str=t,
                         url=(url_el["href"] if url_el else source_url),
                         source_url=source_url)
        if evt:
            events.append(evt)
    return events


# ── Generic: Squarespace event lists ─────────────────────────────────────────

def extract_squarespace(soup: BeautifulSoup, source_url: str) -> list[dict]:
    events = []
    for el in soup.find_all(class_=re.compile(
            r"eventlist-event|summary-item--event|event-item", re.I)):
        title_el = (el.find(class_=re.compile(r"eventlist-title|summary-title|event-title", re.I))
                    or el.find(["h1", "h2", "h3", "h4"]))
        title = title_el.get_text(strip=True) if title_el else ""
        time_el = el.find("time")
        d, t = (parse_datetime_attr(time_el.get("datetime", ""))
                if time_el else (None, None))
        if not d:
            raw = time_el.get_text(strip=True) if time_el else ""
            d, t = parse_date(raw), parse_time(raw)
        url_el = el.find("a", href=True)
        evt = make_event(title=title, date_obj=d, time_str=t,
                         url=(url_el["href"] if url_el else source_url),
                         source_url=source_url)
        if evt:
            events.append(evt)
    return events


# ── Generic: <time datetime="..."> near headings ──────────────────────────────

def extract_time_elements(soup: BeautifulSoup, source_url: str) -> list[dict]:
    events = []
    for time_el in soup.find_all("time", datetime=True):
        d, t = parse_datetime_attr(time_el.get("datetime", ""))
        if not d or not in_week(d):
            continue

        # Walk up to find a container with a heading
        container = time_el.parent
        title = ""
        for _ in range(5):
            if not container:
                break
            h = container.find(["h1", "h2", "h3", "h4", "h5"])
            if h:
                title = h.get_text(strip=True)
                break
            container = container.parent

        if not title:
            sib = time_el.find_previous_sibling(["h1", "h2", "h3", "h4", "h5", "strong", "b"])
            if sib:
                title = sib.get_text(strip=True)
        if not title:
            title = time_el.get_text(strip=True)

        url_el = (container or time_el).find("a", href=True) if container else None
        evt = make_event(title=title, date_obj=d, time_str=t,
                         url=(url_el["href"] if url_el else source_url),
                         source_url=source_url)
        if evt:
            events.append(evt)
    return events


# ── Site-specific: YogaMaya ───────────────────────────────────────────────────

def extract_yogamaya(soup: BeautifulSoup, source_url: str) -> list[dict]:
    events = []
    for wrapper in soup.find_all("div", class_=re.compile(r"event-wrapper")):
        date_el  = wrapper.find(class_=re.compile(r"event-date"))
        time_el  = wrapper.find(class_=re.compile(r"event-time"))
        title_el = wrapper.find("h3", class_=re.compile(r"event-title"))
        if not title_el:
            title_el = wrapper.find(class_=re.compile(r"event-title"))

        date_text  = date_el.get_text(strip=True)  if date_el  else ""
        time_text  = time_el.get_text(strip=True)  if time_el  else ""
        title_text = title_el.get_text(strip=True) if title_el else ""

        d = parse_date(date_text)
        t = parse_time(time_text) or parse_time(date_text)
        url_el = wrapper.find("a", href=True)
        evt = make_event(title=title_text, date_obj=d, time_str=t,
                         url=(url_el["href"] if url_el else source_url),
                         source_url=source_url)
        if evt:
            events.append(evt)
    return events


# ── Site-specific: Kinlia (direct API — no browser needed) ───────────────────

KINLIA_API = (
    "https://app.kinlia.life/api/external/events"
    "?page=1&page_size=100&sort_type=upcoming&show_online=true&location=new_york"
)


async def fetch_kinlia_events(source_url: str) -> list[dict]:
    """Hit Kinlia's public JSON API directly — no browser required."""
    try:
        async with httpx.AsyncClient(headers=HTTP_HEADERS, timeout=15) as client:
            r = await client.get(KINLIA_API)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        print(f"  [kinlia api error] {e}")
        return []

    events = []
    for item in data.get("results", []):
        start_str = item.get("start_time", "")
        end_str   = item.get("end_time", "")
        if not start_str:
            continue

        # The API returns times with a Z suffix but they are actually in the
        # event's local timezone (item["timezone"], always "America/New_York").
        # Strip the Z and parse as naive local time to avoid a spurious UTC shift.
        try:
            tz_name = item.get("timezone") or "America/New_York"
            tz = ZoneInfo(tz_name)
            start_dt = datetime.fromisoformat(start_str.rstrip("Z")).replace(tzinfo=tz)
            end_dt   = datetime.fromisoformat(end_str.rstrip("Z")).replace(tzinfo=tz) if end_str else None
        except Exception:
            continue

        d = start_dt.date()
        t_start = start_dt.strftime("%-I:%M %p")
        t_end   = end_dt.strftime("%-I:%M %p") if end_dt else None
        time_str = f"{t_start} – {t_end}" if t_end and t_start != t_end else t_start

        title    = item.get("name", "")
        desc     = item.get("details") or ""
        location = item.get("location_name") or item.get("city") or None
        event_id = item.get("id")
        url      = f"https://kinlia.com/events/{event_id}" if event_id else source_url

        evt = make_event(title=title, date_obj=d, time_str=time_str,
                         description=desc, url=url, location=location,
                         source_url=source_url)
        if evt:
            events.append(evt)

    return dedup(events)


# ── Site-specific: Broome Street Ganesh (The Events Calendar REST API) ────────

BSG_API = (
    "https://broomestreetganesh.org/wp-json/tribe/events/v1/events"
    "?per_page=50&status=publish"
)


async def fetch_bsg_events(source_url: str) -> list[dict]:
    """Fetch events from The Events Calendar REST API on broomestreetganesh.org."""
    from datetime import date as Date
    today = Date.today().isoformat()
    url = f"{BSG_API}&start_date={today}"
    try:
        async with httpx.AsyncClient(headers=HTTP_HEADERS, timeout=15) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        print(f"  [bsg api error] {e}")
        return []

    events = []
    for item in data.get("events", []):
        start_str = item.get("start_date", "")
        end_str   = item.get("end_date", "")
        if not start_str:
            continue

        try:
            tz = ZoneInfo(item.get("timezone") or "America/New_York")
            start_dt = datetime.fromisoformat(start_str).replace(tzinfo=tz)
            end_dt   = datetime.fromisoformat(end_str).replace(tzinfo=tz) if end_str else None
        except Exception:
            continue

        d = start_dt.date()
        t_start = start_dt.strftime("%-I:%M %p")
        t_end   = end_dt.strftime("%-I:%M %p") if end_dt else None
        time_str = f"{t_start} – {t_end}" if t_end and t_start != t_end else t_start

        title = unescape(item.get("title", ""))
        desc  = item.get("description") or ""
        evt_url = item.get("url") or source_url

        evt = make_event(title=title, date_obj=d, time_str=time_str,
                         description=desc, url=evt_url, location=None,
                         source_url=source_url)
        if evt:
            events.append(evt)

    return dedup(events)


# ── Site-specific: Thus (Shopify JSON API) ────────────────────────────────────

THUS_API = "https://shop.thus.org/collections/programs-events/products.json?limit=50"

# Strip trailing " | Month" suffix from Shopify product titles
_THUS_MONTH_SUFFIX_RE = re.compile(
    r"\s*\|\s*(?:january|february|march|april|may|june|july|august"
    r"|september|october|november|december)$",
    re.IGNORECASE,
)


async def fetch_thus_api(source_url: str) -> list[dict]:
    """Use Shopify's products.json API instead of HTML — works from any IP."""
    try:
        async with httpx.AsyncClient(headers=HTTP_HEADERS, timeout=15) as client:
            r = await client.get(THUS_API, follow_redirects=True)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        print(f"  [thus api error] {e}")
        return []

    events = []
    for product in data.get("products", []):
        title = _THUS_MONTH_SUFFIX_RE.sub("", product.get("title", "")).strip()
        handle = product.get("handle", "")
        url = f"https://shop.thus.org/products/{handle}"

        body_html = product.get("body_html", "")
        body_text = re.sub(r"<[^>]+>", " ", body_html)
        body_text = unescape(body_text)
        body_text = re.sub(r"\s+", " ", body_text).strip()

        t = parse_time(body_text)

        # Extract every explicit date from the body (handles multi-date events)
        seen: set = set()
        for m in DATE_RE.finditer(body_text):
            d = parse_date(m.group(0))
            if d and d not in seen:
                seen.add(d)
                evt = make_event(title=title, date_obj=d, time_str=t,
                                 url=url, source_url=source_url)
                if evt:
                    events.append(evt)

    return dedup(events)


# ── Site-specific: Thus (Shopify HTML fallback — kept for reference) ───────────

_THUS_DATE_RE = re.compile(
    r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s*[•·]\s*"
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})\s*[•·]\s*"
    r"(\d{1,2}:\d{2}\s*[AP]M)",
    re.IGNORECASE,
)

_THUS_MONTH_RE = re.compile(
    r"(?:^|\|\s*)(January|February|March|April|May|June|July|August"
    r"|September|October|November|December)\s*(?:\|)",
    re.IGNORECASE,
)


def extract_thus(soup: BeautifulSoup, source_url: str) -> list[dict]:
    events = []
    for card in soup.find_all(class_="product-item"):
        url_el = card.find("a", href=True)
        url = url_el["href"] if url_el else source_url
        if url and url.startswith("/"):
            url = "https://shop.thus.org" + url

        text = card.get_text(separator=" | ", strip=True)
        m = _THUS_DATE_RE.search(text)
        if not m:
            continue

        month = MONTH_MAP.get(m.group(1).lower()[:3])
        if not month:
            continue
        try:
            d = Date(datetime.now().year, month, int(m.group(2)))
        except ValueError:
            continue

        t = m.group(3).strip()

        # Title: text between the type indicator and the month name
        month_m = _THUS_MONTH_RE.search(text)
        parts = [p.strip() for p in text.split("|") if p.strip()]
        if month_m:
            # Find which part index contains the standalone month
            title_parts = []
            for part in parts:
                if re.match(r"^(january|february|march|april|may|june|july|august"
                            r"|september|october|november|december)$", part, re.I):
                    break
                if not re.match(r"^(in-person|online|hybrid|virtual)", part, re.I):
                    title_parts.append(part)
            title = " | ".join(title_parts).strip(" |") if title_parts else parts[0]
        else:
            title = parts[1] if len(parts) > 1 else parts[0]

        evt = make_event(title=title, date_obj=d, time_str=t,
                         url=url, source_url=source_url)
        if evt:
            events.append(evt)
    return events


# ── Site-specific: Kula Yoga ──────────────────────────────────────────────────

_KULA_SHORT_DATE_RE = re.compile(r"^[A-Za-z]+,\s+\d{1,2}/\d{1,2}/\d{2,4}$")


def extract_kula(soup: BeautifulSoup, source_url: str) -> list[dict]:
    events = []
    text = soup.get_text(separator="\n", strip=True)
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    for i, line in enumerate(lines):
        if not _KULA_SHORT_DATE_RE.match(line):
            continue
        d = parse_date(line)
        t = parse_time(line)
        if not d or not in_week(d):
            continue
        # Title is 2 lines above (title, then teacher)
        title = lines[i - 2] if i >= 2 else (lines[i - 1] if i >= 1 else "")
        if not title or len(title) < 4:
            continue
        evt = make_event(title=title, date_obj=d, time_str=t,
                         source_url=source_url)
        if evt:
            events.append(evt)
    return events


# ── Site-specific: Ohm Center ─────────────────────────────────────────────────

# "Thursday, March 26th 7-8:00pm: Deep Rest..."
_OHM_DAY_DATE_RE = re.compile(
    r"^\w+day,?\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)",
    re.IGNORECASE,
)
# "March 25, Wednesday 7-8:15pm, Let it Go..."
_OHM_DATE_DAY_RE = re.compile(
    r"^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+\d+",
    re.IGNORECASE,
)
# "Tuesdays March 17th, 24th, 31st 7-9pm: ..."  (multi-date recurring)
_OHM_MULTI_RE = re.compile(
    r"^\w+days?\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)",
    re.IGNORECASE,
)
# Recurring like "Thursdays 7-8:00pm" — standalone line
_OHM_DOW_TIME_RE = re.compile(
    r"^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)s?\s+"
    r"\d{1,2}(?::\d{2})?(?:\s*[-–]\s*\d{1,2}(?::\d{2})?)?\s*(?:am|pm)",
    re.IGNORECASE,
)


def _ohm_title_from_line(line: str) -> str:
    """Strip date/time prefix to get the event title."""
    # After "pm:" or "pm," separator (e.g. "March 25th 7pm: My Event")
    m = re.search(r"(?:pm|am)\s*[:\-,]\s*(.+)", line, re.I)
    if m:
        title = m.group(1).strip()
        # Strip trailing DOW schedule noise: "Thursdays 8:15pm & Saturdays ..."
        title = re.sub(r"[:,]\s*(?:mondays?|tuesdays?|wednesdays?|thursdays?|fridays?|saturdays?|sundays?).*$",
                       "", title, flags=re.IGNORECASE).strip()
        return title[:120]
    # Remove date and time, keep the rest
    cleaned = DATE_RE.sub("", line)
    cleaned = TIME_RE.sub("", cleaned)
    cleaned = TIME_RANGE_NO_AMPM_RE.sub("", cleaned)
    cleaned = re.sub(r"^\s*[,:\-–]\s*", "", cleaned).strip()
    return cleaned[:120]


def extract_ohm(soup: BeautifulSoup, source_url: str) -> list[dict]:
    events = []
    text = soup.get_text(separator="\n", strip=True)
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    prev_title = ""  # for recurring events, title may precede DOW+time line

    for i, line in enumerate(lines):
        # Multi-date: "Tuesdays March 17th, 24th, 31st 7-9pm: Title"
        if _OHM_MULTI_RE.match(line):
            title = _ohm_title_from_line(line)
            t = parse_time(line)
            # Extract all individual dates
            for dm in re.finditer(
                r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+(\d+)",
                line, re.IGNORECASE
            ):
                month = MONTH_MAP.get(dm.group(1).lower()[:3])
                if not month:
                    continue
                try:
                    d = Date(datetime.now().year, month, int(dm.group(2)))
                except ValueError:
                    continue
                evt = make_event(title=title, date_obj=d, time_str=t,
                                 source_url=source_url)
                if evt:
                    events.append(evt)
            continue

        # Single date: "Thursday, March 26th ..." or "March 25, Wednesday ..."
        if _OHM_DAY_DATE_RE.match(line) or _OHM_DATE_DAY_RE.match(line):
            d = parse_date(line)
            t = parse_time(line)
            title = _ohm_title_from_line(line)
            if not title:
                # try next non-trivial line
                if i + 1 < len(lines) and len(lines[i + 1]) > 5:
                    title = lines[i + 1]
            evt = make_event(title=title, date_obj=d, time_str=t,
                             source_url=source_url)
            if evt:
                events.append(evt)
            continue

        # Recurring DOW+time: "Thursdays 7-8:00pm"
        if _OHM_DOW_TIME_RE.match(line):
            dow_m = re.match(
                r"^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)s?",
                line, re.I
            )
            if dow_m:
                dow = DOW_MAP.get(dow_m.group(1).lower())
                if dow is not None:
                    d = next_weekday(dow)
                    t = parse_time(line)
                    # Use the line above (or 2 above) as title
                    title = prev_title or (lines[i - 1] if i > 0 else "")
                    evt = make_event(title=title, date_obj=d, time_str=t,
                                     source_url=source_url)
                    if evt:
                        events.append(evt)

        # Track previous non-trivial line for recurring title lookup
        # Don't use DOW+time lines or price/booking lines as titles
        if (len(line) > 8
                and not _OHM_DOW_TIME_RE.match(line)
                and not re.match(r"^\$|^\d|^book|^all levels|^open to", line, re.I)):
            prev_title = line

    return dedup(events)


# ── Dedup + master extractor ──────────────────────────────────────────────────

def dedup(events: list[dict]) -> list[dict]:
    seen: set = set()
    out = []
    for e in events:
        norm_title = re.sub(r'[^a-z0-9]', '', e["title"].lower())[:40]
        key = (norm_title, e["date"])
        if key not in seen:
            seen.add(key)
            out.append(e)
    return out


# ── Site-specific: Souk Studio ────────────────────────────────────────────────

def extract_souk(soup: BeautifulSoup, source_url: str) -> list[dict]:
    events = []
    for table in soup.find_all("div", id="schedule-table"):
        title_el = table.find("h2", class_="table_title")
        if not title_el:
            continue
        try:
            date_obj = dateutil_parser.parse(title_el.get_text(strip=True)).date()
        except Exception:
            continue
        if not in_week(date_obj):
            continue

        for row in table.find_all("div", class_="row"):
            left = row.find("div", class_="left")
            if not left:
                continue

            time_el = left.find("div", class_="first-column")
            time_str = None
            if time_el:
                raw = time_el.find("p")
                if raw:
                    time_str = raw.contents[0].strip() if raw.contents else None

            title_col = left.find("div", class_="second-column")
            if not title_col:
                continue
            title_p = title_col.find("p")
            if not title_p:
                continue
            title = title_p.get_text(strip=True)

            book_a = row.find("a", class_="link")
            event_url = book_a["href"] if book_a and book_a.get("href") else source_url

            evt = make_event(title=title, date_obj=date_obj, time_str=time_str,
                             url=event_url, source_url=source_url)
            if evt:
                events.append(evt)
    return events


# ── Site-specific: Satsang NYC (public JSON API) ─────────────────────────────

SATSANG_API = "https://www.satsangnyc.com/api/events"


async def fetch_satsang_api(source_url: str) -> list[dict]:
    try:
        async with httpx.AsyncClient(headers=HTTP_HEADERS, timeout=15) as client:
            r = await client.get(SATSANG_API)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        print(f"  [satsang api error] {e}")
        return []

    events = []
    for item in data:
        start_str = item.get("startDateTime", "")
        end_str = item.get("endDateTime", "")
        if not start_str:
            continue
        try:
            start_dt = datetime.fromisoformat(start_str).astimezone(NYC_TZ)
            end_dt = datetime.fromisoformat(end_str).astimezone(NYC_TZ) if end_str else None
        except Exception:
            continue

        d = start_dt.date()
        t_start = start_dt.strftime("%-I:%M %p")
        t_end = end_dt.strftime("%-I:%M %p") if end_dt else None
        time_str = f"{t_start} – {t_end}" if t_end and t_start != t_end else t_start

        name = item.get("name", "")
        desc = item.get("description", "")
        center = item.get("center", "")
        url = item.get("link") or item.get("registrationLink") or source_url

        evt = make_event(title=name, date_obj=d, time_str=time_str,
                         description=desc, location=center,
                         url=url, source_url=source_url)
        if evt:
            events.append(evt)

    return dedup(events)


# ── Site-specific: Bhakti School NYC (Wix data-hook attributes) ───────────────

def extract_bhaktischool(soup: BeautifulSoup, source_url: str) -> list[dict]:
    events = []
    for card in soup.find_all(attrs={"data-hook": "events-card"}):
        title_el = card.find(attrs={"data-hook": "title"})
        date_el = card.find(attrs={"data-hook": "short-date"})
        link_el = card.find("a", href=True)

        title = title_el.get_text(strip=True) if title_el else ""
        date_text = date_el.get_text(strip=True) if date_el else ""
        url = link_el["href"] if link_el else source_url

        d = parse_date(date_text)
        evt = make_event(title=title, date_obj=d, url=url, source_url=source_url)
        if evt:
            events.append(evt)
    return dedup(events)


# ── Site-specific: ISHTA Yoga (Squarespace eventlist; absolute URLs) ─────────

def extract_ishtayoga(soup: BeautifulSoup, source_url: str) -> list[dict]:
    events = []
    base = "https://ishtayoga.com"
    for el in soup.find_all(class_=re.compile(r"eventlist-event", re.I)):
        title_el = el.find(class_=re.compile(r"eventlist-title", re.I)) or el.find(["h1", "h2", "h3"])
        title = title_el.get_text(strip=True) if title_el else ""

        date_el = el.find("time", class_="event-date")
        d, _ = parse_datetime_attr(date_el.get("datetime", "")) if date_el else (None, None)

        start_el = el.find("time", class_="event-time-localized-start")
        t = parse_time(start_el.get_text(strip=True)) if start_el else None

        link_el = el.find("a", class_="eventlist-title-link") or el.find("a", href=True)
        href = link_el["href"] if link_el else ""
        url = (base + href) if href.startswith("/") else (href or source_url)

        evt = make_event(title=title, date_obj=d, time_str=t, url=url, source_url=source_url)
        if evt:
            events.append(evt)
    return dedup(events)


# ── Site-specific: Life Shop NY (Wix SSR; shows next upcoming event) ─────────

def extract_lifeshop(soup: BeautifulSoup, source_url: str) -> list[dict]:
    events = []
    titles = soup.find_all(attrs={"data-hook": "event-title"})
    dates = soup.find_all(attrs={"data-hook": "event-full-date"})
    for title_el, date_el in zip(titles, dates):
        title = title_el.get_text(strip=True)
        date_text = date_el.get_text(strip=True)  # e.g. "Apr 07, 2026, 8:00 PM – 10:15 PM"
        d = parse_date(date_text)
        t = parse_time(date_text)
        evt = make_event(title=title, date_obj=d, time_str=t, url=source_url, source_url=source_url)
        if evt:
            events.append(evt)
    return dedup(events)


# ── Site-specific: Groupmuse (NYC events only via EDT/EST filter) ─────────────

def extract_groupmuse(soup: BeautifulSoup, source_url: str) -> list[dict]:
    events = []
    for card in soup.find_all("div", class_="card-content"):
        parts = [p.strip() for p in card.get_text(separator="|", strip=True).split("|")]

        # Only NYC events have EDT or EST timezone markers
        try:
            tz_idx = next(i for i, p in enumerate(parts) if p in ("EDT", "EST", "ET"))
        except StopIteration:
            continue

        if tz_idx < 1:
            continue

        title = parts[0]
        datetime_str = parts[tz_idx - 1]  # e.g. "Tuesday, Apr  7  7:00 PM"

        d = parse_date(datetime_str)
        t = parse_time(datetime_str)

        location = None
        try:
            nbhd_idx = next(i for i, p in enumerate(parts)
                            if "neighborhood" in p.lower() or "location" in p.lower())
            location = parts[nbhd_idx + 1] if nbhd_idx + 1 < len(parts) else None
        except StopIteration:
            pass

        link_el = card.find("a", href=True)
        if not link_el:
            parent = card.find_parent(class_=re.compile(r"\bcard\b"))
            link_el = parent.find("a", href=True) if parent else None
        url = link_el["href"] if link_el else source_url
        if url and url.startswith("/"):
            url = "https://www.groupmuse.com" + url

        evt = make_event(title=title, date_obj=d, time_str=t,
                         location=location, url=url, source_url=source_url)
        if evt:
            events.append(evt)
    return dedup(events)


# ── Site-specific: Prema Brooklyn (Squarespace fluid-engine text blocks) ──────

_PREMA_DOW_PREFIX_RE = re.compile(
    r"^(?:mondays?|tuesdays?|wednesdays?|thursdays?|fridays?|saturdays?|sundays?)"
    r"\s*\|?\s*",
    re.IGNORECASE,
)


def _prema_parse_date_line(line: str) -> list:
    """Return a list of Date objects parsed from one Prema date line.

    Handles formats like:
      "Thursday April 9"
      "Saturday | April 18th"
      "Sundays | April 19th, May 17th, June 7th"
      "Thursdays | April 23rd, 30th, May 7th"
    """
    clean = _PREMA_DOW_PREFIX_RE.sub("", line).strip()
    parts = re.split(r",\s*(?:and\s+)?|\s+and\s+", clean)

    result = []
    current_month: Optional[str] = None

    for part in parts:
        part = part.strip()
        if not part:
            continue

        m = re.match(
            r"^(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?"
            r"|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?"
            r"|nov(?:ember)?|dec(?:ember)?)\w*",
            part, re.IGNORECASE,
        )
        if m:
            current_month = m.group(0)
            d = parse_date(part)
        elif current_month and re.match(r"^\d{1,2}(?:st|nd|rd|th)?$", part, re.IGNORECASE):
            d = parse_date(f"{current_month} {part}")
        else:
            d = parse_date(part)

        if d:
            result.append(d)

    return result


def extract_prema(soup: BeautifulSoup, source_url: str) -> list[dict]:
    from urllib.parse import urljoin

    for tag in soup.find_all(["script", "style", "svg", "noscript"]):
        tag.decompose()

    # Collect "View Event →" link hrefs before stripping HTML, in document order.
    event_urls = []
    for a in soup.find_all("a", href=True):
        if re.match(r"^view\s+event", a.get_text(strip=True), re.IGNORECASE):
            event_urls.append(urljoin(source_url, a["href"]))

    text = soup.get_text(separator="\n", strip=True)
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # Split into event blocks delimited by "View Event →"
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in lines:
        if re.match(r"^view\s+event", line, re.IGNORECASE):
            if current:
                blocks.append(current[:])
            current = []
        else:
            current.append(line)

    _NOISE = {"(map)", "google calendar", "ics"}

    events = []
    for i, block in enumerate(blocks):
        event_url = event_urls[i] if i < len(event_urls) else source_url

        # Find the full date line (e.g. "Sunday, April 12, 2026").
        # The title is the line immediately before it; times follow immediately after.
        date_line_idx = None
        for j, line in enumerate(block):
            if re.match(
                r"^(?:mon|tue|wed|thu|fri|sat|sun)\w*,?\s+\w+\s+\d{1,2},?\s+\d{4}",
                line, re.IGNORECASE,
            ):
                date_line_idx = j
                break

        if date_line_idx is None or date_line_idx == 0:
            continue

        title = block[date_line_idx - 1]
        if not title:
            continue

        d = parse_date(block[date_line_idx])
        if not d:
            continue

        # Collect consecutive time lines after the date (stop at noise/description).
        time_parts = []
        for line in block[date_line_idx + 1:]:
            if line.lower() in _NOISE:
                break
            if TIME_RE.search(line) or TIME_RANGE_NO_AMPM_RE.search(line):
                time_parts.append(parse_time(line))
            else:
                break

        if len(time_parts) >= 2:
            time_str = f"{time_parts[0]} – {time_parts[1]}"
        elif time_parts:
            time_str = time_parts[0]
        else:
            time_str = None

        evt = make_event(title=title, date_obj=d, time_str=time_str,
                         url=event_url, source_url=source_url)
        if evt:
            events.append(evt)

    return dedup(events)


# ── Site-specific: 113 Spring (Shopify products.json; "Offered on" dates) ─────

_113SPRING_API = "https://113spring.com/collections/all/products.json?limit=100"
_113SPRING_OFFERED_RE = re.compile(
    r"Offered\s+on\s+(.*?)(?=\s+\d+\s+minutes|\s+Complimentary|This\s+event|$)",
    re.IGNORECASE | re.DOTALL,
)


async def fetch_113spring_api(source_url: str) -> list[dict]:
    try:
        async with httpx.AsyncClient(headers=HTTP_HEADERS, timeout=15) as client:
            r = await client.get(_113SPRING_API, follow_redirects=True)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        print(f"  [113spring api error] {e}")
        return []

    events = []
    for product in data.get("products", []):
        title = product.get("title", "").strip()
        handle = product.get("handle", "")
        url = f"https://113spring.com/products/{handle}"

        body_html = product.get("body_html", "")
        body_text = re.sub(r"<[^>]+>", " ", body_html)
        body_text = unescape(body_text)
        body_text = re.sub(r"\s+", " ", body_text).strip()

        m = _113SPRING_OFFERED_RE.search(body_text)
        if not m:
            continue

        date_text = re.sub(r"\s+", " ", m.group(1)).strip().rstrip(".")

        # Parse comma/and-separated dates with carry-forward month logic
        parts = re.split(r",\s*(?:and\s+)?|\s+and\s+", date_text)
        current_month: Optional[str] = None
        seen: set = set()

        for part in parts:
            part = part.strip()
            if not part:
                continue

            month_m = re.match(
                r"^(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?"
                r"|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?"
                r"|nov(?:ember)?|dec(?:ember)?)\w*",
                part, re.IGNORECASE,
            )
            if month_m:
                current_month = month_m.group(0)
                d = parse_date(part)
            elif current_month and re.match(r"^\d{1,2}(?:st|nd|rd|th)?$", part, re.IGNORECASE):
                d = parse_date(f"{current_month} {part}")
            else:
                d = parse_date(part)

            if d and d not in seen:
                seen.add(d)
                evt = make_event(title=title, date_obj=d, url=url,
                                 source_url=source_url)
                if evt:
                    events.append(evt)

    return dedup(events)


# ── Site-specific: Bhakti Center (Avia builder card grid) ─────────────────────
#
# Each event is a div.flex_column.av_one_fourth.avia-link-column containing:
#   - data-link-column-url  → event page URL
#   - img[title]            → event name (the visible card image has a title attr)
#   - section.av_textblock_section p → date text e.g. "saturday, april 4th"

def extract_bhakticenter(soup: BeautifulSoup, source_url: str) -> list[dict]:
    events = []
    for div in soup.find_all("div", class_=lambda c: c and "avia-link-column" in c):
        url = div.get("data-link-column-url", "").strip() or source_url

        img = div.find("img", title=True)
        title = img["title"].strip() if img else None
        # Skip missing titles or WordPress dimension strings like "1080 x 1234 (1)"
        if not title or re.match(r"^\d+\s*[x×]\s*\d+", title):
            continue

        date_p = div.find("section", class_="av_textblock_section")
        if not date_p:
            continue
        date_text = date_p.get_text(strip=True)

        d = parse_date(date_text)
        if not d:
            continue

        evt = make_event(title=title, date_obj=d, url=url, source_url=source_url)
        if evt:
            events.append(evt)
    return dedup(events)


SITE_EXTRACTORS = {
    "yogamaya.com":       extract_yogamaya,
    "thus.org":           extract_thus,
    "kulayoga.com":       extract_kula,
    "ohmcenter.com":      extract_ohm,
    "soukstudio.com":     extract_souk,
    "bhaktischoolnyc.com": extract_bhaktischool,
    "bhakticenter.org":   extract_bhakticenter,
    "lifeshopny.com":     extract_lifeshop,
    "ishtayoga.com":      extract_ishtayoga,
    "groupmuse.com":      extract_groupmuse,
    "premabrooklyn.com":  extract_prema,
}


def extract_events(html: str, source_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "svg", "noscript"]):
        tag.decompose()

    # Site-specific extractors first
    for domain, fn in SITE_EXTRACTORS.items():
        if domain in source_url:
            events = fn(soup, source_url)
            if events:
                return dedup(events)

    # Generic fallbacks (work for all sites)
    all_events: list[dict] = []
    all_events += extract_jsonld(soup, source_url)
    all_events += extract_microdata(soup, source_url)
    all_events += extract_tribe_events(soup, source_url)
    all_events += extract_squarespace(soup, source_url)
    all_events += extract_time_elements(soup, source_url)
    return dedup(all_events)


# ── HTTP fetcher (no browser) ─────────────────────────────────────────────────

async def fetch_page(url: str, client: httpx.AsyncClient) -> str:
    try:
        r = await client.get(url, timeout=20, follow_redirects=True)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"  [fetch error] {url}: {e}")
        return ""


# Sites that block httpx via TLS fingerprinting but allow curl
CURL_DOMAINS = {"soukstudio.com", "bhakticenter.org"}


async def fetch_via_curl(url: str) -> str:
    """Fetch a URL using the system curl binary (bypasses TLS fingerprinting)."""
    import asyncio as _aio
    try:
        proc = await _aio.create_subprocess_exec(
            "curl", "-s", "-L", "--max-time", "20",
            "-A", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            url,
            stdout=_aio.subprocess.PIPE,
            stderr=_aio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        return stdout.decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [curl error] {url}: {e}")
        return ""


async def scrape_source(source: dict, client: httpx.AsyncClient,
                        semaphore: asyncio.Semaphore) -> list[dict]:
    async with semaphore:
        print(f"Scraping: {source['name']} ...")
        t0 = asyncio.get_event_loop().time()

        if "kinlia.com" in source["url"]:
            events = await fetch_kinlia_events(source["url"])
        elif "thus.org" in source["url"]:
            events = await fetch_thus_api(source["url"])
        elif "satsangnyc.com" in source["url"]:
            events = await fetch_satsang_api(source["url"])
        elif "broomestreetganesh.org" in source["url"]:
            events = await fetch_bsg_events(source["url"])
        elif "113spring.com" in source["url"]:
            events = await fetch_113spring_api(source["url"])
        elif any(d in source["url"] for d in CURL_DOMAINS):
            html = await fetch_via_curl(source["url"])
            if not html:
                return []
            loop = asyncio.get_event_loop()
            events = await loop.run_in_executor(
                None, extract_events, html, source["url"]
            )
        else:
            html = await fetch_page(source["url"], client)
            if not html:
                return []
            loop = asyncio.get_event_loop()
            events = await loop.run_in_executor(
                None, extract_events, html, source["url"]
            )

        elapsed = asyncio.get_event_loop().time() - t0
        for evt in events:
            evt["source"] = source["name"]
            evt["category"] = source["category"]
            evt["source_url"] = source["url"]
        print(f"  -> {len(events)} events ({elapsed:.1f}s)")
        return events


def load_static_events(path: str = "static_events.json") -> list[dict]:
    """Load manually committed events and filter to the current week."""
    import os
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            events = json.load(f)
        from datetime import date as _Date
        return [e for e in events if in_week(_Date.fromisoformat(e["date"]))]
    except Exception as e:
        print(f"  [static events error] {e}")
        return []


async def scrape_all(sources: list[dict]) -> list[dict]:
    semaphore = asyncio.Semaphore(CONCURRENCY)
    active = [s for s in sources if s.get("status", "active") != "inactive"]
    async with httpx.AsyncClient(headers=HTTP_HEADERS) as client:
        tasks = [scrape_source(s, client, semaphore) for s in active]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    all_events: list[dict] = []
    for r in results:
        if isinstance(r, list):
            all_events.extend(r)
        elif isinstance(r, Exception):
            print(f"  [task error] {r}")

    static = load_static_events()
    if static:
        print(f"  [static] loaded {len(static)} event(s) from static_events.json")
    all_events.extend(static)
    return dedup(all_events)
