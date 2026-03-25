"""
Rule-based event extractor.

Generic strategies (tried for all sites):
  1. JSON-LD Schema.org Event markup
  2. HTML microdata Schema.org Event
  3. WordPress The Events Calendar plugin
  4. Squarespace event lists
  5. <time datetime="..."> elements near headings

Site-specific extractors (keyed by domain substring):
  - yogamaya.com     → .event-wrapper CSS classes
  - kinlia.com       → EventCard_container DOM
  - thus.org         → Shopify product-item with pipe/bullet date
  - kulayoga.com     → text-block: title / teacher / "Sat, 3/28/26"
  - ohmcenter.com    → prose lines: "Month D, Day time: Title"
  - eventbrite.com   → handled by JSON-LD fix (ListItem.item)
"""

import asyncio
import json
import re
from datetime import datetime, timedelta, date as Date
from typing import Optional
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateutil_parser
from playwright.async_api import async_playwright

NYC_TZ = ZoneInfo("America/New_York")
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

CONCURRENCY = 15  # all sources in parallel

# ── Week helpers ─────────────────────────────────────────────────────────────

def _today() -> Date:
    return datetime.now().date()

def _week_end() -> Date:
    return _today() + timedelta(days=7)

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
        "description": description.strip()[:200] if description else None,
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

        # Convert UTC → NYC local time
        try:
            start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00")).astimezone(NYC_TZ)
            end_dt   = datetime.fromisoformat(end_str.replace("Z", "+00:00")).astimezone(NYC_TZ) if end_str else None
        except Exception:
            continue

        d = start_dt.date()
        t_start = start_dt.strftime("%-I:%M %p")
        t_end   = end_dt.strftime("%-I:%M %p") if end_dt else None
        time_str = f"{t_start} – {t_end}" if t_end and t_start != t_end else t_start

        title    = item.get("name", "")
        desc     = (item.get("details") or "")[:200]
        location = item.get("location_name") or item.get("city") or None
        event_id = item.get("id")
        url      = f"https://kinlia.com/events/{event_id}" if event_id else source_url

        evt = make_event(title=title, date_obj=d, time_str=time_str,
                         description=desc, url=url, location=location,
                         source_url=source_url)
        if evt:
            events.append(evt)

    return dedup(events)


# ── Site-specific: Thus (Shopify) ─────────────────────────────────────────────

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
        key = (e["title"].lower()[:40], e["date"])
        if key not in seen:
            seen.add(key)
            out.append(e)
    return out


SITE_EXTRACTORS = {
    "yogamaya.com":  extract_yogamaya,
    "thus.org":      extract_thus,
    "kulayoga.com":  extract_kula,
    "ohmcenter.com": extract_ohm,
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


# ── Playwright fetcher ────────────────────────────────────────────────────────

async def fetch_page(url: str, browser) -> str:
    page = await browser.new_page(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"
    )
    try:
        # Try domcontentloaded first; if it times out, fall back to 'load'
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=20_000)
        except Exception:
            await page.goto(url, wait_until="load", timeout=35_000)
        await page.wait_for_timeout(2_500)
        return await page.content()
    except Exception as e:
        print(f"  [fetch error] {url}: {e}")
        return ""
    finally:
        await page.close()


async def scrape_source(source: dict, browser, semaphore: asyncio.Semaphore) -> list[dict]:
    async with semaphore:
        print(f"Scraping: {source['name']} ...")
        t0 = asyncio.get_event_loop().time()

        # Sources with a direct API — skip the browser entirely
        if "kinlia.com" in source["url"]:
            events = await fetch_kinlia_events(source["url"])
        else:
            html = await fetch_page(source["url"], browser)
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


async def scrape_all(sources: list[dict]) -> list[dict]:
    semaphore = asyncio.Semaphore(CONCURRENCY)
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-dev-shm-usage", "--no-sandbox"],
        )
        tasks = [scrape_source(s, browser, semaphore) for s in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await browser.close()

    all_events: list[dict] = []
    for r in results:
        if isinstance(r, list):
            all_events.extend(r)
        elif isinstance(r, Exception):
            print(f"  [task error] {r}")
    return all_events
