# NYC Event Aggregator — System Design

A personal web app that scrapes ~15 NYC wellness/yoga/community event sources and displays the coming week of events grouped by day.

---

## Architecture

```
Browser
  │
  ▼
Flask (gunicorn, 1 worker)
  ├── GET /              → serves index.html
  ├── GET /api/events    → returns cached JSON (or triggers scrape on miss)
  └── POST /api/refresh  → busts cache and re-scrapes
         │
         ▼
    scrape_all()
    ├── Kinlia NYC        → direct JSON API (app.kinlia.life)
    └── all other sites  → httpx HTTP fetch → rule-based HTML extraction
         │
         ▼
    File cache (/tmp/event-aggregator-cache.json, TTL 1 hour)

APScheduler (background thread, same process)
  └── nightly at 3 AM ET → nightly_cache_refresh()
       └── only overwrites cache if ≥5 events returned
```

---

## Sources

| Source | Category | Extraction method |
|---|---|---|
| Kinlia NYC | General | Direct JSON API (`app.kinlia.life/api/external/events`) |
| Yoga Maya | Yoga | Site-specific: `.event-wrapper` CSS classes |
| Souk Studio | Yoga | Generic HTML (MindBody embed — usually 0 results) |
| Kula Yoga | Yoga | Site-specific: text blocks with `Sat, 3/28/26` date lines |
| Ohm Center | Yoga | Site-specific: prose line parser (`Month D, Day time: Title`) |
| Abhaya Yoga | Yoga | Generic HTML (403 blocked) |
| The Shala | Yoga | Generic JSON-LD / date scan |
| Bhakti Center | Yoga | Generic date scan |
| Om Factory NYC | Yoga | Generic HTML (MindBody — usually 0 results) |
| Warrior Bridge | Yoga | Generic HTML (MindBody embed — usually 0 results) |
| Sound Mind Center | Sound Bath | Generic HTML (JS-rendered — usually 0 results) |
| The Alchemist's Kitchen | Wellness | Generic JSON-LD (Eventbrite `ListItem.item` pattern) |
| Porter Eichenlaub | Therapy | Generic Squarespace event list |
| Thus Institute | Buddhism | Site-specific: Shopify `.product-item` pipe/bullet date format |
| Sacred Sons | Men's Work | Generic HTML (403 blocked) |

---

## Extraction Strategies

Applied in this priority order. Site-specific extractors run first; generic fallbacks apply to everything else.

### Site-specific

**Kinlia** — Bypasses HTML entirely. Calls `app.kinlia.life/api/external/events?location=new_york` directly (discovered by intercepting network requests). Returns clean JSON with UTC timestamps, converted to NYC local time.

**Yoga Maya** — WordPress site with predictable CSS classes: `.event-wrapper > .event-date`, `.event-time`, `h3.event-title`.

**Thus Institute** — Shopify collection page. Each `.product-item` contains pipe-separated text: `Type | Title | Month | Day • Mon DD • HH:MM AM | View Event`. Parsed with a regex targeting the bullet-separated date/time segment.

**Kula Yoga** — MindBody-based but server-renders event listings as plain text blocks. Groups of lines: title → teacher → `Day, M/D/YY` date line → "View Schedule". Detected by matching the short-date line pattern.

**Ohm Center** — WordPress site with prose event listings. Three line patterns handled:
- `Month D, Weekday time: Title` (single date)
- `Weekday, Month Dth time: Title` (single date)
- `Weekdays Month D, D, D time: Title` (multi-date recurring — expanded to individual occurrences)
- Standalone `Thursdays 7-8pm` lines (recurring DOW) matched against the preceding title line.

### Generic fallbacks

1. **JSON-LD** — `<script type="application/ld+json">` blocks. Handles standard `@type: Event` and Eventbrite's `ItemList > ListItem > item` nesting pattern.
2. **Microdata** — `itemtype="https://schema.org/Event"` HTML attributes.
3. **WordPress The Events Calendar** — `article.tribe_events` with `.tribe-events-abbr` date elements.
4. **Squarespace** — `.eventlist-event` / `summary-item--event` containers with `<time datetime>` elements.
5. **`<time datetime="...">` scan** — finds any `<time>` element with a parseable `datetime` attribute within the week, then walks up the DOM to find a nearby heading as the event title.

---

## Date & Time Parsing

All date extraction goes through a single `DATE_RE` regex covering:
- ISO `YYYY-MM-DD`
- `M/D` and `M/D/YY` slash formats
- `Month D, YYYY` and `D Month YYYY` written forms
- Ordinal suffixes (`st`, `nd`, `rd`, `th`)

Time parsing handles:
- `H:MM AM/PM` single times
- `H:MM AM/PM – H:MM AM/PM` ranges
- `H-H:MMpm` compact ranges (e.g. `7-8:00pm` → `7:00 PM – 8:00 PM`)

All dates are filtered to the current day through day+7. Kinlia timestamps are converted from UTC to `America/New_York`.

---

## Caching

- **Storage**: single JSON file at `/tmp/event-aggregator-cache.json`
- **TTL**: 1 hour for on-demand requests
- **Nightly pre-warm**: APScheduler fires `nightly_cache_refresh()` at 3:00 AM ET
  - Runs a full scrape
  - Only overwrites the cache if `≥ 5 events` returned (guards against silent failures overwriting good data)
  - Logs success/failure count

---

## Deployment

Hosted on [Railway](https://railway.app). Deployed via `railway up` using a project service token.

- **Runtime**: Python 3.13, gunicorn, 1 worker
- **Build**: Railpack (auto-detected Python)
- **Config**: `railway.toml` sets start command and healthcheck
- **URL**: `https://web-production-0910b.up.railway.app`
- **Re-deploy**: `RAILWAY_TOKEN=<token> railway up --service b0e5d8ab-d7a6-4235-bb89-65f7617f4bb1 --detach`

---

## Known Limitations

- **MindBody-embedded schedules** (Souk Studio, Warrior Bridge, Om Factory) render via JavaScript in an iframe. Plain HTTP fetches return the shell page only — 0 events extracted.
- **Blocked sites** (Abhaya Yoga, Sacred Sons) return HTTP 403 to non-browser user agents.
- **Railway `/tmp` is ephemeral** — the cache file is lost on redeploy/restart. The nightly cron re-warms it; the first request after a cold start triggers a live scrape (~6s).
