CATEGORY_COLORS = {
    "Yoga":     "#4ade80",   # green
    "Buddhism": "#fbbf24",   # amber
    "Spiritual": "#a78bfa",  # violet
    "General":  "#60a5fa",   # blue
}

SOURCES = [
    # ── Existing ──────────────────────────────────────────────────────────────
    {
        "name": "Yoga Maya",
        "url": "https://www.yogamaya.com/events",
        "category": "Yoga",
        "status": "active",
    },
    {
        "name": "Kinlia NYC",
        "url": "https://kinlia.com/new-york",
        "category": "General",
        "status": "active",
    },
    {
        "name": "Thus Institute",
        "url": "https://shop.thus.org/collections/programs-events",
        "category": "Buddhism",
        "status": "active",
    },
    {
        "name": "Kula Yoga",
        "url": "https://experience.kulayoga.com/events?event_category_id=kula-yoga-project-workshops",
        "category": "Yoga",
        "status": "inactive",
        "note": "Cloudflare JS challenge — requires headless browser, not scrapable with curl/httpx",
    },
    {
        "name": "Ohm Center",
        "url": "https://www.ohmcenter.com/schedule",
        "category": "Yoga",
        "status": "active",
    },
    {
        "name": "Souk Studio",
        "url": "https://www.soukstudio.com/schedule",
        "category": "Yoga",
        "status": "active",
    },
    {
        "name": "Bhakti Center",
        "url": "https://bhakticenter.org/all-offerings-2/",
        "category": "Spiritual",
        "status": "active",
        "note": "Avia builder card grid; img[title] + textblock date; curl required",
    },
    {
        "name": "Broome Street Ganesh",
        "url": "https://broomestreetganesh.org/events-calendar/",
        "category": "Spiritual",
        "status": "active",
        "note": "The Events Calendar REST API at /wp-json/tribe/events/v1/events",
    },
    {
        "name": "Bhakti Marga",
        "url": "https://bhaktimarga.us/pages/calendar",
        "category": "Spiritual",
        "status": "inactive",
        "note": "Cloudflare managed JS challenge — all endpoints blocked, requires headless browser",
    },
    {
        "name": "ISHTA Yoga — Workshops",
        "url": "https://ishtayoga.com/workshops",
        "category": "Yoga",
        "status": "active",
        "note": "Squarespace eventlist-event; absolute URLs resolved from relative hrefs",
    },
    {
        "name": "ISHTA Yoga — Events",
        "url": "https://ishtayoga.com/info-sessions",
        "category": "Yoga",
        "status": "active",
        "note": "Squarespace eventlist-event; teacher training info sessions",
    },
    {
        "name": "Life Shop NY",
        "url": "https://www.lifeshopny.com/events",
        "category": "General",
        "status": "active",
        "note": "Wix SSR; renders next upcoming event via data-hook='event-title'/'event-full-date'",
    },
    # ── New ───────────────────────────────────────────────────────────────────
    {
        "name": "Satsang NYC",
        "url": "https://www.satsangnyc.com/calendar",
        "category": "Spiritual",
        "status": "active",
        "note": "JSON API at /api/events",
    },
    {
        "name": "Bhakti School NYC",
        "url": "https://www.bhaktischoolnyc.com/",
        "category": "Spiritual",
        "status": "active",
        "note": "Wix events-card data-hook attributes",
    },
    {
        "name": "Groupmuse",
        "url": "https://www.groupmuse.com/",
        "category": "General",
        "status": "active",
        "note": "NYC events filtered by EDT/EST timezone",
    },
    {
        "name": "Prema Brooklyn",
        "url": "https://www.premabrooklyn.com/communityevents",
        "category": "Yoga",
        "status": "active",
        "note": "Squarespace fluid-engine text blocks",
    },
    {
        "name": "113 Spring",
        "url": "https://113spring.com/pages/services-events",
        "category": "Yoga",
        "status": "active",
        "note": "Shopify products.json; dates from 'Offered on...' body text",
    },
    {
        "name": "NYC City Center (SRF)",
        "url": "https://newyorkcitycenter.org/calendar-of-events",
        "category": "Spiritual",
        "status": "inactive",
        "note": "GoDaddy site builder — JS-rendered, no accessible API",
    },
    {
        "name": "Infinite Space NYC",
        "url": "https://www.infinitespacenyc.com/schedule",
        "category": "Yoga",
        "status": "inactive",
        "note": "MindBody embedded widget — needs studio API key",
    },
    {
        "name": "Yoga Maya (Momence)",
        "url": "https://momence.com/u/yogamaya",
        "category": "Yoga",
        "status": "inactive",
        "note": "Momence booking platform — fully JS-rendered, no public API found",
    },
]
