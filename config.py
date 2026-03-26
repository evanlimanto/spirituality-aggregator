SOURCES = [
    # General
    {"name": "Kinlia NYC", "url": "https://kinlia.com/new-york", "category": "General",
     "status": "active", "note": "Direct JSON API"},

    # Yoga Studios
    {"name": "Yoga Maya", "url": "https://yogamaya.com/events/", "category": "Yoga",
     "status": "active", "note": "WordPress — custom CSS extractor"},
    {"name": "Souk Studio", "url": "https://soukstudio.com/schedule", "category": "Yoga",
     "status": "js-only", "note": "MindBody embed — JS required"},
    {"name": "Kula Yoga", "url": "https://experience.kulayoga.com/events", "category": "Yoga",
     "status": "active", "note": "Server-rendered event list"},
    {"name": "Ohm Center", "url": "https://ohmcenter.com/", "category": "Yoga",
     "status": "active", "note": "WordPress — prose line parser"},
    {"name": "Abhaya Yoga", "url": "https://abhayayoga.com/events/", "category": "Yoga",
     "status": "blocked", "note": "Returns HTTP 403"},
    {"name": "The Shala", "url": "https://theshala.com/happenings/events-and-workshops/", "category": "Yoga",
     "status": "active", "note": "WordPress — generic date scan"},
    {"name": "Bhakti Center", "url": "https://bhakticenter.org/all-offerings-2/", "category": "Yoga",
     "status": "active", "note": "WordPress — generic date scan"},
    {"name": "Om Factory NYC", "url": "https://www.omfactory.com/schedule", "category": "Yoga",
     "status": "js-only", "note": "MindBody embed — JS required"},
    {"name": "Warrior Bridge", "url": "https://www.warriorbridge.com/in-studio-schedule", "category": "Yoga",
     "status": "js-only", "note": "MindBody embed — JS required"},

    # Sound Baths / Wellness
    {"name": "Sound Mind Center", "url": "https://www.soundmindcenter.us/schedule-events", "category": "Sound Bath",
     "status": "js-only", "note": "JS-rendered schedule widget"},
    {"name": "The Alchemist's Kitchen", "url": "https://www.eventbrite.com/o/the-alchemists-kitchen-38687942063", "category": "Wellness",
     "status": "active", "note": "Eventbrite JSON-LD"},

    # Therapy
    {"name": "Porter Eichenlaub", "url": "https://www.portereichenlaub.com/copy-of-workshops", "category": "Therapy",
     "status": "active", "note": "Squarespace — generic event list"},

    # Buddhism
    {"name": "Thus Institute", "url": "https://shop.thus.org/collections/programs-events", "category": "Buddhism",
     "status": "active", "note": "Shopify — custom pipe/bullet parser"},

    # Men's Work
    {"name": "Sacred Sons", "url": "https://www.sacredsons.com/", "category": "Men's Work",
     "status": "blocked", "note": "Returns HTTP 403"},

    # Tea
    {"name": "Puerh Brooklyn", "url": "https://www.puerhbrooklyn.com/tearoom.html", "category": "Tea",
     "status": "active", "note": "Weebly — prose recurring schedule (tea ceremonies, workshops)"},

    # Yoga
    {"name": "Yogis & Yoginis", "url": "https://yogisandyoginis.com/schedule", "category": "Yoga",
     "status": "js-only", "note": "MindBody embed + SSL handshake issue"},

    # Wellness
    {"name": "Reforesters Lab", "url": "https://www.reforesters.io/", "category": "Wellness",
     "status": "active", "note": "Listening room / adaptogen café — events on homepage"},
    {"name": "Maha Rose", "url": "https://www.maharose.com/upcoming", "category": "Wellness",
     "status": "active", "note": "Healing arts center — upcoming events page"},
    {"name": "Official Ritual", "url": "https://officialritual.com/pages/rituals", "category": "Wellness",
     "status": "active", "note": "Ritual wellness space — rituals/events page"},
]

CATEGORY_COLORS = {
    "General":    "#6B8F71",
    "Yoga":       "#8E7BAD",
    "Sound Bath": "#5B9EA6",
    "Wellness":   "#B8956A",
    "Tea":        "#7BA05B",
    "Therapy":    "#C47C8A",
    "Buddhism":   "#B87333",
    "Men's Work": "#5B7BA0",
}
