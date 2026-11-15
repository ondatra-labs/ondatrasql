# Google Search Console: Search Analytics (clicks, impressions, CTR and
# average position) per day for one property, through the Search Console
# API v3 with a Google service account:
# https://developers.google.com/webmaster-tools/v1/searchanalytics/query
#
#   FROM google_search_console('sc-domain:example.com', 'query', 3)
#
# Args: the property (a domain property as sc-domain:example.com, a URL
# prefix property as https://www.example.com/; '' reads
# GOOGLE_SEARCH_CONSOLE_SITE), the dimensions besides date (comma separated:
# query, page, country, device, searchAppearance; '' for totals per day),
# and days, how many days before the newest date already loaded a run
# fetches again.
#
# The first run (and a rebuild) fetches the 16 months Search Console keeps,
# or from @incremental_initial when that is later. Each row carries site,
# date, every dimension (searchAppearance as search_appearance), clicks,
# impressions, ctr and position (as text, for the model to cast), and
# row_key, a JSON list of the date and the dimension values, which the model
# merges on.
API = {
    "base_url": "https://searchconsole.googleapis.com/webmasters/v3",
    "auth": {
        "service_account": {"env": "GOOGLE_SEARCH_CONSOLE_KEY_FILE"},
        "scope": "https://www.googleapis.com/auth/webmasters.readonly",
    },
    "headers": {"Accept": "application/json"},
    "timeout": 120,
    "retry": 3,
    "backoff": 5,
    "fetch": {"args": ["site", "dimensions", "days"]},
}

DAY = time.parse_duration("24h")

# the most rows one request may ask for is 25000; pages of 5000 keep each
# response small
PAGE_SIZE = 5000

# how far back Search Console keeps data
KEEP_MONTHS = 16

DIMENSIONS = ["query", "page", "country", "device", "searchAppearance"]

_HEX = "0123456789ABCDEF"
_UNRESERVED = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"

def _escape(s):
    # the property is one path segment: sc-domain:example.com becomes
    # sc-domain%3Aexample.com, https://www.example.com/ becomes
    # https%3A%2F%2Fwww.example.com%2F
    out = []
    for b in s.elem_ords():
        c = chr(b) if b < 128 else ""
        if c and c in _UNRESERVED:
            out.append(c)
        else:
            out.append("%" + _HEX[b // 16] + _HEX[b % 16])
    return "".join(out)

def _date(s):
    return time.parse_time(s[:10] + "T00:00:00Z")

def _fmt(t):
    return t.format("2006-01-02")

def _months_back(t, n):
    # the same day n months earlier (time.time normalises the month)
    return time.time(year = t.year, month = t.month - n, day = t.day)

def _column(d):
    return "search_appearance" if d == "searchAppearance" else d

def _text(x):
    # every metric as text, typed by the model's cast: the API sends 0 and
    # 1 as integers and 0.25 as a float in the same column, and the runtime
    # types a column by its first value
    if x == None:
        return None
    if type(x) == "float" and x == int(x) and abs(x) < 1e15:
        return str(int(x))
    return str(x)

def _dimensions(s):
    dims = []
    for x in s.split(","):
        x = x.strip()
        if not x or x == "date":
            continue
        match = [d for d in DIMENSIONS if d.lower() == x.lower()]
        if not match:
            fail("google_search_console: unknown dimension " + repr(x) + "; use " + ", ".join(DIMENSIONS))
        if match[0] in dims:
            fail("google_search_console: dimension " + repr(x) + " given twice")
        dims.append(match[0])
    return dims

def fetch(site, dimensions, days, page, is_backfill = True, last_value = "", initial_value = ""):
    site = site or env.get("GOOGLE_SEARCH_CONSOLE_SITE")
    if not site:
        fail("google_search_console: a property (or GOOGLE_SEARCH_CONSOLE_SITE) is required, e.g. sc-domain:example.com")
    dims = _dimensions(dimensions)
    lookback = int(days)
    if lookback < 0:
        fail("google_search_console: days must be 0 or more")

    if page.cursor:
        cur = page.cursor
    else:
        # yesterday, pinned at the start of the run. Only final data is
        # asked for, so the last two or three days come back empty until
        # Search Console has finished them, and a later run fetches them.
        end = time.now() - DAY
        oldest = _months_back(end, KEEP_MONTHS)
        start = oldest
        if initial_value and _fmt(_date(initial_value)) > _fmt(oldest):
            start = _date(initial_value)
        if not is_backfill and last_value:
            # fetch the last days again: the merge on row_key replaces a
            # re-fetched row in place
            start = _date(last_value) - DAY * lookback
            if _fmt(start) < _fmt(oldest):
                start = oldest
        cur = {"start": _fmt(start), "end": _fmt(end), "row": 0}
    if cur["start"] > cur["end"]:
        return {"rows": [], "next": None}

    body = {
        "startDate": cur["start"],
        "endDate": cur["end"],
        "dimensions": ["date"] + dims,
        "type": "web",
        "dataState": "final",
        "rowLimit": PAGE_SIZE,
        "startRow": cur["row"],
    }
    resp = http.post("/sites/" + _escape(site) + "/searchAnalytics/query", json = body)
    if not resp.ok:
        fail("google_search_console " + site + " " + cur["start"] + ".." + cur["end"] + ": " + str(resp.status_code) + " " + resp.text)

    got = (resp.json or {}).get("rows") or []
    rows = []
    for r in got:
        keys = r.get("keys") or []
        if len(keys) != 1 + len(dims):
            fail("google_search_console: a row has keys " + json.encode(keys) + " for the dimensions " + json.encode(["date"] + dims))
        row = {"site": site, "date": keys[0]}
        for i, d in enumerate(dims):
            row[_column(d)] = keys[i + 1]
        row["clicks"] = _text(r.get("clicks"))
        row["impressions"] = _text(r.get("impressions"))
        row["ctr"] = _text(r.get("ctr"))
        row["position"] = _text(r.get("position"))
        # the merge key is built here: a @fetch model may only cast columns
        row["row_key"] = json.encode(keys)
        rows.append(row)

    nxt = None
    if len(got) == PAGE_SIZE:
        nxt = {"start": cur["start"], "end": cur["end"], "row": cur["row"] + PAGE_SIZE}
    return {"rows": rows, "next": nxt}
