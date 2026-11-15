# Riksbank SWEA: exchange rates, policy and market rates, government bonds and
# any other series the Swedish Riksbank publishes. Public API, no key:
# https://api.riksbank.se/swea/v1
#
#   FROM riksbank('SEKEURPMI,SEKUSDPMI')
#
# Incremental by date: a run asks only for the days after the last one it has.
# One series per request (the API takes one), the range in chunks of at most a
# year, rate limited to the API's 1 request per second. A range without a
# fixing (weekend, holiday) answers 204, which is no rows, not an error.
API = {
    "base_url": "https://api.riksbank.se/swea/v1",
    "headers": {"Accept": "application/json"},
    "timeout": 60,
    "retry": 3,
    # spread retries over tens of seconds: a 5xx raises after the last attempt
    "backoff": 5,
    "rate_limit": {"requests": 1, "per": "1s"},
    "fetch": {"args": ["series"]},
}

DAY = time.parse_duration("24h")

def _date(s):
    return time.parse_time(s[:10] + "T00:00:00Z")

def _fmt(t):
    return t.format("2006-01-02")

def fetch(series, page, is_backfill=True, last_value="", initial_value=""):
    names = [s.strip() for s in series.split(",") if s.strip()]
    if not names:
        fail("riksbank: no series given")
    if page.cursor:
        cur = page.cursor
    else:
        # the end is pinned at the start of the run: yesterday, so a fixing
        # published later today is fetched tomorrow instead of half today
        end = _fmt(time.now() - DAY)
        start = initial_value or "2024-01-01"
        if not is_backfill and last_value:
            start = _fmt(_date(last_value) + DAY)
        cur = {"i": 0, "from": start, "start": start, "end": end}
    if cur["from"] > cur["end"]:
        return {"rows": [], "next": None}

    s = names[cur["i"]]
    to = _fmt(_date(cur["from"]) + time.parse_duration("8760h"))   # 365 days
    if to > cur["end"]:
        to = cur["end"]
    resp = http.get("/Observations/" + s + "/" + cur["from"] + "/" + to)
    rows = []
    if resp.status_code == 204:
        rows = []
    elif not resp.ok:
        fail("riksbank " + s + " " + cur["from"] + ".." + to + ": " + str(resp.status_code) + " " + resp.text)
    else:
        for o in resp.json or []:
            # the merge key is built here: a @fetch model may only cast columns
            rows.append({"obs_key": s + ":" + o["date"], "series": s, "date": o["date"], "value": o["value"]})

    if to < cur["end"]:
        nxt = {"i": cur["i"], "from": _fmt(_date(to) + DAY), "start": cur["start"], "end": cur["end"]}
    elif cur["i"] + 1 < len(names):
        nxt = {"i": cur["i"] + 1, "from": cur["start"], "start": cur["start"], "end": cur["end"]}
    else:
        nxt = None
    return {"rows": rows, "next": nxt}
