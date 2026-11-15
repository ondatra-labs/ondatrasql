# GLEIF: Legal Entity Identifier (LEI) records and the registration-authority
# (RA) code list from the Global LEI Foundation. Public API, no key:
# https://api.gleif.org/api/v1
#
#   FROM gleif('lei-records', 'SE,NO')        one row per LEI in those countries
#   FROM gleif('registration-authorities')    one row per RA code and jurisdiction
#
# Both are incremental on fetched_on, the UTC day a run fetched a row: a run
# on a day that already has a fetch makes no request. lei-records then asks,
# per country, only for records whose registration.lastUpdateDate is on or
# after that day minus LAG (GLEIF publishes some updates a day or so after
# their lastUpdateDate). The RA list changes rarely; it is fetched again
# once RA_REFRESH has passed.
#
# JSON:API paging, 200 per page. The API is said to stop paging at 10 000
# results per query, so after MAX_PAGES pages a query starts over from the
# lastUpdateDate day it reached. A record seen twice in a run (that restart,
# or a record updated while the run pages) is returned once.
# Rate limited below GLEIF's 60 requests per minute.
API = {
    "base_url": "https://api.gleif.org/api/v1",
    "headers": {"Accept": "application/vnd.api+json"},
    "timeout": 60,
    "retry": 3,
    "backoff": 5,
    "rate_limit": {"requests": 1, "per": "1200ms"},
    "fetch": {"args": ["dataset", "countries"], "page_size": 200},
}

DAY = time.parse_duration("24h")
LAG = 2 * DAY
RA_REFRESH = 7 * DAY
MAX_PAGES = 50

def _date(s):
    return time.parse_time(s[:10] + "T00:00:00Z")

def _day(t):
    return t.in_location("UTC").format("2006-01-02")

def _compact(day):
    # 2026-11-14 -> 20261114, the date format GLEIF's filters take
    return day[:10].replace("-", "")

def _get(d, *keys):
    for k in keys:
        if d == None:
            return None
        d = d.get(k)
    return d

def _page(path, params, size, number):
    params = dict(params)
    params["page[size]"] = str(size)
    params["page[number]"] = str(number)
    resp = http.get(path, params=params)
    if not resp.ok:
        fail("gleif " + path + " " + str(params) + ": " + str(resp.status_code) + " " + resp.text)
    body = resp.json or {}
    pg = _get(body, "meta", "pagination") or {}
    more = (pg.get("currentPage") or 0) < (pg.get("lastPage") or 0)
    return body.get("data") or [], more

def gleif_lei(countries, page, is_backfill, last_value, initial_value):
    if page.cursor:
        cur = page.cursor
    else:
        codes = [c.strip().upper() for c in countries.split(",") if c.strip()]
        if not codes:
            fail("gleif: no countries given, e.g. gleif('lei-records', 'SE,NO')")
        for c in codes:
            if len(c) != 2 or not c.isalpha():
                fail("gleif: " + repr(c) + " is not an ISO 3166-1 alpha-2 country code")
        today = _day(time.now())
        if is_backfill or not last_value:
            start = (initial_value or "1970-01-01")[:10]
        elif last_value[:10] >= today:
            # fetched today already: up to date, no request
            return {"rows": [], "next": None}
        else:
            start = _day(_date(last_value) - LAG)
        cur = {"codes": codes, "i": 0, "start": _compact(start), "from": _compact(start),
               "page": 1, "today": today, "seen": {}}

    country = cur["codes"][cur["i"]]
    data, more = _page("/lei-records", {
        "filter[entity.legalAddress.country]": country,
        "filter[registration.lastUpdateDate]": ">=" + cur["from"],
        "sort": "registration.lastUpdateDate",
    }, page.size, cur["page"])

    seen = cur["seen"]
    rows = []
    last_update = ""
    for item in data:
        a = item.get("attributes") or {}
        e = a.get("entity") or {}
        r = a.get("registration") or {}
        lei = a.get("lei") or item.get("id")
        last_update = r.get("lastUpdateDate") or last_update
        if lei in seen:
            continue
        seen[lei] = True
        rows.append({
            "lei": lei,
            "name": _get(e, "legalName", "name"),
            "country": _get(e, "legalAddress", "country"),
            "city": _get(e, "legalAddress", "city"),
            "postal_code": _get(e, "legalAddress", "postalCode"),
            "jurisdiction": e.get("jurisdiction"),
            "legal_form": _get(e, "legalForm", "id"),
            "entity_status": e.get("status"),
            "registered_as": e.get("registeredAs"),
            "registered_at": _get(e, "registeredAt", "id"),
            "registration_status": r.get("status"),
            "last_update": r.get("lastUpdateDate"),
            "next_renewal": r.get("nextRenewalDate"),
            "fetched_on": cur["today"],
        })

    nxt = dict(cur)
    if more and cur["page"] < MAX_PAGES:
        nxt["page"] = cur["page"] + 1
    elif more:
        # past the paging limit: query again from the day this one reached
        day = _compact(last_update)
        if day <= cur["from"]:
            fail("gleif: more than " + str(MAX_PAGES * page.size) + " " + country +
                 " records updated on " + cur["from"] + "; the API cannot page that far")
        nxt["from"] = day
        nxt["page"] = 1
    elif cur["i"] + 1 < len(cur["codes"]):
        nxt["i"] = cur["i"] + 1
        nxt["from"] = cur["start"]
        nxt["page"] = 1
    else:
        nxt = None
    return {"rows": rows, "next": nxt}

def gleif_ra(page, is_backfill, last_value):
    if page.cursor:
        cur = page.cursor
    else:
        today = _day(time.now())
        if not is_backfill and last_value and _day(_date(last_value) + RA_REFRESH) > today:
            # fetched within RA_REFRESH: no request
            return {"rows": [], "next": None}
        cur = {"page": 1, "today": today, "seen": {}}

    data, more = _page("/registration-authorities", {}, page.size, cur["page"])
    seen = cur["seen"]
    rows = []
    for item in data:
        a = item.get("attributes") or {}
        code = a.get("code") or item.get("id")
        for j in a.get("jurisdictions") or [{}]:
            # the merge key is built here: a @fetch model may only cast columns
            key = code + ":" + (j.get("jurisdiction") or j.get("countryCode") or "")
            if key in seen:
                continue
            seen[key] = True
            rows.append({
                "ra_key": key,
                "ra_code": code,
                "international_name": a.get("internationalName"),
                "local_name": a.get("localName"),
                "organization": a.get("internationalOrganizationName"),
                "local_organization": a.get("localOrganizationName"),
                "website": a.get("website"),
                "country_code": j.get("countryCode"),
                "country": j.get("country"),
                "jurisdiction": j.get("jurisdiction"),
                "fetched_on": cur["today"],
            })

    nxt = None
    if more:
        nxt = dict(cur)
        nxt["page"] = cur["page"] + 1
    return {"rows": rows, "next": nxt}

def fetch(dataset, page, countries="", is_backfill=True, last_value="", initial_value=""):
    if dataset == "lei-records":
        return gleif_lei(countries, page, is_backfill, last_value, initial_value)
    if dataset == "registration-authorities":
        return gleif_ra(page, is_backfill, last_value)
    fail("gleif: dataset " + repr(dataset) + " is not 'lei-records' or 'registration-authorities'")
