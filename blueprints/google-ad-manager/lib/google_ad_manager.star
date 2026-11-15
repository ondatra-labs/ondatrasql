# Google Ad Manager: a historical report through the Ad Manager REST API v1,
# with a Google service account:
# https://developers.google.com/ad-manager/api/beta
#
#   FROM google_ad_manager('', 'AD_UNIT_NAME', 'AD_SERVER_IMPRESSIONS,AD_SERVER_CLICKS', 3)
#
# Args: the network codes (comma separated; '' is every network the service
# account is an API user on), the dimensions and metrics (comma separated
# API names; DATE is always the first dimension), and days, how many days
# before the newest date already loaded a run fetches again, since Ad
# Manager revises recent days.
#
# Async: submit() creates and runs one report per network, check() polls the
# runs until all are done, fetch_result() reads the rows page by page. Each
# row carries its network, the date, every dimension and every metric under
# its API name in lower case, and row_key, which the model merges on.
API = {
    "base_url": "https://admanager.googleapis.com/v1",
    "auth": {
        "service_account": {"env": "GAM_KEY_FILE"},
        "scope": "https://www.googleapis.com/auth/admanager",
    },
    "headers": {"Accept": "application/json"},
    "timeout": 120,
    "retry": 3,
    "backoff": 5,
    "fetch": {
        "args": ["networks", "dimensions", "metrics", "days"],
        "async": True,
        "poll_interval": "5s",
        "poll_timeout": "30m",
        "poll_backoff": 2,
    },
}

DAY = time.parse_duration("24h")

PAGE_SIZE = 10000

def _date(s):
    return time.parse_time(s[:10] + "T00:00:00Z")

def _fmt(t):
    return t.format("2006-01-02")

def _ymd(t):
    return {"year": t.year, "month": t.month, "day": t.day}

def _names(s):
    return [x.strip().upper() for x in s.split(",") if x.strip()]

def _networks(networks):
    codes = [x.strip() for x in networks.split(",") if x.strip()]
    if codes:
        return codes
    # no codes given: every network the service account can read
    token = ""
    for _ in range(100):
        params = {"pageToken": token} if token else {}
        resp = http.get("/networks", params=params)
        if not resp.ok:
            fail("google_ad_manager: list networks: " + str(resp.status_code) + " " + resp.text)
        for n in resp.json.get("networks", []):
            codes.append(n.get("networkCode") or n["name"].split("/")[-1])
        token = resp.json.get("nextPageToken", "")
        if not token:
            break
    if not codes:
        fail("google_ad_manager: the service account is an API user on no network; add it in Ad Manager (Admin > Global settings)")
    return codes

def submit(networks, dimensions, metrics, days, target="", is_backfill=True, last_value="", initial_value=""):
    dims = [d for d in _names(dimensions) if d != "DATE"]
    mets = _names(metrics)
    if not mets:
        fail("google_ad_manager: no metrics given")
    lookback = int(days)
    if lookback < 0:
        fail("google_ad_manager: days must be 0 or more")

    # yesterday, pinned at the start of the run: today is still filling up
    end = time.now() - DAY
    start = _date(initial_value) if initial_value else end
    if not is_backfill and last_value:
        # fetch the last days again: Ad Manager revises recent days, and
        # the merge on row_key replaces a re-fetched row in place
        start = _date(last_value) - DAY * lookback
    if _fmt(start) > _fmt(end):
        abort()

    jobs = []
    for code in _networks(networks):
        body = {
            "displayName": "ondatrasql " + (target or "google_ad_manager") + " " + _fmt(start) + ".." + _fmt(end),
            "visibility": "HIDDEN",
            "reportDefinition": {
                "dimensions": ["DATE"] + dims,
                "metrics": mets,
                "reportType": "HISTORICAL",
                "timeZoneSource": "PUBLISHER",
                "dateRange": {"fixed": {"startDate": _ymd(start), "endDate": _ymd(end)}},
            },
        }
        resp = http.post("/networks/" + code + "/reports", json=body)
        if not resp.ok:
            fail("google_ad_manager: create report on network " + code + ": " + str(resp.status_code) + " " + resp.text)
        report = "/networks/" + code + "/reports/" + str(resp.json["reportId"])
        resp = http.post(report + ":run", json={})
        if not resp.ok:
            fail("google_ad_manager: run " + report + ": " + str(resp.status_code) + " " + resp.text)
        jobs.append({"network": code, "operation": resp.json["name"]})
    return {"jobs": jobs, "dimensions": dims, "metrics": mets}

def check(job_ref):
    results = []
    for job in job_ref["jobs"]:
        resp = http.get("/" + job["operation"])
        if not resp.ok:
            fail("google_ad_manager: poll " + job["operation"] + ": " + str(resp.status_code) + " " + resp.text)
        op = resp.json
        if not op.get("done"):
            return None
        if op.get("error"):
            fail("google_ad_manager: report on network " + job["network"] + " failed: " + json.encode(op["error"]))
        results.append({"network": job["network"], "result": op["response"]["reportResult"]})
    return {"results": results, "dimensions": job_ref["dimensions"], "metrics": job_ref["metrics"]}

def _value(v):
    # one of stringValue, intValue (an int64, so a JSON string), doubleValue,
    # boolValue; an empty value is NULL
    if "intValue" in v:
        return int(v["intValue"])
    for k in ["stringValue", "doubleValue", "boolValue"]:
        if k in v:
            return v[k]
    return None

def fetch_result(result_ref, page):
    cur = page.cursor or {"i": 0, "token": ""}
    res = result_ref["results"][cur["i"]]
    params = {"pageSize": str(PAGE_SIZE)}
    if cur["token"]:
        params["pageToken"] = cur["token"]
    resp = http.get("/" + res["result"] + ":fetchRows", params=params)
    if not resp.ok:
        fail("google_ad_manager: fetch rows of " + res["result"] + ": " + str(resp.status_code) + " " + resp.text)

    dims = result_ref["dimensions"]
    mets = result_ref["metrics"]
    rows = []
    for r in resp.json.get("rows", []):
        dv = [_value(v) for v in r.get("dimensionValues", [])]
        groups = r.get("metricValueGroups", [])
        mv = [_value(v) for v in groups[0].get("primaryValues", [])] if groups else []
        # DATE is an int like 20251210
        d = str(dv[0])
        date = d[0:4] + "-" + d[4:6] + "-" + d[6:8]
        row = {"network_code": res["network"], "date": date}
        for i, name in enumerate(dims):
            row[name.lower()] = dv[i + 1] if i + 1 < len(dv) else None
        for i, name in enumerate(mets):
            row[name.lower()] = mv[i] if i < len(mv) else None
        # the merge key is built here: a @fetch model may only cast columns
        row["row_key"] = json.encode([res["network"], date] + [row[n.lower()] for n in dims])
        rows.append(row)

    token = resp.json.get("nextPageToken", "")
    if token:
        nxt = {"i": cur["i"], "token": token}
    elif cur["i"] + 1 < len(result_ref["results"]):
        nxt = {"i": cur["i"] + 1, "token": ""}
    else:
        nxt = None
    return {"rows": rows, "next": nxt}
