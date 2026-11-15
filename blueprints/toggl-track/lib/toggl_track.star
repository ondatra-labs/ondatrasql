# Toggl Track: one workspace's time entries through the Reports API v3
# detailed search, authenticated with a personal API token:
# https://engineering.toggl.com/docs/reports/detailed_reports
#
#   FROM toggl_track('<workspace id>')
#
# Without a workspace id (or with ''), TOGGL_WORKSPACE_ID names it.
#
# The API answers with rows that group one or more time entries under the
# same user, project, description, billable flag and tags. The lib returns
# one row per time entry: the entry's id, seconds, start, stop and at (last
# changed), date (the local day of start), and every field of its group
# (user_id, username, project_id, project_name, client_name, description,
# billable, tag_ids, ...). The model picks the ones it keeps.
#
# By month: the first run fetches from @incremental_initial up to today,
# every later run fetches last month and this month again, so entries
# edited or added late replace what was loaded. One request per month and
# page; the next page starts at the X-Next-Row-Number the API returns.
API = {
    "base_url": "https://api.track.toggl.com/reports/api/v3",
    # Authorization: Basic base64(<token>:api_token)
    "auth": {
        "user": {"env": "TOGGL_API_TOKEN"},
        "pass": "api_token",
    },
    "headers": {"Accept": "application/json"},
    "timeout": 60,
    "retry": 3,
    "backoff": 5,
    # 240 requests an hour per user (fewer on the free plan): spread them
    "rate_limit": {"requests": 1, "per": "1s"},
    "fetch": {
        "args": ["workspace_id"],
        "page_size": 50,
    },
}

DAY = time.parse_duration("24h")

def _date(s):
    return time.parse_time(s[:10] + "T00:00:00Z")

def _fmt(t):
    return t.format("2006-01-02")

def _month_start(d):
    return d[:8] + "01"

def _next_month(d):
    y, m = int(d[:4]), int(d[5:7])
    if m == 12:
        return str(y + 1) + "-01-01"
    return str(y) + "-" + ("0" if m < 9 else "") + str(m + 1) + "-01"

def _prev_month(d):
    y, m = int(d[:4]), int(d[5:7])
    if m == 1:
        return str(y - 1) + "-12-01"
    return str(y) + "-" + ("0" if m < 11 else "") + str(m - 1) + "-01"

def _header(resp, name):
    for k, v in resp.headers.items():
        if k.lower() == name:
            return v[0] if type(v) == "list" else v
    return ""

def fetch(workspace_id, page, is_backfill=True, initial_value=""):
    workspace_id = workspace_id or env.get("TOGGL_WORKSPACE_ID")
    if not workspace_id:
        fail("toggl_track: a workspace id (or TOGGL_WORKSPACE_ID) is required")
    if page.cursor:
        cur = page.cursor
    else:
        today = _fmt(time.now())
        # every run fetches last month and this month again: late edits land
        start = _prev_month(today)
        if is_backfill and initial_value:
            start = initial_value[:10]
        # the end is pinned at the start of the run, a day ahead so an entry
        # dated tomorrow in a time zone east of UTC is not left out
        cur = {"from": start, "end": _fmt(time.now() + DAY), "row": 0, "skip": [], "edge": []}
    if cur["from"] >= cur["end"]:
        return {"rows": [], "next": None}

    # one month at a time: from cur["from"] to the first day of the next month.
    # Both chunks ask for that day, so whichever way the API counts end_date
    # no day falls between two months; ids already returned are skipped.
    to = _next_month(cur["from"])
    if to > cur["end"]:
        to = cur["end"]
    body = {
        "start_date": cur["from"],
        "end_date": to,
        "enrich_response": True,
        "page_size": page.size,
    }
    if cur["row"]:
        body["first_row_number"] = cur["row"]
    resp = http.post("/workspace/" + str(workspace_id) + "/search/time_entries", json=body)
    if not resp.ok:
        fail("toggl_track " + cur["from"] + ".." + to + ": " + str(resp.status_code) + " " + resp.text)

    edge_from = _fmt(_date(to) - DAY)
    skip = {i: True for i in cur["skip"]}
    edge = list(cur["edge"])
    rows = []
    for g in resp.json or []:
        group = {k: v for k, v in g.items() if k != "time_entries"}
        for te in g.get("time_entries") or []:
            if str(te["id"]) in skip:
                continue
            row = dict(group)
            row.update(te)
            row["date"] = te["start"][:10]
            rows.append(row)
            # an entry near the end of the month may come back in the next one
            if row["date"] >= edge_from:
                edge.append(str(te["id"]))

    nxt_row = _header(resp, "x-next-row-number")
    if nxt_row:
        nxt = {"from": cur["from"], "end": cur["end"], "row": int(nxt_row), "skip": cur["skip"], "edge": edge}
    elif to < cur["end"]:
        nxt = {"from": to, "end": cur["end"], "row": 0, "skip": edge, "edge": []}
    else:
        nxt = None
    return {"rows": rows, "next": nxt}
