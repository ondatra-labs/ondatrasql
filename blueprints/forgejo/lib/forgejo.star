# Forgejo and Gitea: an organisation's (or a user's) repositories and their
# pull requests, from any instance's REST API v1. Public data needs no key:
# https://forgejo.org/docs/latest/user/api-usage/
#
#   FROM forgejo('repos', 'forgejo', 'https://codeberg.org')   one row per repository
#   FROM forgejo('pulls', 'forgejo', 'https://codeberg.org')   one row per pull request
#
# The base URL is the third argument, or FORGEJO_URL when it is empty; with or
# without /api/v1. FORGEJO_TOKEN, when set, is sent as "Authorization: token
# <t>" (private repositories); without it only public data is read.
# The owner is tried as an organisation first and, when the instance answers
# 404, as a user.
#
# pulls is incremental on updated_at. The API cannot filter pull requests by
# time, so each repository's list is read newest update first
# (sort=recentupdate) and stops at the first one older than the bound: on a
# backfill @incremental_initial, later the newest updated_at minus LOOKBACK.
# Pages of at most 50 (the API's cap), until X-Total-Count is reached.
API = {
    "timeout": 60,
    "retry": 3,
    "backoff": 2,
    "rate_limit": {"requests": 2, "per": "1s"},
    "fetch": {"args": ["kind", "owner", "base_url"], "page_size": 50},
}

LOOKBACK = time.parse_duration("1h")
MAX_LIMIT = 50

def _base(base_url):
    base = (base_url or env.get("FORGEJO_URL", default="") or "").strip().rstrip("/")
    if not base:
        fail("forgejo: no base URL; pass it as the third argument or set FORGEJO_URL")
    if not base.endswith("/api/v1"):
        base = base + "/api/v1"
    return base

def _headers():
    h = {"Accept": "application/json"}
    token = env.get("FORGEJO_TOKEN", default="") or ""
    if token:
        h["Authorization"] = "token " + token
    return h

def _ts(s):
    # an API timestamp (RFC 3339) or a DuckDB TIMESTAMPTZ cast to VARCHAR
    # ("2026-11-14 10:20:30.5+01", the form last_value has) as a time
    s = s.strip().replace(" ", "T")
    if len(s) == 10:
        s = s + "T00:00:00Z"
    if not s.endswith("Z"):
        sign = max(s.rfind("+"), s.rfind("-"))
        if sign > 10:
            off = s[sign + 1:]
            if len(off) == 2:
                s = s + ":00"
            elif len(off) == 4 and ":" not in off:
                s = s[:sign + 3] + ":" + off[2:]
        else:
            s = s + "Z"
    return time.parse_time(s)

def _total(resp):
    for k, v in resp.headers.items():
        if k.lower() == "x-total-count":
            if type(v) == "list":
                v = v[0]
            return int(v)
    return None

def _get(base, path, number, size):
    resp = http.get(base + path, headers=_headers(),
                    params={"page": str(number), "limit": str(size)})
    if not resp.ok:
        return resp, [], False
    items = resp.json or []
    total = _total(resp)
    if total != None:
        more = number * size < total and len(items) > 0
    else:
        more = len(items) == size
    return resp, items, more

def _repos_page(cur, size):
    # one page of the owner's repositories; finds out on the first page
    # whether the owner is an organisation or a user
    if not cur.get("path"):
        org = "/orgs/" + cur["owner"] + "/repos"
        resp, items, more = _get(cur["base"], org, cur["rp"], size)
        if resp.ok:
            cur["path"] = org
            return items, more
        if resp.status_code != 404:
            fail("forgejo GET " + org + ": " + str(resp.status_code) + " " + resp.text)
        cur["path"] = "/users/" + cur["owner"] + "/repos"
    resp, items, more = _get(cur["base"], cur["path"], cur["rp"], size)
    if not resp.ok:
        if resp.status_code == 404 and cur["path"].startswith("/users/"):
            fail("forgejo: " + repr(cur["owner"]) + " is neither an organisation nor a user on " + cur["base"])
        fail("forgejo GET " + cur["path"] + ": " + str(resp.status_code) + " " + resp.text)
    return items, more

def _login(u):
    if u == None:
        return None
    return u.get("login")

def _ref(b):
    if b == None:
        return None
    return b.get("ref")

def _repo_row(r):
    return {
        "id": r.get("id"),
        "full_name": r.get("full_name"),
        "owner": _login(r.get("owner")),
        "name": r.get("name"),
        "description": r.get("description"),
        "private": r.get("private"),
        "fork": r.get("fork"),
        "mirror": r.get("mirror"),
        "archived": r.get("archived"),
        "empty": r.get("empty"),
        "default_branch": r.get("default_branch"),
        "language": r.get("language"),
        "stars_count": r.get("stars_count"),
        "forks_count": r.get("forks_count"),
        "open_issues_count": r.get("open_issues_count"),
        "open_pr_counter": r.get("open_pr_counter"),
        "has_pull_requests": r.get("has_pull_requests"),
        "html_url": r.get("html_url"),
        "created_at": r.get("created_at"),
        "updated_at": r.get("updated_at"),
    }

def _pull_row(repo, p):
    return {
        "id": p.get("id"),
        "repo": repo,
        "number": p.get("number"),
        "title": p.get("title"),
        "state": p.get("state"),
        "draft": p.get("draft"),
        "author": _login(p.get("user")),
        "base_ref": _ref(p.get("base")),
        "head_ref": _ref(p.get("head")),
        "merged": p.get("merged"),
        "merged_by": _login(p.get("merged_by")),
        "comments": p.get("comments"),
        "html_url": p.get("html_url"),
        "created_at": p.get("created_at"),
        "updated_at": p.get("updated_at"),
        "closed_at": p.get("closed_at"),
        "merged_at": p.get("merged_at"),
    }

def forgejo_repos(cur, size):
    items, more = _repos_page(cur, size)
    nxt = None
    if more:
        nxt = dict(cur)
        nxt["rp"] = cur["rp"] + 1
    return {"rows": [_repo_row(r) for r in items], "next": nxt}

def forgejo_pulls(cur, size):
    if cur["repos"] == None:
        # first call: list the repositories that have pull requests
        repos = []
        for _ in range(1000):
            items, more = _repos_page(cur, size)
            for r in items:
                if r.get("has_pull_requests") != False:
                    repos.append(r.get("full_name"))
            if not more:
                break
            cur["rp"] = cur["rp"] + 1
        cur["repos"] = repos
    if cur["i"] >= len(cur["repos"]):
        return {"rows": [], "next": None}

    repo = cur["repos"][cur["i"]]
    since = _ts(cur["since"])
    path = "/repos/" + repo + "/pulls"
    resp = http.get(cur["base"] + path, headers=_headers(), params={
        "state": "all", "sort": "recentupdate",
        "page": str(cur["pp"]), "limit": str(size),
    })
    if not resp.ok:
        fail("forgejo GET " + path + ": " + str(resp.status_code) + " " + resp.text)
    items = resp.json or []
    total = _total(resp)
    rows = []
    older = False
    for p in items:
        if _ts(p["updated_at"]) < since:
            # newest update first: the rest of this repository is older still
            older = True
            break
        rows.append(_pull_row(repo, p))
    if total != None:
        more = cur["pp"] * size < total and len(items) > 0
    else:
        more = len(items) == size

    nxt = dict(cur)
    if more and not older:
        nxt["pp"] = cur["pp"] + 1
    elif cur["i"] + 1 < len(cur["repos"]):
        nxt["i"] = cur["i"] + 1
        nxt["pp"] = 1
    else:
        nxt = None
    return {"rows": rows, "next": nxt}

def fetch(kind, owner, page, base_url="", is_backfill=True, last_value="", initial_value=""):
    if page.cursor:
        cur = dict(page.cursor)
    else:
        owner = owner.strip()
        if not owner or "/" in owner:
            fail("forgejo: owner " + repr(owner) + " is not an organisation or user name")
        cur = {"base": _base(base_url), "owner": owner, "path": "", "rp": 1}
        if kind == "pulls":
            if is_backfill or not last_value:
                since = _ts(initial_value or "1970-01-01")
            else:
                since = _ts(last_value) - LOOKBACK
            cur.update({"repos": None, "i": 0, "pp": 1,
                        "since": since.in_location("UTC").format("2006-01-02T15:04:05Z07:00")})
    size = min(page.size or MAX_LIMIT, MAX_LIMIT)
    if kind == "repos":
        return forgejo_repos(cur, size)
    if kind == "pulls":
        return forgejo_pulls(cur, size)
    fail("forgejo: kind " + repr(kind) + " is not 'repos' or 'pulls'")
