# Peppol Directory: who can receive Peppol e-invoices, per country, plus the
# participant identifier scheme code list (the ICDs such as 0007, 0088,
# 0192). Public, no key: https://directory.peppol.eu
#
#   FROM peppol('participants', 'NO,SE/0007:2021')
#   FROM peppol('schemes')
#
# participants: each entry of the list is a country code, optionally with a
# participant prefix after a slash ('SE/0007:2021' is every Swedish receiver
# whose identifier starts with 0007:2021). One row per participant and
# business entity in that country.
#
# The search API only lets a query page so deep into its results (1000, an
# assumption from the directory's REST docs, not checked against the live
# API). A query with more hits than that is not paged but split: a whole
# country into one query per ICD from the scheme code list, a prefix into one
# query per next character. Each split query searches the prefix with q= and
# keeps only participants whose identifier starts with it, so the splits do
# not overlap. Rows per country are compared with the directory's own count
# at the end, and a difference is printed as a warning.
#
# schemes: the participant identifier schemes from the Peppol code lists at
# https://docs.peppol.eu/edelivery/codelists/. The newest "Participant
# identifier schemes" JSON linked from that page is used, unless
# codelist_url names one.
API = {
    "base_url": "https://directory.peppol.eu",
    "headers": {"Accept": "application/json"},
    "timeout": 60,
    # the directory answered 503 for minutes on 2026-09-06: with backoff 10
    # the retries wait about 10, 20, 40, 80 and 160 seconds before failing
    "retry": 5,
    "backoff": 10,
    "rate_limit": {"requests": 1, "per": "1s"},
    "fetch": {"args": ["dataset", "countries", "codelist_url"], "page_size": 100},
}

CODELISTS = "https://docs.peppol.eu/edelivery/codelists/"

# how far one query can be paged (first index of a page must stay below it)
MAX_RESULTS = 1000

# the characters a prefix is extended with when it has too many hits
CHARS = "0123456789abcdefghijklmnopqrstuvwxyz"

# a prefix this long that still has too many hits is paged as far as the API
# allows, and the warning at the end reports what is missing
MAX_PREFIX = 24

def _get(url, params=None):
    resp = http.get(url, params=params or {})
    if not resp.ok:
        fail("peppol " + url + " " + json.encode(params or {}) + ": " + str(resp.status_code) + " " + resp.text[:500])
    return resp

def _codelist_url():
    # The index page links each release's files; the newest JSON of the
    # participant identifier schemes is the one with the highest vX.Y.
    html = _get(CODELISTS).text
    best = None
    best_ver = None
    for part in html.split('href="')[1:]:
        href = part.split('"')[0]
        name = href.replace("%20", " ").lower()
        if "participant identifier schemes" not in name or not name.endswith(".json"):
            continue
        ver = []
        for n in name[name.rfind(" v") + 2:-len(".json")].split("."):
            ver.append(int(n) if n.isdigit() else 0)
        if best_ver == None or ver > best_ver:
            best, best_ver = href, ver
    if best == None:
        fail("peppol: no participant identifier schemes JSON linked from " + CODELISTS + "; pass its URL as the third argument")
    if best.startswith("http"):
        return best
    if best.startswith("/"):
        return "https://docs.peppol.eu" + best
    return CODELISTS + best

def _schemes(codelist_url):
    resp = _get(codelist_url or _codelist_url())
    doc = resp.json
    if type(doc) != "dict" or type(doc.get("values")) != "list":
        fail("peppol: " + (codelist_url or CODELISTS) + " is not a code list with values: " + resp.text[:300])
    rows = []
    for v in doc["values"]:
        if "iso6523" not in v:
            fail("peppol: code list entry without iso6523, keys " + str(sorted(v.keys())))
        # keys as the code list has them, '-' made '_' so SQL can name them
        rows.append({k.replace("-", "_"): v[k] for k in v})
    return rows

def _countries(countries):
    out = []
    for c in countries.split(","):
        c = c.strip()
        if not c:
            continue
        cc, _, prefix = c.partition("/")
        if len(cc.strip()) != 2:
            fail("peppol: " + repr(c) + " is not a country code (NO) or a country with a prefix (SE/0007:2021)")
        out.append({"cc": cc.strip().upper(), "prefix": prefix.strip().lower(), "rpi": 0, "root": True})
    if not out:
        fail("peppol: no countries given")
    return out

def _rows(matches, cc, prefix):
    rows = []
    for m in matches:
        pid = m["participantID"]["value"]
        if prefix and not pid.lower().startswith(prefix):
            continue   # q= also matches other fields; keep the splits disjoint
        icd, _, ident = pid.partition(":")
        doc_types = [d["value"] for d in m.get("docTypes") or []]
        for e in m.get("entities") or []:
            names = e.get("name") or []
            rows.append({
                "participant_scheme": m["participantID"]["scheme"],
                "participant_id": pid,
                "icd": icd,
                "identifier": ident,
                "name": names[0]["name"] if names else None,
                "country_code": e.get("countryCode"),
                "reg_date": e.get("regDate"),
                "additional_info": e.get("additionalInfo"),
                "doc_types": doc_types,
                "query_country": cc,
            })
    return rows

def _participants(countries, codelist_url, page):
    if page.cursor:
        cur = json.decode(json.encode(page.cursor))   # a copy this call can change
    else:
        cur = {"todo": _countries(countries), "icds": None, "rows": {}, "expect": {}}
    todo = cur["todo"]
    if not todo:
        return {"rows": [], "next": None}
    q = todo.pop(0)
    key = q["cc"] + ("/" + q["prefix"] if q["prefix"] else "")
    params = {"country": q["cc"], "rpc": str(page.size), "rpi": str(q["rpi"])}
    if q["prefix"]:
        params["q"] = q["prefix"]
    body = _get("/search/1.0/json", params).json
    total = int(body["total-result-count"])
    matches = body.get("matches") or []
    if q["root"]:
        q["key"] = key
        cur["expect"][key] = total
        cur["rows"][key] = 0

    rows = []
    if q["rpi"] == 0 and total > MAX_RESULTS and len(q["prefix"]) < MAX_PREFIX:
        # too many hits to page through: split instead of reading this page
        if q["prefix"]:
            exts = [q["prefix"] + ch for ch in CHARS.elems()]
        else:
            if cur["icds"] == None:
                cur["icds"] = sorted([r["iso6523"] for r in _schemes(codelist_url)])
            exts = [icd + ":" for icd in cur["icds"]]
        cur["todo"] = [{"cc": q["cc"], "prefix": p, "rpi": 0, "root": False, "key": q["key"]} for p in exts] + todo
    else:
        rows = _rows(matches, q["cc"], q["prefix"])
        cur["rows"][q["key"]] += len(rows)
        # total counts entities, a page holds participants: a short page or
        # a page that reaches total is the last one
        nxt = (q["rpi"] + 1) * page.size
        if len(matches) == page.size and nxt < total and nxt < MAX_RESULTS:
            cur["todo"] = [{"cc": q["cc"], "prefix": q["prefix"], "rpi": q["rpi"] + 1, "root": False, "key": q["key"]}] + todo

    if not cur["todo"]:
        for k in sorted(cur["expect"].keys()):
            if cur["rows"][k] != cur["expect"][k]:
                print("peppol: " + k + ": the directory counts " + str(cur["expect"][k]) + " entities, fetched " + str(cur["rows"][k]))
        return {"rows": rows, "next": None}
    return {"rows": rows, "next": cur}

def fetch(dataset, page, countries="", codelist_url=""):
    if dataset == "participants":
        return _participants(countries, codelist_url, page)
    if dataset == "schemes":
        return {"rows": _schemes(codelist_url), "next": None}
    fail("peppol: dataset must be 'participants' or 'schemes', not " + repr(dataset))
