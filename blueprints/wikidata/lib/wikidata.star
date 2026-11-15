# Wikidata Query Service: any SPARQL SELECT query, one row per result.
# Public, no key: https://query.wikidata.org
#
#   FROM wikidata('SELECT ?item ?itemLabel WHERE { ... } LIMIT 100')
#
# One row per binding, two columns per variable: <var> holds the value as text
# (a URI, a literal's lexical form, a blank node label), <var>_qid the entity
# id (Q42, P31, L7) when the value is a Wikidata entity URI and NULL otherwise.
# A variable left unbound by OPTIONAL is NULL in both.
#
# One query is one request. There is no LIMIT/OFFSET paging: the query is sent
# as written, and its own LIMIT is the size of the result. The service stops a
# query after 60 seconds; keep it inside that (see index.md).
API = {
    "base_url": "https://query.wikidata.org",
    "headers": {
        "Accept": "application/sparql-results+json",
        # Wikimedia's User-Agent policy: name the tool and where to reach it
        "User-Agent": "ondatrasql-wikidata-blueprint/1.0 (https://github.com/ondatra-labs/ondatrasql)",
    },
    # longer than the service's own 60 s limit, so its timeout error is read
    "timeout": 90,
    # a 429 carries Retry-After, which the retry honours; a query that timed
    # out is not retried often, since its time counts against the client's quota
    "retry": 2,
    "backoff": 10,
    "rate_limit": {"requests": 1, "per": "1s"},
    "fetch": {"args": ["query"]},
}

ENTITY = "http://www.wikidata.org/entity/"

def _qid(v):
    if v["type"] != "uri" or not v["value"].startswith(ENTITY):
        return None
    return v["value"][len(ENTITY):]

def fetch(query, page, columns=[]):
    if not query.strip():
        fail("wikidata: no query given")
    resp = http.get("/sparql", params={"query": query})
    if not resp.ok:
        fail("wikidata: " + str(resp.status_code) + " " + resp.text[:2000])
    data = resp.json
    if type(data) != "dict" or type(data.get("results")) != "dict":
        fail("wikidata: the answer has no results; only a SELECT query is supported: " + resp.text[:500])

    names = data["head"]["vars"]
    known = {}
    for n in names:
        known[n] = True
    for n in names:
        if n + "_qid" in known:
            fail("wikidata: variable ?" + n + "_qid clashes with the entity id column of ?" + n + "; rename it")
        known[n + "_qid"] = True
    # a column the model reads but the query does not select would be NULL in
    # every row; say so instead
    for c in columns:
        if c["name"] not in known:
            fail("wikidata: column " + c["name"] + " is not a variable of the query (or <var>_qid of one); it selects " + ", ".join(["?" + n for n in names]))

    rows = []
    for b in data["results"]["bindings"]:
        row = {}
        for n in names:
            v = b.get(n)
            row[n] = v["value"] if v else None
            row[n + "_qid"] = _qid(v) if v else None
        rows.append(row)
    return {"rows": rows, "next": None}
