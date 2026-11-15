# Mistral OCR: PDFs to typed rows, one row per document. The SELECT is the
# extraction schema: each typed column becomes a property of the JSON Schema
# Mistral fills in, so a cast in the model decides what the API is asked for.
# https://docs.mistral.ai/capabilities/document_ai/annotations/
#
#   FROM mistral_ocr('data/invoices/*.pdf')
#
# Four columns are filled by the lib, not by the schema; keep their names:
#   source_file          the path, as glob() returns it (the @group_key)
#   source_hash          md5 of the file's contents
#   pages                pages Mistral processed (and billed)
#   document_annotation  the extraction as Mistral returned it, raw JSON
#
# Only a new or changed file (by md5) is sent to the API. An unchanged file is
# rebuilt from the document_annotation already in the target, so tracked keeps
# its row; a file that is gone from the folder drops out of the table. A run
# that rebuilds the target (the first one, or after the SQL changed) sends
# every file again, since the schema may have changed.
#
# Per file: upload it (purpose ocr), run OCR with the schema, delete the
# upload. At most BATCH files per page.
API = {
    "base_url": "https://api.mistral.ai",
    "auth": {"env": "MISTRAL_API_KEY"},
    "headers": {"Accept": "application/json"},
    "timeout": 300,
    "retry": 3,
    "backoff": 5,
    "rate_limit": {"requests": 5, "per": "1s"},
    "fetch": {"args": ["pattern"]},
}

MODEL = "mistral-ocr-latest"
BATCH = 10
LIB_COLUMNS = ["source_file", "source_hash", "pages", "document_annotation"]

def _strict(s):
    # every field is asked for, and may be null: a field the document lacks
    # (no due date) comes back NULL instead of made up
    if s.get("type") == "object":
        props = {k: {"anyOf": [_strict(v), {"type": "null"}]} for k, v in s["properties"].items()}
        return {"type": "object", "properties": props, "required": sorted(props.keys()), "additionalProperties": False}
    if s.get("type") == "array":
        return {"type": "array", "items": _strict(s["items"])}
    return s

def _schema(columns):
    props = {}
    for col in columns:
        if col["name"] not in LIB_COLUMNS:
            props[col["name"]] = lib_helpers.to_json_schema(col["type"])
    if not props:
        fail("mistral_ocr: the SELECT has no column to extract besides " + ", ".join(LIB_COLUMNS))
    return _strict({"type": "object", "properties": props})

def _row(path, digest, pages, annotation, fields):
    if type(annotation) == "dict":
        # lookup() of a column cast to JSON instead of VARCHAR
        annotation = json.encode(annotation)
    data = json.decode(annotation) if annotation else {}
    if type(data) != "dict":
        fail("mistral_ocr " + path + ": document_annotation is not a JSON object: " + annotation)
    row = {"source_file": path, "source_hash": digest, "pages": pages, "document_annotation": annotation}
    for f in fields:
        row[f] = data.get(f)
    return row

def _extract(path, schema):
    up = http.upload("/v1/files", file=path, fields={"purpose": "ocr"})
    if not up.ok:
        fail("mistral_ocr upload " + path + ": " + str(up.status_code) + " " + up.text)
    file_id = up.json["id"]
    resp = http.post("/v1/ocr", json={
        "model": MODEL,
        "document": {"type": "file", "file_id": file_id},
        "document_annotation_format": {
            "type": "json_schema",
            "json_schema": {"name": "extraction", "schema": schema},
        },
    })
    # delete the upload before looking at the OCR answer, so a failed
    # extraction does not leave the file behind
    gone = http.delete("/v1/files/" + file_id)
    if not resp.ok:
        fail("mistral_ocr ocr " + path + ": " + str(resp.status_code) + " " + resp.text)
    if not gone.ok:
        fail("mistral_ocr delete file " + file_id + " (" + path + "): " + str(gone.status_code) + " " + gone.text)
    usage = resp.json.get("usage_info") or {}
    return usage.get("pages_processed"), resp.json.get("document_annotation") or ""

def fetch(pattern, page, columns=[], target="", is_backfill=True):
    names = [c["name"] for c in columns]
    for c in LIB_COLUMNS:
        if c not in names:
            fail("mistral_ocr: the SELECT needs " + c + " (" + ", ".join(LIB_COLUMNS) + " are filled by the lib)")
    fields = [n for n in names if n not in LIB_COLUMNS]
    schema = _schema(columns)

    rows = []
    if page.cursor:
        todo = page.cursor["todo"]
    else:
        files = glob(pattern)
        if not files:
            # nothing to read: keep what the target has (empty_result
            # defaults to no_change), a mistyped pattern must not wipe it
            return {"rows": [], "next": None}
        hashes = {f: md5_file(f) for f in files}
        known, stored, pages = {}, {}, {}
        if not is_backfill:
            known = lookup(table=target, key="source_file", value="source_hash", where=files)
            same = [f for f in files if known.get(f) == hashes[f]]
            if same:
                stored = lookup(table=target, key="source_file", value="document_annotation", where=same)
                pages = lookup(table=target, key="source_file", value="pages", where=same)
        todo = []
        for f in files:
            if f in stored:
                rows.append(_row(f, hashes[f], pages.get(f), stored[f], fields))
            else:
                todo.append({"file": f, "hash": hashes[f]})

    for t in todo[:BATCH]:
        n, annotation = _extract(t["file"], schema)
        rows.append(_row(t["file"], t["hash"], n, annotation, fields))
    rest = todo[BATCH:]
    return {"rows": rows, "next": {"todo": rest} if rest else None}
