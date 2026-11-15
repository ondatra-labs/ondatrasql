# Google Sheets: read a range into a table, and write a model's rows back to
# a range. Sheets API v4, authenticated as a Google service account:
# https://developers.google.com/sheets/api/reference/rest
#
#   FROM google_sheets('Orders!A:E', '<spreadsheet id>')        -- @fetch
#   -- @push: google_sheets('Summary', '<spreadsheet id>')     -- @push
#
# Without a spreadsheet id (or with ''), GOOGLE_SHEETS_SPREADSHEET_ID names
# it. The id comes last so it can be left out: @push drops an empty ''.
#
# Share the spreadsheet with the service account's email (Editor to push,
# Viewer is enough to read). GOOGLE_SHEETS_KEY_FILE is the path of its JSON
# key.
#
# Read: the first row of the range is the header; every later row becomes a
# row keyed by the header, every cell as text for the model's casts to type.
# Cells come back unformatted (19.9, not "$19.90"), date cells as the text
# they show.
#
# Push: a full replace. The range is cleared, then the header (the model's
# columns) and the rows are written from its top-left cell. Writing alone
# would leave the old rows below when the new result is shorter.
API = {
    "base_url": "https://sheets.googleapis.com/v4",
    "auth": {
        "service_account": {"env": "GOOGLE_SHEETS_KEY_FILE"},
        "scope": "https://www.googleapis.com/auth/spreadsheets",
    },
    "headers": {"Accept": "application/json"},
    "timeout": 60,
    "retry": 3,
    "backoff": 2,
    # the read quota is 60 requests per minute per user
    "rate_limit": {"requests": 60, "per": "60s"},
    "fetch": {"args": ["range", "spreadsheet_id"]},
    "push": {
        "args": ["range", "spreadsheet_id"],
        # a push replaces the whole range, so only a full-replace kind fits
        "supported_kinds": ["table"],
        # one batch holds the whole result (the old rows' deletes included)
        "batch_size": 10000,
        "batch_mode": "atomic",
    },
}

_HEX = "0123456789ABCDEF"

# A1 notation goes in the URL path: keep what a path allows ('Sheet 1'!A1:E
# keeps its quotes, ! and :), percent-encode the rest (spaces, #, ?, /, %,
# non-ASCII sheet names).
_PATH_SAFE = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~!$&'()*+,;=:@"

def _escape(s):
    out = []
    for b in s.elem_ords():
        c = chr(b) if b < 128 else ""
        if c and c in _PATH_SAFE:
            out.append(c)
        else:
            out.append("%" + _HEX[b // 16] + _HEX[b % 16])
    return "".join(out)

def _values_url(spreadsheet_id, range):
    spreadsheet_id = spreadsheet_id or env.get("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id or not range:
        fail("google_sheets: a range and a spreadsheet id (or GOOGLE_SHEETS_SPREADSHEET_ID) are required")
    return "/spreadsheets/" + _escape(spreadsheet_id) + "/values/" + _escape(range)

def _check(resp, what):
    if not resp.ok:
        fail("google_sheets " + what + ": " + str(resp.status_code) + " " + resp.text)

def _text(x):
    # every cell as text, typed by the model's cast: a column can mix number
    # and text cells, and the runtime types a column by its first value
    if type(x) == "float" and x == int(x) and abs(x) < 1e15:
        return str(int(x))
    if type(x) == "bool":
        return "true" if x else "false"
    return str(x)

def fetch(range, spreadsheet_id="", columns=[]):
    resp = http.get(_values_url(spreadsheet_id, range), params={
        "majorDimension": "ROWS",
        "valueRenderOption": "UNFORMATTED_VALUE",
        # without it a date cell is a serial number (46327)
        "dateTimeRenderOption": "FORMATTED_STRING",
    })
    _check(resp, "read " + range)
    values = (resp.json or {}).get("values") or []
    if not values:
        fail("google_sheets: " + range + " is empty; its first row must be the header")

    header = []
    for i, h in enumerate(values[0]):
        name = str(h).strip()
        if name and name in [n for _, n in header]:
            fail("google_sheets: " + range + " has the header " + repr(name) + " twice")
        if name:
            header.append((i, name))
    names = [n for _, n in header]
    for c in columns:
        if c["name"] not in names:
            fail("google_sheets: column " + repr(c["name"]) + " is not in the header of " + range + ": " + repr(names))

    rows = []
    for v in values[1:]:
        # the API drops trailing empty cells, and an empty row is []
        if not [x for x in v if x != ""]:
            continue
        row = {}
        for i, name in header:
            x = v[i] if i < len(v) else None
            row[name] = None if x == None or x == "" else _text(x)
        rows.append(row)
    # the range is read whole, so no rows means the sheet has none
    return {"rows": rows, "next": None, "empty_result": "delete_missing"}

def _cell(v):
    if v == None:
        return ""
    if type(v) in ("string", "int", "float", "bool"):
        return v
    return str(v)

def _sort_key(r, names):
    # rows arrive unordered: sort by the columns in order, NULLs last
    return [(r.get(n) == None, r.get(n) if r.get(n) != None else 0) for n in names]

def push(rows=[], batch_number=1, columns=[], range="", spreadsheet_id=""):
    url = _values_url(spreadsheet_id, range)
    names = [c["name"] for c in columns]
    if not names and rows:
        names = sorted([k for k in rows[0] if not k.startswith("__ondatra_")])
    # a table push is the old rows' deletes and the new rows' inserts
    new = [r for r in rows if r["__ondatra_change_type"] in ("insert", "update_postimage")]
    new = sorted(new, key=lambda r: _sort_key(r, names))
    values = [[_cell(r.get(n)) for n in names] for r in new]

    if batch_number > 1:
        # a result larger than batch_size: the first batch replaced the
        # range, the later ones go below it
        if values:
            resp = http.post(url + ":append", params={
                "valueInputOption": "RAW",
                "insertDataOption": "OVERWRITE",
            }, json={"majorDimension": "ROWS", "values": values})
            _check(resp, "append to " + range)
        return None

    _check(http.post(url + ":clear", json={}), "clear " + range)
    resp = http.put(url, params={"valueInputOption": "RAW"},
                    json={"majorDimension": "ROWS", "values": [names] + values})
    _check(resp, "write " + range)
    return None
