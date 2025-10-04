#!/usr/bin/env python3
import csv, io, re, sys, textwrap
from urllib.request import urlopen

URL = "https://api.data.amsterdam.nl/v1/gebieden/buurten?_format=csv"

def fetch_csv(u: str) -> str:
    with urlopen(u) as r:
        b = r.read()
    for enc in ("utf-8", "latin-1", "windows-1252"):
        try:
            return b.decode(enc)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("Could not decode CSV bytes")

def norm_name(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "")).strip()
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s)  # drop trailing "(...)"
    return s

def looks_like_ams_buurt_code(val: str) -> bool:
    # Accept BU0363.... (Amsterdam buurt codes)
    return isinstance(val, str) and val.upper().startswith("BU0363")

def is_ams_gemeente(val: str) -> bool:
    if not isinstance(val, str):
        return False
    v = val.upper().strip()
    return v in {"0363", "GM0363", "AMSTERDAM"}

def main():
    print("Downloading buurten CSV…", file=sys.stderr)
    text = fetch_csv(URL)
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)

    if not rows:
        sys.exit("CSV came back empty.")

    # Try to find a column that holds buurt codes like BU0363xxxx
    code_cols = [c for c in reader.fieldnames if any(
        looks_like_ams_buurt_code(r.get(c, "")) for r in rows[:200]
    )]

    # Try to find a gemeente column that identifies Amsterdam
    gem_cols = [c for c in reader.fieldnames if any(
        is_ams_gemeente(r.get(c, "")) for r in rows[:200]
    )]

    # Filter candidates
    ams_rows = []
    if code_cols:
        for r in rows:
            if any(looks_like_ams_buurt_code(r.get(c, "")) for c in code_cols):
                ams_rows.append(r)

    # If none matched via BU0363, try gemeente code/name columns
    if not ams_rows and gem_cols:
        for r in rows:
            if any(is_ams_gemeente(r.get(c, "")) for c in gem_cols):
                ams_rows.append(r)

    if not ams_rows:
        print("\nNo Amsterdam rows found with the naive filter.", file=sys.stderr)
        print("CSV headers I see:", file=sys.stderr)
        print(", ".join(reader.fieldnames), file=sys.stderr)
        print("\nSample row:", file=sys.stderr)
        from itertools import islice
        for sample in islice(rows, 1):
            for k, v in sample.items():
                print(f"- {k}: {v}", file=sys.stderr)
        sys.exit(1)

    # Find the best name column
    name_cols_pref = ["naam", "naamNL", "naam_nl", "buurt_naam", "buurtnaam", "name"]
    name_col = next((c for c in name_cols_pref if c in reader.fieldnames), None)
    if not name_col:
        # Fallback: pick the first texty-looking column with varied values
        candidates = []
        for c in reader.fieldnames:
            vals = { (r.get(c) or "").strip() for r in ams_rows[:200] }
            if len(vals) > 20:
                candidates.append(c)
        name_col = candidates[0] if candidates else reader.fieldnames[0]

    names = []
    for r in ams_rows:
        nm = norm_name(r.get(name_col, ""))
        if nm:
            names.append(nm)

    unique = sorted(set(names), key=str.casefold)

    out_path = "out/LOCATIONS.amsterdam.js"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("LOCATIONS = [\n")
        for i, n in enumerate(unique):
            sep = "," if i < len(unique) - 1 else ""
            f.write(f'  "{n}, Amsterdam, Netherlands"{sep}\n')
        f.write("]\n")

    print(f"✓ Wrote {out_path} with {len(unique)} neighbourhoods.", file=sys.stderr)

if __name__ == "__main__":
    main()

