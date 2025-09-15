#!/usr/bin/env python3
import os, re, json, csv, gzip, zlib
from datetime import datetime, timezone
from dateutil import parser as dateparser

# Optional Brotli (Powerball often returns br-compressed bodies)
try:
    import brotli as _brotli
except Exception:
    _brotli = None

SNAP_DIR = "layer1/snaps"
OUT_JSON = "public/datasets/latest-draws.json"
OUT_CSV  = "public/datasets/latest-draws.csv"
BLOG_HTML= "public/blog/lottery-draws.html"

os.makedirs("public/datasets", exist_ok=True)
os.makedirs("public/blog", exist_ok=True)

NOW = datetime.now(timezone.utc)
THIS_YEAR = NOW.year

# ---------------- helpers ----------------

def sane_date(s):
    try:
        dt = dateparser.parse(str(s), fuzzy=True)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        if dt.year < THIS_YEAR - 1 or dt.year > THIS_YEAR:
            return None
        if (NOW - dt).days < 0 or (NOW - dt).days > 14:
            return None
        return dt.date().isoformat()
    except Exception:
        return None

def parse_money(s):
    if s is None:
        return None
    s = re.sub(r'(?i)\best(imated)?\b', '', str(s))
    m = re.search(r'[\$£€]?\s*([0-9][0-9,\.]+)', s)
    if not m:
        return None
    try:
        return int(float(m.group(1).replace(',', '')))
    except Exception:
        return None

def read_text_any(path):
    """Read bytes; try utf-8; then gzip; then brotli; then deflate."""
    b = open(path, 'rb').read()

    # try plain utf-8 first
    try:
        s = b.decode('utf-8')
        # if it looks textual, keep it
        control_bytes = sum(ch < 9 or (13 < ch < 32) for ch in b[:200])
        if control_bytes < 5:
            return s
    except Exception:
        pass

    # gzip magic header
    try:
        if b.startswith(b'\x1f\x8b'):
            return gzip.decompress(b).decode('utf-8', 'replace')
    except Exception:
        pass

    # brotli (common on powerball.com)
    if _brotli is not None:
        try:
            return _brotli.decompress(b).decode('utf-8', 'replace')
        except Exception:
            pass

    # deflate/zlib (wbits=47 handles gzip/zlib)
    try:
        return zlib.decompress(b, 47).decode('utf-8', 'replace')
    except Exception:
        pass

    # last resort
    return b.decode('utf-8', 'replace')

def as_json(body_text: str):
    """Coerce text to JSON; unwrap ASMX {'d':'...'}; strip junk before first bracket; try fragments."""
    if not isinstance(body_text, str):
        return None
    b = body_text.lstrip('\ufeff').strip()

    # Quick HTML guard (so we log a helpful message upstream)
    if b[:200].lower().startswith('<!doctype html') or b[:20].lower().startswith('<html'):
        return None

    # Strip any junk before the first JSON bracket (XSSI, preambles, etc.)
    i1, i2 = b.find('{'), b.find('[')
    starts = [x for x in (i1, i2) if x != -1]
    if starts:
        i = min(starts)
        if i > 0:
            b = b[i:].strip()

    # Direct parse
    try:
        obj = json.loads(b)
        if isinstance(obj, dict) and 'd' in obj:
            inner = obj['d']
            if isinstance(inner, str):
                try:
                    return json.loads(inner)
                except Exception:
                    return None
            return inner
        return obj
    except Exception:
        pass

    # Try successive minimal JSON fragments non-greedily
    for m in re.finditer(r'(\{[\s\S]*?\}|\[[\s\S]*?\])', b):
        frag = m.group(1)
        try:
            obj = json.loads(frag)
            if isinstance(obj, dict) and 'd' in obj:
                inner = obj['d']
                if isinstance(inner, str):
                    try:
                        return json.loads(inner)
                    except Exception:
                        return None
                return inner
            return obj
        except Exception:
            continue

    return None

def rec(game, iso, mains, bonus, jackpot, url):
    return {
        "date": iso,
        "game": game,
        "numbers": mains + ([bonus] if isinstance(bonus, int) else []),
        "jackpot_usd": jackpot,
        "winners": None,
        "source_url": url,
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "extraction_method": "json",
        "confidence": None,
    }

def infer_url_from_slug(slug):
    return "https://" + slug.replace("_", "/").replace("https///", "https://")

# ---------------- parsers ----------------

def parse_powerball_payload(data, url):
    out = []
    arr = data if isinstance(data, list) else (data.get("data") if isinstance(data, dict) else None)
    if not isinstance(arr, list):
        return out
    for it in arr:
        iso = sane_date(it.get("field_draw_date") or it.get("draw_date") or it.get("date"))
        if not iso:
            continue
        nums_s = it.get("field_winning_numbers") or it.get("winning_numbers") or it.get("numbers") or ""
        allnums = [int(x) for x in re.findall(r'\d{1,2}', json.dumps(nums_s))]
        pb = it.get("field_powerball") or it.get("powerball")
        if isinstance(pb, str):
            mm = re.findall(r'\d{1,2}', pb)
            pb = int(mm[0]) if mm else None
        if pb is None and len(allnums) >= 6:
            pb = allnums[5]
        mains = allnums[:5]
        if len(mains) == 5 and 1 <= min(mains) <= 69 and max(mains) <= 69 and isinstance(pb, int) and 1 <= pb <= 26:
            jp = it.get("jackpot") or it.get("estimated_jackpot")
            if isinstance(jp, str):
                jp = parse_money(jp)
            out.append(rec("Powerball", iso, mains, pb, jp, url))
    return out

def parse_megamillions_payload(data, url):
    out = []
    arr = data if isinstance(data, list) else [data]
    for it in arr:
        if not isinstance(it, dict):
            continue

        # ASMX inner bundle: {"Drawing":{N1..N5, MBall, PlayDate}, "Jackpot":{...}}
        if "Drawing" in it and isinstance(it["Drawing"], dict):
            d = it["Drawing"]
            iso = sane_date(d.get("PlayDate") or d.get("DrawDate") or d.get("Date"))
            if not iso:
                continue
            try:
                mains = [int(d.get(f"N{i}")) for i in range(1, 6)]
            except Exception:
                continue
            mb = d.get("MBall")
            try:
                mb = int(mb) if mb is not None else None
            except Exception:
                mb = None

            jp_obj = it.get("Jackpot", {})
            jp = jp_obj.get("CurrentPrizePool") or jp_obj.get("jackpot") or jp_obj.get("Jackpot")
            if isinstance(jp, str):
                jp = parse_money(jp)

            if len(mains) == 5 and 1 <= min(mains) <= 70 and max(mains) <= 70 and isinstance(mb, int) and 1 <= mb <= 25:
                out.append(rec("Mega Millions", iso, mains, mb, jp, url))
            continue

        # Flat variant with winningNumbers + megaball
        iso = sane_date(it.get("drawDate") or it.get("DrawDate") or it.get("date"))
        if not iso:
            continue
        nums_s = it.get("winningNumbers") or it.get("WinningNumbers") or it.get("numbers") or ""
        allnums = [int(x) for x in re.findall(r'\d{1,2}', json.dumps(nums_s))]
        mb = it.get("megaball") or it.get("MegaBall") or it.get("megaBall")
        if isinstance(mb, str):
            mm = re.findall(r'\d{1,2}', mb)
            mb = int(mm[0]) if mm else None
        if not isinstance(mb, int) and len(allnums) >= 6:
            mb = allnums[5]
        mains = allnums[:5]
        if len(mains) == 5 and 1 <= min(mains) <= 70 and max(mains) <= 70 and isinstance(mb, int) and 1 <= mb <= 25:
            jp = it.get("jackpot") or it.get("Jackpot") or it.get("estimatedJackpot")
            if isinstance(jp, str):
                jp = parse_money(jp)
            out.append(rec("Mega Millions", iso, mains, mb, jp, url))
    return out

def parse_cash4life_payload(data, url):
    out = []
    arr = data if isinstance(data, list) else []
    for it in arr:
        iso = sane_date(it.get("draw_date") or it.get("DrawDate") or it.get("date"))
        if not iso:
            continue
        nums_s = it.get("winning_numbers") or it.get("WinningNumbers") or it.get("numbers") or ""
        allnums = [int(x) for x in re.findall(r'\d{1,2}', json.dumps(nums_s))]
        cash = it.get("cash_ball") or it.get("cashBall") or (allnums[5] if len(allnums) >= 6 else None)
        if isinstance(cash, str):
            mm = re.findall(r'\d{1,2}', cash)
            cash = int(mm[0]) if mm else None
        mains = allnums[:5]
        if len(mains) == 5 and 1 <= min(mains) <= 60 and max(mains) <= 60 and isinstance(cash, int) and 1 <= cash <= 4:
            out.append(rec("Cash4Life", iso, mains, cash, None, url))
    return out

# ---------------- main ----------------

def main():
    records = []

    for fname in sorted(os.listdir(SNAP_DIR)):
        if not fname.endswith(".body.json"):
            continue
        path = os.path.join(SNAP_DIR, fname)
        slug = fname[:-10]
        meta = os.path.join(SNAP_DIR, slug + ".meta.json")
        url = None
        if os.path.exists(meta):
            try:
                m = json.load(open(meta))
                url = m.get("final_url") or m.get("url")
            except Exception:
                pass
        url = url or infer_url_from_slug(slug)

        body_text = read_text_any(path)
        data = as_json(body_text)
        if data is None:
            head = (body_text or "")[:120].replace("\n"," ")
            print(f"[INGEST] {fname} → could not parse JSON (head: {head!r})")
            continue

        before = len(records)
        # Try all parsers; whichever matches adds rows
        records += parse_powerball_payload(data, url)
        records += parse_megamillions_payload(data, url)
        records += parse_cash4life_payload(data, url)
        added = len(records) - before
        print(f"[INGEST] {fname} → +{added} rows")

    # Write outputs
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "game", "numbers", "jackpot_usd", "source_url", "extraction_method"])
        for r in records:
            w.writerow([
                r["date"],
                r["game"],
                " ".join(map(str, r["numbers"])),
                r["jackpot_usd"],
                r["source_url"],
                r["extraction_method"]
            ])
    rows = "\n".join(
        f"<tr><td>{r['date']}</td><td>{r['game']}</td><td>{' '.join(map(str, r['numbers']))}</td>"
        f"<td>{'' if r['jackpot_usd'] is None else r['jackpot_usd']}</td>"
        f"<td><a href='{r['source_url']}' rel='nofollow'>src</a></td><td>{r['extraction_method']}</td></tr>"
        for r in records
    )
    open(BLOG_HTML, "w", encoding="utf-8").write(
        f"<!doctype html><meta charset='utf-8'><title>Lottery Draws</title>"
        f"<h1>Latest Lottery Draws</h1>"
        f"<p>Records: {len(records)} — <a href='/datasets/latest-draws.json'>JSON</a> · "
        f"<a href='/datasets/latest-draws.csv'>CSV</a></p>"
        f"<table border='1' cellspacing='0' cellpadding='6'>"
        f"<tr><th>Date</th><th>Game</th><th>Numbers</th><th>Jackpot (USD)</th><th>Source</th><th>Method</th></tr>"
        f"{rows}</table>"
    )
    print(f"Wrote {len(records)} records from *.body.json → {OUT_JSON}")

if __name__ == "__main__":
    main()
