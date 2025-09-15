#!/usr/bin/env python3
import os, re, json, glob, csv
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

# optional: adapters
try:
    import yaml  # pip install pyyaml
    from pathlib import Path
    HAVE_YAML = True
except Exception:
    HAVE_YAML = False
    Path = None

SNAP_DIR = "layer1/snaps"
OUT_JSON = "public/datasets/latest-draws.json"
OUT_CSV  = "public/datasets/latest-draws.csv"
BLOG_HTML= "public/blog/lottery-draws.html"

os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
os.makedirs(os.path.dirname(BLOG_HTML), exist_ok=True)

LAST_N_DAYS = 14
NOW = datetime.now(timezone.utc)
THIS_YEAR = NOW.year

GAME_RULES = {
  "Powerball":      {"main":(5,1,69),   "bonus":(1,1,26), "bonus_name":"Powerball"},
  "Mega Millions":  {"main":(5,1,70),   "bonus":(1,1,25), "bonus_name":"Mega Ball"},
  "Lucky for Life": {"main":(5,1,48),   "bonus":(1,1,18), "bonus_name":"Lucky Ball"},
  "Cash4Life":      {"main":(5,1,60),   "bonus":(1,1,4),  "bonus_name":"Cash Ball"},
  "Lotto America":  {"main":(5,1,52),   "bonus":(1,1,10), "bonus_name":"Star Ball"},
}

GAME_HINTS = [
  ("Powerball", ["powerball","double play"]),
  ("Mega Millions", ["mega","megamillions","mega millions"]),
  ("Lucky for Life", ["lucky for life","luckyforlife"]),
  ("Cash4Life", ["cash4life","cash for life","cash 4 life"]),
  ("Lotto America", ["lotto america","lottoamerica"]),
]

# ----------------- helpers -----------------

def parse_money_html(text: str):
    if not text: return None
    pat = re.compile(r'(?is)(?:estimated\s+)?jackpot[^$0-9]{0,60}([\$£€]?\s?[0-9][0-9,\.]*)')
    m = pat.search(text)
    if not m: return None
    x = m.group(1).replace(',', '').strip().lstrip('$')
    try:
        return int(float(x))
    except Exception:
        return None

def parse_money_general(s: str):
    if not s: return None
    s = re.sub(r'(?i)\best(imated)?\b', '', s)
    m = re.findall(r'[\$£€]?\s*([0-9][0-9,\.]{0,})', s)
    if not m: return None
    x = m[0].replace(',', '')
    try:
        return int(float(x))
    except Exception:
        return None

def sane_date(date_str: str):
    try:
        dt = dateparser.parse(date_str, fuzzy=True)
        if not dt.tzinfo: dt = dt.replace(tzinfo=timezone.utc)
        if dt.year < THIS_YEAR - 1 or dt.year > THIS_YEAR:
            return None
        delta = NOW - dt
        if delta.days < 0 or delta.days > LAST_N_DAYS:
            return None
        return dt.date().isoformat()
    except Exception:
        return None

def validate_numbers(game, nums, bonus):
    rule = GAME_RULES.get(game)
    if not rule:
        return False
    main_count, lo, hi = rule["main"]
    _, blo, bhi = rule["bonus"]
    if len(nums) != main_count: return False
    if not all(isinstance(x, int) and lo <= x <= hi for x in nums): return False
    if bonus is None: return False
    if isinstance(bonus, int): return blo <= bonus <= bhi
    if isinstance(bonus, (list, tuple)) and len(bonus) == 1 and isinstance(bonus[0], int):
        return blo <= bonus[0] <= bhi
    return False

def detect_game(context_text, url):
    ctx = ((context_text or "") + " " + (url or "")).lower()
    for g, hints in GAME_HINTS:
        if any(h in ctx for h in hints):
            return g
    return None

def to_record(game, iso_date, nums, bonus, jackpot, source_url, method, confidence=None):
    nums = list(map(int, nums))
    if not isinstance(bonus, int) and isinstance(bonus, (list,tuple)) and bonus:
        bonus = bonus[0]
    rec = {
      "date": iso_date,
      "game": game,
      "numbers": nums + ([bonus] if isinstance(bonus,int) else []),
      "jackpot_usd": jackpot,
      "winners": None,
      "source_url": source_url,
      "fetched_at": datetime.utcnow().isoformat() + "Z",
      "extraction_method": method,
      "confidence": confidence
    }
    return rec

def dedupe_keep_best(records):
    seen = {}
    rank = {"json":3, "html":2, "vision":1}
    for r in records:
        key = (r.get("game"), r.get("date"), tuple(r.get("numbers") or []))
        if key not in seen or rank.get(r.get("extraction_method"),0) > rank.get(seen[key].get("extraction_method"),0):
            seen[key] = r
    return list(seen.values())

# ----------------- Adapters (YAML) -----------------

ADAPTERS = {}
if HAVE_YAML:
    ADAPTER_DIR = Path("layer2/adapters")
    ADAPTER_DIR.mkdir(parents=True, exist_ok=True)
    for p in ADAPTER_DIR.glob("*.yaml"):
        try:
            cfg = yaml.safe_load(p.read_text())
            host = (cfg or {}).get("host")
            if host:
                ADAPTERS.setdefault(host.lower(), []).append(cfg)
        except Exception:
            pass

def html_text_blocks(soup):
    blocks = []
    for el in soup.find_all(['section','article','table','tbody','tr','ul','ol','div','p','li','main']):
        t = el.get_text(" ", strip=True)
        if t and len(t) > 40:
            blocks.append((el, t))
    page_txt = soup.get_text(" ", strip=True)
    if page_txt and all(page_txt != b[1] for b in blocks):
        blocks.append((soup, page_txt))
    return blocks

def apply_adapter_html(html, url):
    if not HAVE_YAML: return []
    try:
        host = re.sub(r'^https?://', '', url).split('/')[0].lower()
    except Exception:
        return []
    rules = ADAPTERS.get(host, [])
    if not rules: return []

    soup = BeautifulSoup(html, "lxml")
    blocks = html_text_blocks(soup)
    out = []

    for rule in rules:
        game = rule.get("game")
        scope_any = [s.lower() for s in rule.get("scope_contains", [])]
        css_scope = rule.get("css_scope")
        date_rx  = re.compile(rule.get("date_regex"), re.I|re.S)    if rule.get("date_regex")    else None
        nums_rx  = re.compile(rule.get("numbers_regex"), re.I|re.S) if rule.get("numbers_regex") else None
        bonus_rx = re.compile(rule.get("bonus_regex"), re.I|re.S)   if rule.get("bonus_regex")   else None
        jpot_rx  = re.compile(rule.get("jackpot_regex"), re.I|re.S) if rule.get("jackpot_regex") else None

        scopes = []
        if css_scope:
            scopes = [(el, el.get_text(" ", strip=True)) for el in soup.select(css_scope)]
        if not scopes:
            scopes = blocks

        for _, text in scopes:
            low = text.lower()
            if scope_any and not any(k in low for k in scope_any):
                continue

            # date
            iso = None
            if date_rx:
                dm = date_rx.search(text)
                if dm: iso = sane_date(dm.group(1))
            if not iso:
                dm = DATE_RX.search(text)
                if dm: iso = sane_date(dm.group(1))
            if not iso:
                continue

            # numbers
            nums = None
            if nums_rx:
                nm = nums_rx.search(text)
                if nm: nums = [int(x) for x in re.findall(r'\d{1,2}', nm.group(0))]
            if not nums:
                nm = NUMS_RX.search(text)
                if nm: nums = [int(x) for x in re.findall(r'\d{1,2}', nm.group(0))]
            if not nums or len(nums) < 5:
                continue
            nums = nums[:5]

            # bonus
            bval = None
            if bonus_rx:
                bm = bonus_rx.search(text)
                if bm:
                    bb = re.findall(r'\d{1,2}', bm.group(0))
                    bval = int(bb[0]) if bb else None
            if bval is None:
                bm = BONUS_RX.search(text)
                if bm:
                    bval = int(bm.group(2))

            # jackpot
            jackpot = None
            if jpot_rx:
                jm = jpot_rx.search(text)
                if jm:
                    jackpot = parse_money_general(jm.group(0))
            if jackpot is None:
                jackpot = parse_money_html(text)

            if not (game and validate_numbers(game, nums, bval)):
                continue

            out.append(to_record(game, iso, nums, bval, jackpot, url, "html"))
            if out: break
        if out: break

    return out

# ----------------- Lane A: network JSON -----------------

def parse_powerball_recent(items, url):
    out = []
    for it in items:
        date = it.get("field_draw_date") or it.get("draw_date") or it.get("date")
        if not date: continue
        iso = sane_date(str(date))
        if not iso: continue
        nums_raw = it.get("field_winning_numbers") or it.get("winning_numbers") or it.get("numbers") or ""
        nums = [int(x) for x in re.findall(r'\d{1,2}', json.dumps(nums_raw))][:5]
        pb = it.get("field_powerball") or it.get("powerball")
        if isinstance(pb, str):
            m = re.findall(r'\d{1,2}', pb); pb = int(m[0]) if m else None
        if pb is None:
            tail = [int(x) for x in re.findall(r'\d{1,2}', json.dumps(nums_raw))]
            if len(tail) >= 6: pb = tail[5]
        jp = it.get("jackpot") or it.get("estimated_jackpot")
        if isinstance(jp, str): jp = parse_money_general(jp)
        game = "Powerball"
        if validate_numbers(game, nums, pb):
            out.append(to_record(game, iso, nums, pb, jp, url, "json"))
    return out

def parse_megamillions_asmx(obj, url):
    out = []
    if not isinstance(obj, dict) or "d" not in obj: return out
    try:
        inner = json.loads(obj["d"])
    except Exception:
        return out
    candidates = inner if isinstance(inner, list) else [inner]
    for it in candidates:
        date = it.get("drawDate") or it.get("DrawDate") or it.get("draw_date") or it.get("date")
        nums_raw = it.get("winningNumbers") or it.get("WinningNumbers") or it.get("numbers") or ""
        mb = it.get("megaball") or it.get("MegaBall") or it.get("megaBall")
        jp = it.get("jackpot") or it.get("Jackpot") or it.get("estimatedJackpot")
        if isinstance(jp, str): jp = parse_money_general(jp)
        if not date or not nums_raw: continue
        iso = sane_date(str(date))
        if not iso: continue
        nums = [int(x) for x in re.findall(r'\d{1,2}', json.dumps(nums_raw))][:5]
        if isinstance(mb, str):
            m = re.findall(r'\d{1,2}', mb); mb = int(m[0]) if m else None
        game = "Mega Millions"
        if validate_numbers(game, nums, mb):
            out.append(to_record(game, iso, nums, mb, jp, url, "json"))
    return out

def extract_from_network_json(njson: str, url: str):
    out = []
    try:
        data = json.loads(njson)
    except Exception:
        return out

    if isinstance(data, list) and data and isinstance(data[0], dict) and any(k in data[0] for k in ("field_winning_numbers","winning_numbers","field_draw_date","draw_date")):
        out.extend(parse_powerball_recent(data, url))
        return out

    if isinstance(data, dict) and "d" in data and isinstance(data["d"], str):
        try:
            json.loads(data["d"])
            out.extend(parse_megamillions_asmx({"d": data["d"]}, url))
            return out
        except Exception:
            pass

    def rec(o):
        if isinstance(o, dict):
            lower = {k.lower(): k for k in o.keys()}
            date = None
            for dk in ["draw_date","date","drawdate","playdate","drawdateformatted","drawing_date","drawn_date"]:
                if dk in lower: date = str(o[lower[dk]]); break
            nums = None
            for nk in ["winning_numbers","numbers","winningnumbers","field_winning_numbers","winningNumbers"]:
                if nk in lower: nums = o[lower[nk]]; break
            bonus = None
            for bk in ["powerball","megaball","luckyball","cashball","starball","bonus","bonus_ball"]:
                if bk in lower: bonus = o[lower[bk]]; break
            jackpot = None
            for jk in ["jackpot","estimated_jackpot","jackpot_amount","prize","jackpotcashvalue","jackpot_value","estimatedjackpot"]:
                if jk in lower: jackpot = o[lower[jk]]; break

            if date and nums:
                iso = sane_date(date)
                if iso:
                    nums_list = [int(x) for x in re.findall(r'\d{1,2}', json.dumps(nums))]
                    if isinstance(bonus, str):
                        bb = re.findall(r'\d{1,2}', bonus); bonus = int(bb[0]) if bb else None
                    if isinstance(bonus, list):
                        bb = [int(x) for x in re.findall(r'\d{1,2}', json.dumps(bonus))]; bonus = bb[0] if bb else None
                    if isinstance(jackpot, str): jackpot = parse_money_general(jackpot)
                    game = detect_game(json.dumps(o), url)
                    mains = nums_list[:5]
                    if game and validate_numbers(game, mains, bonus):
                        out.append(to_record(game, iso, mains, bonus, jackpot, url, "json"))
            for v in o.values(): rec(v)
        elif isinstance(o, list):
            for v in o: rec(v)
    rec(data)
    return out

# ----------------- Lane B: rendered HTML -----------------

DATE_RX  = re.compile(r'(?i)(?:draw|date|drawing)[^\n]{0,20}\b([A-Za-z]{3,9}\.? \d{1,2}(?:, \d{2,4})?|\d{1,2}/\d{1,2}/\d{2,4}|[A-Za-z]{3}\.? \d{1,2})')
NUMS_RX  = re.compile(r'(\b\d{1,2}\b(?:\s*[,\-•–—]\s*|\s+)){4,6}\b')
BONUS_RX = re.compile(r'(?i)(powerball|mega\s*ball|lucky\s*ball|cash\s*ball|star\s*ball|bonus)[^\d]{0,6}(\d{1,2})')

def extract_from_html(html: str, url: str):
    # Adapter-first
    recs = apply_adapter_html(html, url)
    if recs:
        return recs

    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)

    # Drop obvious 404s/soft errors
    if re.search(r'(?i)\b(404|not found|page wasn.t a winner)\b', text):
        return []

    game = detect_game(text, url)
    if not game:
        return []

    nums_hit = NUMS_RX.search(text)
    if not nums_hit:
        return []
    nums = [int(x) for x in re.findall(r'\d{1,2}', nums_hit.group(0))][:5]

    b = None
    m = BONUS_RX.search(text)
    if m:
        b = int(m.group(2))

    date = None
    dm = DATE_RX.search(text)
    if dm:
        date = dm.group(1)

    iso = sane_date(date) if date else None
    if not iso:
        return []
    if not validate_numbers(game, nums[:5], b):
        return []
    jackpot = parse_money_html(text)

    return [to_record(game, iso, nums[:5], b, jackpot, url, "html")]

# ----------------- Lane C: Vision on screenshot -----------------

def extract_with_vision(img_path: str, url: str):
    try:
        out = os.popen(f"python3 layer2/vision_extract.py '{img_path}'").read()
        data = json.loads(out)
        if isinstance(data, list):
            ok = []
            for r in data:
                iso = sane_date(r.get("date",""))
                if not iso: continue
                game = r.get("game")
                nums = r.get("numbers", [])[:5]
                bonus = r.get("numbers", [None])[-1] if len(r.get("numbers",[]))==6 else r.get("bonus")
                if not (game and validate_numbers(game, nums, bonus)): continue
                r["date"] = iso
                r["source_url"] = r.get("source_url") or url
                r["extraction_method"] = r.get("extraction_method","vision")
                ok.append(r)
            return ok
        return []
    except Exception:
        return []

# ----------------- main -----------------

def main():
    records = []

    for meta_path in glob.glob(os.path.join(SNAP_DIR, "*.meta.json")):
        try:
            meta = json.load(open(meta_path))
        except Exception:
            continue

        base = meta_path[:-10]  # strip ".meta.json"
        url = meta.get("final_url") or meta.get("url")

        # Lane A0: clean body file (from fetch_nationals.py) — preferred over everything
        body_path = base + ".body.json"
        if os.path.exists(body_path):
            try:
                body = open(body_path, "r", encoding="utf-8", errors="ignore").read()
                recs = extract_from_network_json(body, url)
                if recs:
                    records.extend(recs)
                    continue
            except Exception:
                pass

        # Lane A: network JSON captured by snapshotter
        net_path = base + ".network.json"
        if os.path.exists(net_path):
            try:
                nets = json.load(open(net_path))
                got = False
                for n in nets:
                    recs = extract_from_network_json(n.get("body",""), url)
                    if recs:
                        records.extend(recs); got = True
                        break
                if got:
                    continue
            except Exception:
                pass

        # Lane B: HTML
        html_path = base + ".html"
        if os.path.exists(html_path):
            try:
                html = open(html_path, "r", encoding="utf-8", errors="ignore").read()
                recs = extract_from_html(html, url)
                if recs:
                    records.extend(recs)
                    continue
            except Exception:
                pass

        # Lane C: Vision
        img_path = base + ".full.png"
        if os.path.exists(img_path):
            recs = extract_with_vision(img_path, url)
            if recs:
                records.extend(recs)

    clean = dedupe_keep_best(records)

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(clean, f, indent=2)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","game","numbers","jackpot_usd","source_url","extraction_method"])
        for r in clean:
            w.writerow([r.get("date"), r.get("game"), " ".join(map(str, r.get("numbers",[]))), r.get("jackpot_usd"), r.get("source_url"), r.get("extraction_method")])

    rows = "\n".join(
      f"<tr><td>{r.get('date')}</td><td>{r.get('game')}</td><td>{' '.join(map(str,r.get('numbers',[])))}</td><td>{'' if r.get('jackpot_usd') is None else r.get('jackpot_usd')}</td><td><a href='{r.get('source_url')}' rel='nofollow'>src</a></td><td>{r.get('extraction_method')}</td></tr>"
      for r in clean
    )
    html = f"""<!doctype html><meta charset="utf-8"><title>Lottery Draws</title>
    <h1>Latest Lottery Draws</h1>
    <p>Records: {len(clean)} — <a href="/datasets/latest-draws.json">JSON</a> · <a href="/datasets/latest-draws.csv">CSV</a></p>
    <table border="1" cellspacing="0" cellpadding="6">
      <tr><th>Date</th><th>Game</th><th>Numbers</th><th>Jackpot (USD)</th><th>Source</th><th>Method</th></tr>
      {rows}
    </table>
    """
    with open(BLOG_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Wrote {len(clean)} records → {OUT_JSON} / {OUT_CSV} and {BLOG_HTML}")

if __name__ == "__main__":
    main()
