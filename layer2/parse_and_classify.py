#!/usr/bin/env python3
"""
Layer 2 — Parse & Classify (Lottery)
- Reads latest Layer 1 run (uses sidecars for URL + fetched_at)
- Parses known sources into a unified schema
- Writes latest-draws.json and latest-draws.csv

Schema per record:
  date (YYYY-MM-DD), game, numbers [ints], jackpot_usd (int|None),
  winners (int|None), source_url, fetched_at
"""

import json, csv, re, pathlib, datetime, html
from urllib.parse import urlparse

BASE   = pathlib.Path(".")
L1_OUT = BASE / "layer1" / "out"
L2_OUT = BASE / "layer2" / "out"

# ---------- config knobs ----------
STATE_SCAN_WINDOW = 2500     # chars before/after each game label on state pages
HARD_SCAN_WINDOW  = 6000     # larger window for very spread-out markup (fallback)
JSON_MAX_RECORDS  = 500      # safety cap per file for generic JSON harvesting

# ---------- utilities ----------

MONTHS = ("jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec")

def latest_run_dir(root: pathlib.Path) -> pathlib.Path:
    runs = [p for p in root.iterdir() if p.is_dir()]
    if not runs:
        raise SystemExit("No layer1 runs found")
    return sorted(runs)[-1]

def load_sidecar_meta(p: pathlib.Path) -> dict | None:
    m = p.with_suffix(".meta.json")
    if m.exists():
        try:
            return json.loads(m.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None

def to_int_list(maybe_iter):
    out = []
    for x in (maybe_iter or []):
        try:
            out.append(int(x))
        except Exception:
            pass
    return out

def _coerce_int(text):
    if text is None:
        return None
    s = re.sub(r"[^\d]", "", str(text))
    return int(s) if s.isdigit() else None

def normalize_date(any_text: str | None) -> str:
    """Return best-effort YYYY-MM-DD from various shapes."""
    if not any_text:
        return datetime.date.today().isoformat()

    s = str(any_text)

    # ISO or YYYY-MM-DD-ish
    m = re.search(r"(\d{4})[-/](\d{2})[-/](\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # MM/DD/YYYY or M/D/YY(YY)
    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b", s)
    if m:
        mm, dd, yy = m.groups()
        yyyy = yy if len(yy) == 4 else f"20{yy:0>2}"
        return f"{int(yyyy):04d}-{int(mm):02d}-{int(dd):02d}"

    # Month DD, YYYY
    m = re.search(
        r"\b(" + "|".join(MONTHS) + r")[a-z]*\s+(\d{1,2}),\s*(\d{4})",
        s, flags=re.I
    )
    if m:
        mon_txt, dd, yyyy = m.groups()
        month_index = [mth for mth in MONTHS].index(mon_txt.lower()[:3]) + 1
        return f"{int(yyyy):04d}-{month_index:02d}-{int(dd):02d}"

    # "Draw Date: 09/13/2025" style
    m = re.search(r"draw\s*date[^0-9]{0,8}(\d{1,2}/\d{1,2}/\d{2,4})", s, flags=re.I)
    if m:
        return normalize_date(m.group(1))

    # Fallback: first 10 chars
    return s[:10]

def make_record(date, game, numbers, jackpot_usd, winners, source_url, fetched_at):
    return {
        "date": str(date),
        "game": game,
        "numbers": to_int_list(numbers),
        "jackpot_usd": jackpot_usd if isinstance(jackpot_usd, int) else None,
        "winners": winners if isinstance(winners, int) else None,
        "source_url": source_url,
        "fetched_at": fetched_at,
    }

def html_to_text(raw_html: str) -> str:
    """Rough HTML→text: drop scripts/styles, replace tags with spaces, unescape entities, collapse whitespace."""
    if not raw_html:
        return ""
    t = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", raw_html)
    t = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", t)
    # replace tags with spaces so numbers separated by tags still join with whitespace
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    t = html.unescape(t)
    t = re.sub(r"[ \t\f\v]+", " ", t)
    t = re.sub(r"\s*\n+\s*", "\n", t)
    return t

def first_json_blob(text: str) -> dict | list | None:
    """
    Try to extract the first JSON object/array from HTML/text, including JSON-LD.
    """
    t = text.strip()
    # whole body is JSON?
    if t.startswith("{") or t.startswith("["):
        try:
            return json.loads(t)
        except Exception:
            pass

    # Look inside <script> tags for JSON quickly
    scripts = re.findall(r"<script[^>]*>(.*?)</script>", text, flags=re.S|re.I)
    for sc in scripts:
        sc_t = sc.strip()
        if not sc_t:
            continue
        if sc_t.startswith("{") or sc_t.startswith("["):
            for _ in (0, 1):
                try:
                    return json.loads(sc_t)
                except Exception:
                    sc_t = sc_t.replace("&quot;", '"').replace("&amp;", "&")

        # ASMX sometimes: {"d":"...json..."}
        m_d = re.search(r'"\s*d\s*"\s*:\s*"(.+)"\s*}', sc, flags=re.S)
        if m_d:
            try:
                inner = m_d.group(1)
                inner = inner.encode("utf-8").decode("unicode_escape")
                return json.loads(inner)
            except Exception:
                pass

    # Generic fallback: first {...} or [...]
    m = re.search(r"(\{.*\}|\[.*\])", text, flags=re.S)
    if m:
        j = m.group(1)
        try:
            return json.loads(j)
        except Exception:
            try:
                j2 = json.loads(j)
                return j2 if isinstance(j2, (dict, list)) else None
            except Exception:
                return None
    return None

def extract_numbers_generic(text: str, count_main: int = 5) -> list[int] | None:
    """
    Look for patterns like "01 12 23 34 45 + 10" or "1, 12, 23, 34, 45 10".
    Returns the first plausible [count_main + bonus] set.
    """
    s = re.sub(r"\s+", " ", text)
    patterns = [
        r"(?P<a>\d{1,2})[,\s]+(?P<b>\d{1,2})[,\s]+(?P<c>\d{1,2})[,\s]+(?P<d>\d{1,2})[,\s]+(?P<e>\d{1,2})\s*(?:[\+\-–]\s*|\s*\(|\s+)(?P<x>\d{1,2})\)?",
        r"(?P<a>\d{1,2})\s+(?P<b>\d{1,2})\s+(?P<c>\d{1,2})\s+(?P<d>\d{1,2})\s+(?P<e>\d{1,2})\s+(?P<x>\d{1,2})",
    ]
    for pat in patterns:
        m = re.search(pat, s)
        if m:
            nums = [int(m.group(k)) for k in ("a","b","c","d","e","x")]
            if all(1 <= n <= 75 for n in nums[:count_main]) and 1 <= nums[-1] <= 30:
                return nums
    return None

def _numbers_near_keyword(text: str, keyword: str) -> list[int] | None:
    """
    Find 5 numbers plus one bonus number when a given keyword (Lucky Ball / Star Ball / Cash Ball / Powerball)
    appears nearby (within ~40 chars after 5 numbers).
    """
    t = re.sub(r"\s+", " ", text)
    pat = (
        r"(?P<a>\d{1,2})[,\s]+(?P<b>\d{1,2})[,\s]+(?P<c>\d{1,2})[,\s]+"
        r"(?P<d>\d{1,2})[,\s]+(?P<e>\d{1,2}).{0,40}?"
        + re.escape(keyword) +
        r".{0,10}?(?P<x>\d{1,2})"
    )
    m = re.search(pat, t, flags=re.I)
    if not m:
        return None
    nums = [int(m.group(k)) for k in ("a","b","c","d","e","x")]
    if all(1 <= n <= 75 for n in nums[:5]) and 1 <= nums[-1] <= 30:
        return nums
    return None

def _sniff_date_from_text(text: str) -> str:
    for pat in [
        r"Draw\s*Date[^0-9]{0,8}(\d{1,2}/\d{1,2}/\d{2,4})",
        r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2},\s*\d{4}\b",
        r"\b\d{4}-\d{2}-\d{2}\b",
    ]:
        m = re.search(pat, text, flags=re.I)
        if m:
            return normalize_date(m.group(1) if m.lastindex else m.group(0))
    return normalize_date(text)

# ---------- NEW: generic JSON harvesting helpers ----------

DATE_KEYS = {
    "draw_date","drawdate","date","field_draw_date","field_date","post_date",
    "drawDate","DrawDate","Date","Drawdate"
}

JACKPOT_KEYS = {
    "jackpot","estimated_jackpot","field_jackpot","prize","annuity_jackpot","estimatedAnnuity",
    "jackpot_prize","current_jackpot","estimatedJackpot","Jackpot","Prize"
}

WINNER_KEYS = {
    "winners","winner_count","winners_count","num_winners","number_of_winners","Winners"
}

# numbers-like keys often found in JSON
NUMBER_KEYS = {
    "winning_numbers","numbers","results","drawn_numbers","drawnNumbers",
    "field_winning_numbers","white_balls","whiteballs","whiteBalls",
    "red_ball","redball","redBall","powerball","pb","powerPlay",
    "mega_ball","megaBall","mb","megaplier",
    "lucky_ball","luckyBall","cash_ball","cashBall","star_ball","starBall",
}

GAME_KEYWORDS = [
    ("Powerball",      ("powerball","pb")),
    ("Mega Millions",  ("mega millions","mega-millions","megamillions","mb","megaball")),
    ("Lucky for Life", ("lucky for life","lucky-for-life","luckyball")),
    ("Lotto America",  ("lotto america","star ball")),
    ("Cash4Life",      ("cash4life","cash 4 life","cash ball")),
]

def _looks_like_json(body: bytes) -> bool:
    t = body.lstrip()[:1]
    return t in (b"{", b"[")

def _iter_dicts_anywhere(obj):
    """Yield all dict nodes inside a JSON-like structure."""
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _iter_dicts_anywhere(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from _iter_dicts_anywhere(it)

def _extract_numbers_from_value(v):
    """
    Try very hard to pull a 5+1 set from a JSON value (string/list/object).
    Returns list[int] or None.
    """
    # already a list of ints?
    if isinstance(v, list):
        flat = []
        for x in v:
            if isinstance(x, int):
                flat.append(x)
            elif isinstance(x, str) and x.strip().isdigit():
                flat.append(int(x.strip()))
        # If it already looks like 5+1 (or 5+2 for EuroMillions, but we still store 6 for consistency)
        if 5 <= len(flat) <= 7:
            # try to choose first 6
            cand = flat[:6]
            if len(cand) >= 6 and all(1 <= n <= 75 for n in cand[:5]) and 1 <= cand[5] <= 35:
                return cand
        # fallback: None here; we will also try stringification below

    # strings like "01 12 23 34 45 10" or "01,12,23,34,45 + 10"
    if isinstance(v, str):
        cand = extract_numbers_generic(v)
        if cand:
            return cand

    # objects like {"white_balls":[...], "red_ball":10} etc.
    if isinstance(v, dict):
        white = None
        bonus = None

        # try common patterns
        for key in ("white_balls","whiteBalls","numbers","winning_numbers","field_winning_numbers"):
            if key in v:
                if isinstance(v[key], list):
                    white = [int(x) for x in v[key] if str(x).isdigit()]
                elif isinstance(v[key], str):
                    nums = re.findall(r"\d{1,2}", v[key])
                    white = [int(x) for x in nums]

        for key in ("powerball","pb","red_ball","redBall","mega_ball","mb","megaBall","lucky_ball","cash_ball","star_ball","starBall"):
            if key in v and str(v[key]).strip():
                s = re.findall(r"\d{1,2}", str(v[key]))
                if s:
                    bonus = int(s[0])

        if white and len(white) >= 5 and bonus:
            cand = white[:5] + [bonus]
            if all(1 <= n <= 75 for n in cand[:5]) and 1 <= cand[5] <= 35:
                return cand

        # Fallback: stringify dict and scan
        cand = extract_numbers_generic(json.dumps(v))
        if cand:
            return cand

    # final fallback: None
    return None

def _extract_date_from_row(row: dict):
    # try known keys first
    for k in row.keys():
        if k in DATE_KEYS:
            val = row.get(k)
            if val:
                return normalize_date(val)
    # try any key with "date" in it
    for k, v in row.items():
        if isinstance(k, str) and "date" in k.lower() and v:
            return normalize_date(v)
    # look for date-ish string anywhere in the row
    txt = json.dumps(row)
    return _sniff_date_from_text(txt)

def _extract_jackpot_winners(row: dict):
    jackpot = None
    for k in row.keys():
        if k in JACKPOT_KEYS and row.get(k) is not None:
            jackpot = _coerce_int(row.get(k))
            if jackpot:
                break
    winners = None
    for k in row.keys():
        if k in WINNER_KEYS and isinstance(row.get(k), (int, str)):
            w = _coerce_int(row.get(k))
            if isinstance(w, int):
                winners = w
                break
    return jackpot, winners

def _guess_game_from_context(meta: dict, nearby_text: str = "") -> str:
    host = urlparse(meta.get("final_url") or meta.get("url") or "").netloc.lower()
    path = urlparse(meta.get("final_url") or meta.get("url") or "").path.lower()
    ctx = " ".join([host, path, nearby_text.lower()])

    for game, needles in GAME_KEYWORDS:
        if any(n in ctx for n in needles):
            return game

    # host-based hints
    if "powerball" in ctx:
        return "Powerball"
    if "mega" in ctx and "million" in ctx:
        return "Mega Millions"
    if "luckyforlife" in ctx or "lucky-for-life" in ctx:
        return "Lucky for Life"
    if "lottoamerica" in ctx:
        return "Lotto America"
    if "cash4life" in ctx or "cash-4-life" in ctx:
        return "Cash4Life"

    # state sites — leave generic; higher layers can filter later
    return "Unknown"

def parse_json_generic(body_bytes: bytes, meta: dict) -> list[dict]:
    """
    Walk any JSON, try to synthesize records from dict-ish rows that contain date + numbers,
    grab jackpot/winners if present. Aggressive but capped.
    """
    try:
        root = json.loads(body_bytes.decode("utf-8", errors="ignore"))
    except Exception:
        # maybe JSON is embedded in HTML
        text = body_bytes.decode("utf-8", errors="ignore")
        blob = first_json_blob(text)
        if blob is None:
            return []
        root = blob

    out = []
    count = 0
    for node in _iter_dicts_anywhere(root):
        if count >= JSON_MAX_RECORDS:
            break

        # find a numbers-like value in this dict
        numbers = None
        # 1) check known number-ish keys first
        for k in node.keys():
            if k in NUMBER_KEYS:
                numbers = _extract_numbers_from_value(node.get(k))
                if numbers:
                    break
        # 2) try each value if not yet found
        if numbers is None:
            for v in node.values():
                numbers = _extract_numbers_from_value(v)
                if numbers:
                    break
        if not numbers:
            continue

        date = _extract_date_from_row(node)
        jackpot, winners = _extract_jackpot_winners(node)

        # decide game name
        nearby_text = ""
        # include some keys that might carry hints
        for k in ("game","game_name","name","title","product","productName"):
            if node.get(k):
                nearby_text += f" {node.get(k)}"
        game = _guess_game_from_context(meta, nearby_text)

        out.append(make_record(
            date, game, numbers, jackpot, winners,
            meta.get("final_url") or meta.get("url"),
            meta.get("fetched_at")
        ))
        count += 1

    return out

# ---------- per-source parsers (specialized) ----------

def parse_powerball(body_bytes: bytes, meta: dict) -> list[dict]:
    """
    Robust Powerball parsing:
    - Try raw JSON / dict["items"]
    - Try first JSON blob (HTML/JSON-LD)
    - Fallback: HTML stripped to text, then regex for five numbers + 'Powerball' near bonus
    - Finally: generic JSON walk if we missed it
    """
    text = body_bytes.decode("utf-8", errors="ignore")
    out = []

    # JSON / JSON-LD paths
    data = None
    try:
        data = json.loads(text)
    except Exception:
        pass
    if data is None:
        data = first_json_blob(text)

    if data is not None:
        arr = data["items"] if isinstance(data, dict) and "items" in data else data
        if isinstance(arr, list):
            for row in arr[:200]:
                date = (
                    row.get("field_draw_date")
                    or row.get("draw_date")
                    or row.get("date")
                    or row.get("field_date")
                )
                nums = (
                    row.get("field_winning_numbers")
                    or row.get("winning_numbers")
                    or row.get("numbers")
                    or ""
                )
                numbers = _extract_numbers_from_value(nums if nums else row)
                if not numbers:
                    continue
                jackpot_usd = _coerce_int(row.get("jackpot") or row.get("field_jackpot") or row.get("prize"))
                winners = row.get("winners") if isinstance(row.get("winners"), int) else None
                out.append(
                    make_record(
                        normalize_date(date if date else _sniff_date_from_text(json.dumps(row))),
                        "Powerball",
                        numbers,
                        jackpot_usd,
                        winners,
                        meta.get("final_url") or meta.get("url"),
                        meta.get("fetched_at"),
                    )
                )

    if not out:
        # HTML fallback — strip markup so numbers separated by tags become visible
        plain = html_to_text(text)
        cand = _numbers_near_keyword(plain, "Powerball") or extract_numbers_generic(plain, 5)
        if cand:
            out.append(
                make_record(
                    _sniff_date_from_text(plain),
                    "Powerball",
                    cand,
                    None,
                    None,
                    meta.get("final_url") or meta.get("url"),
                    meta.get("fetched_at"),
                )
            )

    # last chance: generic JSON mining (handles odd shapes embedded)
    if not out:
        out = parse_json_generic(body_bytes, meta)

    return out

def parse_megamillions_asmx(body_bytes: bytes, meta: dict) -> list[dict]:
    text = body_bytes.decode("utf-8", errors="ignore")
    blob = first_json_blob(text)
    if blob is None:
        # try generic walker anyway (some deployments wrap ASMX differently)
        return parse_json_generic(body_bytes, meta)
    rows = blob if isinstance(blob, list) else [blob]
    out = []
    for row in rows:
        draw_date = row.get("DrawDate") or row.get("draw_date") or row.get("Date")
        nums_str  = row.get("WinningNumbers") or row.get("winning_numbers") or row.get("Numbers") or ""
        extra = None
        for k in ("MegaBall","mega_ball","Mega","Megaball"):
            if row.get(k) is not None:
                extra = row.get(k)
                break
        numbers = _extract_numbers_from_value(nums_str) or _extract_numbers_from_value(row)
        if not numbers:
            # fallback to string scan
            numbers = extract_numbers_generic(str(nums_str))
        if extra is not None and numbers and len(numbers) == 5:
            try:
                numbers = numbers + [int(re.findall(r"\d{1,2}", str(extra))[0])]
            except Exception:
                pass
        jackpot_usd = _coerce_int(row.get("Jackpot") or row.get("jackpot"))
        out.append(
            make_record(
                normalize_date(draw_date or _sniff_date_from_text(json.dumps(row))),
                "Mega Millions", numbers or [], jackpot_usd, None,
                meta.get("final_url") or meta.get("url"), meta.get("fetched_at")
            )
        )
    return out

# --- State-site generic helpers (WA / MD / RI and others) ---

_STATE_GAME_HINTS = [
    ("Powerball",      "Powerball",    "Powerball"),
    ("Mega Millions",  "Mega Millions","Mega"),
    ("Lotto America",  "Lotto America","Star Ball"),
    ("Lucky for Life", "Lucky for Life","Lucky Ball"),
    ("Cash4Life",      "Cash4Life",    "Cash Ball"),
    ("Cash 4 Life",    "Cash4Life",    "Cash Ball"),
]

def _parse_state_generic(text_html: str, meta: dict, host_label: str) -> list[dict]:
    """
    Greedy scraper for state pages that render numbers in HTML.
    Strategy:
      - Convert whole HTML to plain text
      - For each game label, scan ALL occurrences with a large window (±STATE_SCAN_WINDOW)
      - Try keyword-based bonus detection, then generic 5+1
      - Sniff a date from the local window (fallback to page-level date)
    """
    raw_text = html_to_text(text_html)
    out = []

    page_date = _sniff_date_from_text(raw_text)

    for label, game_name, bonus_key in _STATE_GAME_HINTS:
        for m in re.finditer(re.escape(label), raw_text, flags=re.I):
            start = max(0, m.start() - STATE_SCAN_WINDOW)
            end   = min(len(raw_text), m.end() + STATE_SCAN_WINDOW)
            window = raw_text[start:end]

            cand = _numbers_near_keyword(window, bonus_key) or extract_numbers_generic(window, 5)
            if not cand and (end-start) < HARD_SCAN_WINDOW:
                # enlarge once if window was too small
                start2 = max(0, m.start() - HARD_SCAN_WINDOW)
                end2   = min(len(raw_text), m.end() + HARD_SCAN_WINDOW)
                window2 = raw_text[start2:end2]
                cand = _numbers_near_keyword(window2, bonus_key) or extract_numbers_generic(window2, 5)

            if cand:
                out.append(
                    make_record(
                        _sniff_date_from_text(window) or page_date,
                        game_name,
                        cand,
                        None,
                        None,
                        meta.get("final_url") or meta.get("url"),
                        meta.get("fetched_at"),
                    )
                )
    return out

def parse_walottery_html(body_bytes: bytes, meta: dict) -> list[dict]:
    return _parse_state_generic(body_bytes.decode("utf-8", errors="ignore"), meta, "WA Lottery")

def parse_mdlottery_html(body_bytes: bytes, meta: dict) -> list[dict]:
    # Try HTML text scrape…
    html_recs = _parse_state_generic(body_bytes.decode("utf-8", errors="ignore"), meta, "MD Lottery")
    if html_recs:
        return html_recs
    # …and also attempt JSON mining from any embedded blobs
    return parse_json_generic(body_bytes, meta)

def parse_rilot_html(body_bytes: bytes, meta: dict) -> list[dict]:
    html_recs = _parse_state_generic(body_bytes.decode("utf-8", errors="ignore"), meta, "RI Lottery")
    if html_recs:
        return html_recs
    return parse_json_generic(body_bytes, meta)

# --- Multi-state HTML pages (still supported) ---

def parse_luckyforlife_html(body_bytes: bytes, meta: dict) -> list[dict]:
    text = body_bytes.decode("utf-8", errors="ignore")
    blob = first_json_blob(text)
    if isinstance(blob, (dict, list)):
        # mine JSON first
        recs = parse_json_generic(body_bytes, meta)
        if recs:
            return recs
        # fallback to quick scan
        blob_txt = json.dumps(blob)
        cand = _numbers_near_keyword(blob_txt, "Lucky Ball") or extract_numbers_generic(blob_txt, 5)
        if cand:
            return [make_record(
                normalize_date(blob_txt), "Lucky for Life", cand, None, None,
                meta.get("final_url") or meta.get("url"), meta.get("fetched_at")
            )]
    plain = html_to_text(text)
    cand = _numbers_near_keyword(plain, "Lucky Ball") or extract_numbers_generic(plain, 5)
    if cand:
        return [make_record(
            _sniff_date_from_text(plain), "Lucky for Life", cand, None, None,
            meta.get("final_url") or meta.get("url"), meta.get("fetched_at")
        )]
    return []

def parse_lottoamerica_html(body_bytes: bytes, meta: dict) -> list[dict]:
    text = body_bytes.decode("utf-8", errors="ignore")
    blob = first_json_blob(text)
    if isinstance(blob, (dict, list)):
        recs = parse_json_generic(body_bytes, meta)
        if recs:
            return recs
        blob_txt = json.dumps(blob)
        cand = _numbers_near_keyword(blob_txt, "Star Ball") or extract_numbers_generic(blob_txt, 5)
        if cand:
            return [make_record(
                normalize_date(blob_txt), "Lotto America", cand, None, None,
                meta.get("final_url") or meta.get("url"), meta.get("fetched_at")
            )]
    plain = html_to_text(text)
    cand = _numbers_near_keyword(plain, "Star Ball") or extract_numbers_generic(plain, 5)
    if cand:
        return [make_record(
            _sniff_date_from_text(plain), "Lotto America", cand, None, None,
            meta.get("final_url") or meta.get("url"), meta.get("fetched_at")
        )]
    return []

def parse_cash4life_html(body_bytes: bytes, meta: dict) -> list[dict]:
    text = body_bytes.decode("utf-8", errors="ignore")
    blob = first_json_blob(text)
    if isinstance(blob, (dict, list)):
        recs = parse_json_generic(body_bytes, meta)
        if recs:
            return recs
        blob_txt = json.dumps(blob)
        cand = _numbers_near_keyword(blob_txt, "Cash Ball") or extract_numbers_generic(blob_txt, 5)
        if cand:
            return [make_record(
                normalize_date(blob_txt), "Cash4Life", cand, None, None,
                meta.get("final_url") or meta.get("url"), meta.get("fetched_at")
            )]
    plain = html_to_text(text)
    cand = _numbers_near_keyword(plain, "Cash Ball") or extract_numbers_generic(plain, 5)
    if cand:
        return [make_record(
            _sniff_date_from_text(plain), "Cash4Life", cand, None, None,
            meta.get("final_url") or meta.get("url"), meta.get("fetched_at")
        )]
    return []

def parse_unknown(body: bytes, meta: dict) -> list[dict]:
    """
    New behavior:
      1) If it smells like JSON or contains a JSON blob — run generic JSON harvester.
      2) Otherwise strip HTML → text and look for any 5+1 sets with common bonus keywords,
         then generic 5+1 pattern with a sniffed date.
    """
    if _looks_like_json(body):
        recs = parse_json_generic(body, meta)
        if recs:
            return recs

    text = body.decode("utf-8", errors="ignore")
    # try to mine any embedded JSON
    blob = first_json_blob(text)
    if blob is not None:
        try:
            recs = parse_json_generic(json.dumps(blob).encode("utf-8"), meta)
            if recs:
                return recs
        except Exception:
            pass

    # HTML fallback
    plain = html_to_text(text)
    for key, game in (("Powerball","Powerball"),
                      ("Mega Ball","Mega Millions"),
                      ("Lucky Ball","Lucky for Life"),
                      ("Cash Ball","Cash4Life"),
                      ("Star Ball","Lotto America")):
        cand = _numbers_near_keyword(plain, key)
        if cand:
            return [make_record(_sniff_date_from_text(plain), game, cand, None, None,
                                meta.get("final_url") or meta.get("url"), meta.get("fetched_at"))]
    cand = extract_numbers_generic(plain, 5)
    if cand:
        return [make_record(_sniff_date_from_text(plain), _guess_game_from_context(meta, plain),
                            cand, None, None, meta.get("final_url") or meta.get("url"), meta.get("fetched_at"))]
    return []

# ---------- router ----------

PARSERS = {
    # National / multi-state
    "www.powerball.com":        parse_powerball,
    "powerball.com":            parse_powerball,
    "www.megamillions.com":     parse_megamillions_asmx,
    "megamillions.com":         parse_megamillions_asmx,
    "www.luckyforlife.us":      parse_luckyforlife_html,
    "luckyforlife.us":          parse_luckyforlife_html,
    "www.lottoamerica.com":     parse_lottoamerica_html,
    "lottoamerica.com":         parse_lottoamerica_html,
    "www.cash4lifelottery.net": parse_cash4life_html,
    "cash4lifelottery.net":     parse_cash4life_html,

    # States (smoke targets you’re using)
    "walottery.com":            parse_walottery_html,
    "www.mdlottery.com":        parse_mdlottery_html,
    "www.rilot.com":            parse_rilot_html,
}

# ---------- main ----------

def main():
    run_dir = latest_run_dir(L1_OUT)
    out_dir = L2_OUT / datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)

    records = []
    by_host = {}

    for src in sorted(run_dir.glob("source_*.*")):
        if src.suffix not in (".json", ".html", ".xml", ".txt", ".bin"):
            continue

        meta = load_sidecar_meta(src) or {}
        body = src.read_bytes()

        host = ""
        try:
            host = urlparse(meta.get("final_url") or meta.get("url") or "").netloc
        except Exception:
            pass

        parser = PARSERS.get(host, parse_unknown)
        recs = parser(body, meta)

        # Safety: if specialized parser returns nothing and body is JSON/has JSON, try generic walker too.
        if not recs:
            if _looks_like_json(body) or first_json_blob(body.decode("utf-8", errors="ignore")) is not None:
                recs = parse_json_generic(body, meta)

        records.extend(recs)
        by_host[host] = by_host.get(host, 0) + len(recs)

    # De-duplicate by (game,date,numbers)
    seen, unique = set(), []
    for r in records:
        key = (r["game"], r["date"], tuple(r["numbers"]))
        if key in seen:
            continue
        seen.add(key)
        unique.append(r)

    # Sort newest first
    unique.sort(key=lambda r: (r["date"], r["game"]), reverse=True)

    now = datetime.datetime.utcnow().replace(microsecond=0)
    dataset = {
        "version": now.strftime("%Y.%m.%d"),
        "last_updated": now.isoformat() + "Z",
        "records": unique,
        "parse_stats": by_host,
    }

    json_path = out_dir / "latest-draws.json"
    csv_path  = out_dir / "latest-draws.csv"

    json_path.write_text(json.dumps(dataset, indent=2), encoding="utf-8")

    with csv_path.open("w", newline="", encoding="utf-8") as w:
        writer = csv.writer(w)
        writer.writerow(
            ["date","game","numbers","jackpot_usd","winners","source_url","fetched_at"]
        )
        for r in unique:
            writer.writerow([
                r["date"],
                r["game"],
                " ".join(map(str, r["numbers"])),
                r["jackpot_usd"] if r["jackpot_usd"] is not None else "",
                r["winners"] if r["winners"] is not None else "",
                r["source_url"] or "",
                r["fetched_at"] or "",
            ])

    print(json.dumps({
        "run_id": out_dir.name,
        "records": len(unique),
        "by_host": by_host,
        "json": json_path.name,
        "csv":  csv_path.name,
    }, indent=2))

if __name__ == "__main__":
    main()
