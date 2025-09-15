#!/usr/bin/env python3
import json, re, html, datetime, pathlib, argparse, os
from collections import defaultdict

# ---------- Helpers ----------
def coerce_jackpot(v):
    if v is None: return None
    m = re.findall(r'\d+', str(v))
    return int(''.join(m)) if m else None

def coerce_int(v):
    if v is None: return None
    m = re.findall(r'\d+', str(v))
    return int(''.join(m)) if m else None

def safe_date(s):
    if not s: return None
    try:
        return datetime.date.fromisoformat(s)
    except Exception:
        return None

def fmt_money(n):
    return f"{n:,}" if isinstance(n, int) else "—"

def domain_of(url):
    if not url: return ""
    m = re.match(r'https?://([^/]+)/?', url)
    return m.group(1) if m else ""

def is_national(game: str, domain: str) -> bool:
    g = (game or "").lower()
    d = (domain or "").lower()
    if g == "powerball" and "powerball.com" in d: return True
    if g == "mega millions" and "megamillions.com" in d: return True
    if g == "cash4life" and "data.ny.gov" in d: return True
    return False

def jackpot_display(rec):
    """
    Display with honesty:
      - Primary: jackpot_usd (assume annuity if jackpot_type says so)
      - Append cash_value_usd if present
      - If jackpot_type is missing for Powerball/Mega and source isn’t national, add '(from state site; may be cash value)'
    """
    main = fmt_money(rec.get('jackpot_usd'))
    jt = rec.get('jackpot_type')
    cash = rec.get('cash_value_usd')
    cash = coerce_int(cash) if isinstance(cash, str) else cash

    pieces = []
    if jt: pieces.append(html.escape(jt))
    if cash:
        pieces.append(f"cash ${fmt_money(cash)}")

    # Honest caveat when type is unknown and source is a state site (Powerball/Mega)
    dom = domain_of(rec.get('source_url') or "")
    game = rec.get('game') or ""
    if not jt and (game.lower() in ("powerball","mega millions")) and not is_national(game, dom):
        pieces.append("from state site; may be cash value")

    suffix = f" ({'; '.join(pieces)})" if pieces else ""
    return f"{main}{suffix}"

# ---------- Main ----------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", required=True)
    p.add_argument("--out", dest="outp", required=True)
    p.add_argument("--site", dest="site", default=os.environ.get("SITE_BASE",""))  # e.g. https://hypnodata.com
    p.add_argument("--editorial", dest="editorial", default="")  # optional path to an HTML/MD snippet
    args = p.parse_args()

    IN  = pathlib.Path(args.inp)
    OUT = pathlib.Path(args.outp)
    site = (args.site or "").rstrip("/")

    rows = json.load(open(IN))
    # normalize
    for r in rows:
        r['jackpot_usd'] = coerce_jackpot(r.get('jackpot_usd'))
        if 'cash_value_usd' in r:
            r['cash_value_usd'] = coerce_jackpot(r.get('cash_value_usd'))
        r['_dt'] = safe_date(r.get('date'))

    # sort newest first
    rows.sort(key=lambda r: (r['_dt'] or datetime.date.min, r.get('game','')), reverse=True)
    total = len(rows)

    # group by game
    games = defaultdict(list)
    for r in rows:
        games[r.get('game','Unknown')].append(r)
    games_list = sorted(games.keys())

    # latest per game + top jackpots (for auto-summary)
    latest_per_game = {}
    for g, recs in games.items():
        recs_sorted = sorted(recs, key=lambda r: (r['_dt'] or datetime.date.min, r.get('jackpot_usd') or -1), reverse=True)
        latest_per_game[g] = recs_sorted[0]

    top_jackpots = sorted(
        (r for r in rows if r.get('jackpot_usd')),
        key=lambda r: r['jackpot_usd'],
        reverse=True
    )[:5]

    # source domains (attribution)
    src_domains = sorted(set(domain_of(r.get("source_url")) for r in rows if r.get("source_url")))
    json_url = (site + "/datasets/latest-draws.json") if site else "/datasets/latest-draws.json"
    csv_url  = (site + "/datasets/latest-draws.csv")  if site else "/datasets/latest-draws.csv"

    # ---------- JSON-LD Dataset (SEO) ----------
    parts = []
    for g in games_list:
        parts.append({
            "@type": "Dataset",
            "name": f"Lottery results — {g}",
            "description": f"Latest {g} draw results compiled from official sources.",
            "keywords": [g, "lottery", "winning numbers", "jackpot", "results"],
        })

    dataset_ld = {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": "Latest US Lottery Draw Results",
        "description": "Daily compiled lottery draw numbers (Powerball, Mega Millions, Cash4Life) with source attribution to official lottery sites.",
        "keywords": ["lottery", "winning numbers", "Powerball", "Mega Millions", "Cash4Life", "jackpot", "results"],
        "dateModified": datetime.datetime.utcnow().isoformat(timespec='seconds') + "Z",
        "publisher": {
            "@type": "Organization",
            "name": "HypnoData",
            "url": site or ""
        },
        "isBasedOn": sorted(set(r.get("source_url") for r in rows if r.get("source_url"))),
        "distribution": [
            {"@type": "DataDownload", "encodingFormat": "application/json", "contentUrl": json_url},
            {"@type": "DataDownload", "encodingFormat": "text/csv",        "contentUrl": csv_url}
        ],
        "variableMeasured": [
            {"@type":"PropertyValue","name":"numbers"},
            {"@type":"PropertyValue","name":"jackpot_usd","description":"primary jackpot (usually annuity)"},
            {"@type":"PropertyValue","name":"jackpot_type","description":"annuity or cash value when known"},
            {"@type":"PropertyValue","name":"cash_value_usd","description":"cash option value when available"}
        ],
        "hasPart": parts
    }

    # ---------- Auto Summary (SEO text) ----------
    def latest_line(game, rec):
        nums = " ".join(str(n) for n in rec.get("numbers", []))
        d = rec.get("date") or "unknown date"
        disp = jackpot_display(rec)
        if disp.startswith("—"):
            return f"{game}: {d} — numbers {nums}."
        return f"{game}: {d} — numbers {nums}, jackpot ${disp}."

    latest_bits = [latest_line(g, latest_per_game[g]) for g in sorted(latest_per_game.keys())]
    jackpots_bits = [f"{r.get('game','?')} ${jackpot_display(r)}" for r in top_jackpots]

    auto_summary = []
    auto_summary.append("<p><strong>Daily Lottery Snapshot.</strong> This page compiles official draw results for major US games and refreshes automatically.</p>")
    if latest_bits:
        auto_summary.append("<p><strong>Latest draws</strong>: " + " ".join(latest_bits) + "</p>")
    if jackpots_bits:
        auto_summary.append("<p><strong>Notable jackpots</strong>: " + ", ".join(jackpots_bits) + ".</p>")
    if src_domains:
        links = " ".join(f"<a href='https://{html.escape(d)}' rel='nofollow'>{html.escape(d)}</a>" for d in src_domains)
        auto_summary.append(f"<p class='muted'>Sources include: {links}</p>")
    auto_summary_html = "\n".join(auto_summary)

    # optional editorial chunk (your manual note)
    editorial_html = ""
    if args.editorial and pathlib.Path(args.editorial).exists():
        editorial_html = pathlib.Path(args.editorial).read_text(encoding="utf-8")
    else:
        editorial_html = (
            "<div id='editorial' class='muted' style='margin:12px 0'>"
            "<em>Editorial note:</em> add your daily commentary here."
            "</div>"
        )

    # ---------- HTML rendering ----------
    def tr(r, show_game=True):
        nums = ' '.join(str(n) for n in r.get('numbers', []))
        src  = r.get('source_url') or '#'
        jack_disp = jackpot_display(r)
        meth = r.get('extraction_method') or ''
        dt   = r.get('date') or ''
        game_cell = (f"<td>{html.escape(r.get('game',''))}</td>") if show_game else ""
        return (
            f"<tr><td>{html.escape(dt)}</td>"
            f"{game_cell}"
            f"<td>{nums}</td><td>{jack_disp}</td>"
            f"<td><a href='{html.escape(src)}' rel='nofollow'>src</a></td>"
            f"<td class='muted'>{html.escape(meth)}</td></tr>"
        )

    html_out = []
    html_out.append("<!doctype html><meta charset='utf-8'>")
    html_out.append("<title>Latest Lottery Draws</title>")
    html_out.append("""
<style>
:root { color-scheme: dark; }
body{font-family:system-ui,Arial,sans-serif;line-height:1.45;padding:24px;background:#0b0b0c;color:#e7e7ea}
table{border-collapse:collapse;width:100%;max-width:1000px;margin:10px 0 24px}
th,td{border:1px solid #2a2a2d;padding:8px 10px;text-align:left}
th{background:#151518}
a{color:#8ab4ff}
.pill{display:inline-block;margin-right:8px;padding:2px 8px;border-radius:999px;background:#151a2b}
.muted{color:#a0a0a8}
.hdr{display:flex;gap:12px;align-items:baseline;flex-wrap:wrap}
.right{margin-left:auto}
.latest{padding:12px 14px;border:1px solid #2a2a2d;border-radius:12px;background:#101014;margin:12px 0}
.tabs{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}
.tablink{padding:6px 10px;border:1px solid #2a2a2d;border-radius:999px;background:#121218;cursor:pointer}
.tablink.active{background:#1c1c22}
.section{display:none}
.section.active{display:block}
.kv{display:flex;gap:12px;flex-wrap:wrap;margin:6px 0}
.kv div{padding:4px 8px;border-radius:8px;background:#121218;border:1px solid #2a2a2d}
code{background:#151518;border:1px solid #2a2a2d;padding:2px 6px;border-radius:6px}
</style>
""")

    # JSON-LD script
    html_out.append("<script type='application/ld+json'>")
    html_out.append(html.escape(json.dumps(dataset_ld, ensure_ascii=False)))
    html_out.append("</script>")

    # Header
    html_out.append("<div class='hdr'><h1>Latest Lottery Draws</h1>")
    html_out.append(f"<span class='muted'>Records: {total}</span>")
    html_out.append(f"<a class='pill' href='{html.escape(json_url)}'>JSON</a>")
    html_out.append(f"<a class='pill' href='{html.escape(csv_url)}'>CSV</a>")
    html_out.append(f"<span class='right muted'>Last updated: {dataset_ld['dateModified']}</span></div>")

    # Latest-draw highlight (most recent overall) — with “honest” caveat
    if rows:
        latest = rows[0]
        nums = " ".join(str(n) for n in latest.get("numbers", []))
        jack_disp = jackpot_display(latest)
        html_out.append("<div class='latest'>")
        html_out.append(f"<div class='muted'>Automatic daily feed</div>")
        html_out.append(f"<h2 style='margin:6px 0'>Latest: {html.escape(latest.get('game',''))} — {html.escape(latest.get('date') or '')}</h2>")
        html_out.append(f"<div style='font-size:1.25rem'>Numbers: <strong>{nums}</strong></div>")
        html_out.append(f"<div>Jackpot: <strong>${jack_disp}</strong></div>")
        if latest.get("source_url"):
            html_out.append(f"<div>Source: <a href='{html.escape(latest['source_url'])}' rel='nofollow'>link</a></div>")
        html_out.append("</div>")

    # Auto summary + editorial area
    html_out.append("<h2>Summary</h2>")
    html_out.append(auto_summary_html)
    html_out.append(editorial_html)

    # Tabs
    html_out.append("<div class='tabs'>")
    html_out.append("<button class='tablink active' data-tab='all'>All Games</button>")
    for g in games_list:
        gid = re.sub(r'[^a-z0-9]+','-', g.lower()).strip('-') or 'game'
        html_out.append(f"<button class='tablink' data-tab='{gid}'>{html.escape(g)}</button>")
    html_out.append("</div>")

    # All games section
    html_out.append("<div class='section active' id='tab-all'>")
    html_out.append("<table><thead><tr><th>Date</th><th>Game</th><th>Numbers</th><th>Jackpot (USD, source-reported)</th><th>Source</th><th>Method</th></tr></thead><tbody>")
    html_out += [tr(r, show_game=True) for r in rows]
    html_out.append("</tbody></table></div>")

    # Per-game sections
    for g in games_list:
        gid = re.sub(r'[^a-z0-9]+','-', g.lower()).strip('-') or 'game'
        html_out.append(f"<div class='section' id='tab-{gid}'>")
        html_out.append(f"<h3 style='margin-top:4px'>{html.escape(g)}</h3>")
        gl = latest_per_game[g]
        gl_nums = " ".join(str(n) for n in gl.get("numbers", []))
        gl_jack_disp = jackpot_display(gl)
        html_out.append("<div class='kv'>")
        html_out.append(f"<div><strong>Latest:</strong> {html.escape(gl.get('date') or '')}</div>")
        html_out.append(f"<div><strong>Numbers:</strong> {gl_nums}</div>")
        html_out.append(f"<div><strong>Jackpot:</strong> ${gl_jack_disp}</div>")
        html_out.append("</div>")

        html_out.append("<table><thead><tr><th>Date</th><th>Numbers</th><th>Jackpot (USD, source-reported)</th><th>Source</th><th>Method</th></tr></thead><tbody>")
        for r in games[g]:
            html_out.append(tr(r, show_game=False))
        html_out.append("</tbody></table></div>")

    # tiny JS for tabs
    html_out.append("""
<script>
document.querySelectorAll('.tablink').forEach(btn=>{
  btn.addEventListener('click', ()=>{
    document.querySelectorAll('.tablink').forEach(b=>b.classList.remove('active'));
    document.querySelectorAll('.section').forEach(s=>s.classList.remove('active'));
    btn.classList.add('active');
    const id = 'tab-' + btn.getAttribute('data-tab');
    const sec = document.getElementById(id);
    if (sec) sec.classList.add('active');
  });
});
</script>
""")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text('\n'.join(html_out), encoding='utf-8')
    print(f"Wrote {OUT} with {total} rows across {len(games_list)} games.")

if __name__ == "__main__":
    main()
