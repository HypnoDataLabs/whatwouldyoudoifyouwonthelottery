#!/usr/bin/env bash
set -euo pipefail

# ==============================
# Config (edit for your domain)
# ==============================
BASE_URL="${BASE_URL:-https://whatwouldyoudoifyouwonthelottery.com}"

# ==============================
# 0) Ensure output dirs exist
# ==============================
mkdir -p public/datasets public/blog

# ==============================
# 1) Ingest source bodies
#    (fetches layer1 -> layer2 *.body.json)
# ==============================
python3 layer2/ingest_bodies_only.py

# ==============================
# 2) Extract from snapshots
#    (optional; produces /tmp/nationals.json when available)
# ==============================
python3 layer2/extract_from_snaps.py || true

# ==========================================================
# 3) Merge nationals + existing with authoritative precedence
# ==========================================================
python3 - <<'PY'
import json, re
from urllib.parse import urlparse
from pathlib import Path

def load_json_safe(path, default):
    try:
        return json.loads(Path(path).read_text(encoding='utf-8'))
    except Exception:
        return default

def domain(url):
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def source_rank(rec):
    d = domain(rec.get("source_url",""))
    g = (rec.get("game") or "").lower()
    # National authoritative feeds
    if g == "powerball" and "powerball.com" in d: return 100
    if g == "mega millions" and "megamillions.com" in d: return 100
    if g == "cash4life" and "data.ny.gov" in d: return 100
    # State lotteries (secondary)
    if ("lottery" in d) or ("lotto" in d): return 60
    # Fallback by extraction method
    m = (rec.get("extraction_method") or "").lower()
    return {"json":50,"html":40,"vision":30}.get(m,0)

def jackpot_quality(rec):
    t = (rec.get("jackpot_type") or "").lower()
    if t == "annuity": return 3
    if t == "cash":    return 2
    return 1

def meth_rank(m):
    return {"json":3,"html":2,"vision":1}.get((m or "").lower(),0)

def better(a, b):
    ra, rb = source_rank(a), source_rank(b)
    if ra != rb: return ra > rb
    qa, qb = jackpot_quality(a), jackpot_quality(b)
    if qa != qb: return qa > qb
    ma, mb = meth_rank(a.get("extraction_method")), meth_rank(b.get("extraction_method"))
    if ma != mb: return ma > mb
    return (a.get("jackpot_usd") or 0) >= (b.get("jackpot_usd") or 0)

a = load_json_safe('/tmp/nationals.json', [])                # preferred fresh
b = load_json_safe('public/datasets/latest-draws.json', [])  # previously merged

num_re = re.compile(r'\d+')
def to_int(v):
    if v is None: return None
    if isinstance(v, int): return v
    s = str(v)
    digs = num_re.findall(s)
    return int(''.join(digs)) if digs else None

# normalize
for r in a + b:
    r["jackpot_usd"] = to_int(r.get("jackpot_usd"))
    if "cash_value_usd" in r:
        r["cash_value_usd"] = to_int(r.get("cash_value_usd"))
    if r.get("jackpot_type"):
        r["jackpot_type"] = str(r["jackpot_type"]).strip().lower()

# dedupe with precedence
out = {}
for r in a + b:
    k = (r.get('game'), r.get('date'), tuple(r.get('numbers') or []))
    if k not in out or better(r, out[k]):
        out[k] = r

Path('public/datasets/latest-draws.json').write_text(
    json.dumps(list(out.values()), indent=2, ensure_ascii=False),
    encoding='utf-8'
)
print("Merged with authoritative precedence -> public/datasets/latest-draws.json")
PY

# ==============================
# 4) Write CSV (with cash_value)
# ==============================
python3 - <<'PY'
import json, csv, pathlib
rows = json.loads(pathlib.Path('public/datasets/latest-draws.json').read_text(encoding='utf-8'))
with open('public/datasets/latest-draws.csv','w',newline='',encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(['date','game','numbers','jackpot_usd','jackpot_type','cash_value_usd','source_url','extraction_method'])
    for r in rows:
        w.writerow([
            r.get('date'),
            r.get('game'),
            ' '.join(map(str,r.get('numbers',[]))),
            r.get('jackpot_usd'),
            r.get('jackpot_type') or '',
            r.get('cash_value_usd') or '',
            r.get('source_url'),
            r.get('extraction_method')
        ])
print("Wrote public/datasets/latest-draws.csv")
PY

# ==========================================================
# 5) Rebuild blog page (includes inline JSON-LD via the py script)
# ==========================================================
python3 scripts/build_lottery_blog.py \
  --in public/datasets/latest-draws.json \
  --out public/blog/lottery-draws.html

# ==========================================================
# 5b) Emit a standalone Dataset JSON-LD for the dataset itself
# ==========================================================
python3 - <<'PY'
import json, datetime, pathlib, os

base = os.environ.get("BASE_URL","https://whatwouldyoudoifyouwonthelottery.com")
data_path = pathlib.Path("public/datasets/latest-draws.json")
rows = json.loads(data_path.read_text(encoding="utf-8"))
lastmod = datetime.datetime.utcfromtimestamp(data_path.stat().st_mtime).isoformat()+"Z"

ld = {
  "@context": "https://schema.org",
  "@type": "Dataset",
  "name": "Latest US Lottery Draw Results",
  "description": "Daily compiled lottery draw numbers (Powerball, Mega Millions, Cash4Life) with official source attribution.",
  "keywords": ["lottery","winning numbers","Powerball","Mega Millions","Cash4Life","jackpot","results","dataset"],
  "dateModified": lastmod,
  "publisher": {"@type":"Organization","name":"HypnoData","url": base},
  "distribution": [
    {"@type":"DataDownload","encodingFormat":"application/json","contentUrl": f"{base}/datasets/latest-draws.json"},
    {"@type":"DataDownload","encodingFormat":"text/csv","contentUrl": f"{base}/datasets/latest-draws.csv"}
  ],
  "variableMeasured": [
    {"@type":"PropertyValue","name":"numbers"},
    {"@type":"PropertyValue","name":"jackpot_usd","description":"primary jackpot (usually annuity)"},
    {"@type":"PropertyValue","name":"jackpot_type","description":"annuity or cash"},
    {"@type":"PropertyValue","name":"cash_value_usd","description":"cash option value when known"}
  ]
}

path = pathlib.Path("public/datasets/latest-draws.dataset.jsonld")
path.write_text(json.dumps(ld, ensure_ascii=False, indent=2), encoding="utf-8")
print("Wrote", path)
PY

# ==============================
# 6) Rebuild sitemap.xml
# ==============================
python3 - <<'PY'
import os, datetime, pathlib

base = os.environ.get("BASE_URL","https://whatwouldyoudoifyouwonthelottery.com")

pages = [
    "index.html",
    "planets.html",
    "license.html",
    "blog/lottery-draws.html",
    "datasets/latest-draws.json",
    "datasets/latest-draws.csv",
    "datasets/latest-draws.dataset.jsonld",
]

urls = []
for p in pages:
    path = pathlib.Path("public")/p
    if path.exists():
        mtime = datetime.datetime.utcfromtimestamp(path.stat().st_mtime).isoformat()+"Z"
        urls.append((p, mtime))

smp = pathlib.Path("public/sitemap.xml")
with smp.open("w", encoding="utf-8") as f:
    f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
    for p, mtime in urls:
        f.write("  <url>\n")
        f.write(f"    <loc>{base}/{p}</loc>\n")
        f.write(f"    <lastmod>{mtime}</lastmod>\n")
        f.write("    <changefreq>daily</changefreq>\n")
        f.write("    <priority>0.8</priority>\n")
        f.write("  </url>\n")
    f.write('</urlset>\n')
print("Wrote public/sitemap.xml")
PY

# ==============================
# 6b) Ensure robots.txt points to sitemap
# ==============================
python3 - <<'PY'
from pathlib import Path
import os

base = os.environ.get("BASE_URL","https://whatwouldyoudoifyouwonthelottery.com")
robots = Path("public/robots.txt")
lines = [
    "User-agent: *",
    "Allow: /",
    f"Sitemap: {base}/sitemap.xml",
    ""
]
robots.write_text("\n".join(lines), encoding="utf-8")
print("Wrote public/robots.txt")
PY

# ==============================
# 7) Ping search engines (where anonymous ping is supported)
# ==============================
echo "Pinging search engines with updated sitemap…"
# Google
curl -fsS "https://www.google.com/ping?sitemap=${BASE_URL}/sitemap.xml" || true
# Bing (also used by Yahoo + some partners)
curl -fsS "https://www.bing.com/ping?sitemap=${BASE_URL}/sitemap.xml" || true
# Note: Baidu, Yandex, Naver, etc. require authenticated webmaster APIs or site verification.
# Keeping it lean for MVP to avoid failing calls.

echo "All done."
echo "Preview locally: python3 -m http.server --directory public 8093  # then open http://127.0.0.1:8093/blog/lottery-draws.html"
