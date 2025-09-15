import os, json, pathlib, datetime, shutil, hashlib

BASE = pathlib.Path(".")
PUBLIC = BASE / "public"
ASSETS = PUBLIC / "assets"
FEEDS = PUBLIC / "feeds"
DATASETS_DIR = PUBLIC / "datasets"
BLOG_DIR = PUBLIC / "blog"
DATA_DIR = PUBLIC / "data"
DEV_DIR = PUBLIC / "developers"

SITE_TITLE = "What Would You Do If You Won The Lottery"
CANONICAL_BASE = "https://whatwouldyoudoifyouwonthelottery.com"
DATASET_BASENAME = "latest-draws"  # stable filename for verify.sh

def latest_run(dirpath: pathlib.Path) -> pathlib.Path:
    runs = [p for p in dirpath.iterdir() if p.is_dir()]
    if not runs:
        raise SystemExit("No layer2 runs found.")
    return sorted(runs)[-1]

def sha256(p: pathlib.Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def write(path: pathlib.Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")

# --- Load latest dataset artifacts from layer2 ---
run_dir = latest_run(BASE / "layer2" / "out")
dataset_json_src = run_dir / f"{DATASET_BASENAME}.json"
dataset_csv_src  = run_dir / f"{DATASET_BASENAME}.csv"

with open(dataset_json_src, "r", encoding="utf-8") as f:
    dataset_obj = json.load(f)

now = datetime.datetime.utcnow()
date_slug = now.strftime("%Y-%m-%d")
article_slug = f"{date_slug}-lottery-draws.html"

# --- Ensure dirs ---
for d in [PUBLIC, ASSETS, FEEDS, DATASETS_DIR, BLOG_DIR, DATA_DIR, DEV_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# --- Copy datasets into /public/datasets ---
dataset_json_dst = DATASETS_DIR / f"{DATASET_BASENAME}.json"
dataset_csv_dst  = DATASETS_DIR / f"{DATASET_BASENAME}.csv"
shutil.copyfile(dataset_json_src, dataset_json_dst)
shutil.copyfile(dataset_csv_src,  dataset_csv_dst)

sha_json = sha256(dataset_json_dst)
sha_csv  = sha256(dataset_csv_dst)

# --- Minimal blog index loader to build listing ---
def load_feed(path: pathlib.Path):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []

# --- Article HTML (includes explicit links to dataset JSON/CSV) ---
article_ld = {
  "@context": "https://schema.org",
  "@type": "Article",
  "headline": "Latest Lottery Draws",
  "datePublished": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
  "author": {"@type": "Organization", "name": SITE_TITLE},
  "mainEntityOfPage": f"{CANONICAL_BASE}/blog/{article_slug}",
  "isPartOf": {"@type": "Blog", "name": SITE_TITLE},
}

dataset_ld = {
  "@context": "https://schema.org",
  "@type": "Dataset",
  "name": "Latest Lottery Draws",
  "identifier": DATASET_BASENAME,
  "url": f"{CANONICAL_BASE}/data/lottery-draws.html",
  "distribution": [
    {"@type": "DataDownload", "encodingFormat": "application/json", "contentUrl": f"{CANONICAL_BASE}/datasets/{DATASET_BASENAME}.json"},
    {"@type": "DataDownload", "encodingFormat": "text/csv", "contentUrl": f"{CANONICAL_BASE}/datasets/{DATASET_BASENAME}.csv"},
  ],
  "isAccessibleForFree": True,
  "license": f"{CANONICAL_BASE}/license.html"
}

records = dataset_obj.get("records", [])
version = dataset_obj.get("version")
last_updated = dataset_obj.get("last_updated")

article_html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Latest Lottery Draws - {SITE_TITLE}</title>
  <meta name="description" content="Auto-published lottery draw results with downloadable datasets." />
  <link rel="stylesheet" href="/assets/styles.css" />
  <link rel="canonical" href="{CANONICAL_BASE}/blog/{article_slug}" />
  <meta property="og:title" content="Latest Lottery Draws" />
  <meta property="og:description" content="Numbers, jackpots, and datasets." />
  <meta property="og:type" content="article" />
  <script type="application/ld+json">{json.dumps(article_ld)}</script>
</head>
<body>
  <header><a href="/">Home</a> · <a href="/data/">Data</a> · <a href="/developers/">Developers</a></header>
  <main class="container">
    <h1>Latest Lottery Draws</h1>
    <p class="meta">Published {now.strftime("%Y-%m-%d %H:%M UTC")}</p>

    <section class="facts">
      <h2>Key Facts</h2>
      <ul>
        <li id="fact-records">Records: <strong>{len(records)}</strong> — <a href="/datasets/{DATASET_BASENAME}.json">JSON</a></li>
        <li id="fact-version">Version: <code>{version}</code>; Updated: <code>{last_updated}</code></li>
      </ul>
    </section>

    <section class="card">
      <h3>Downloads</h3>
      <ul>
        <li><a href="/datasets/{DATASET_BASENAME}.json">Dataset (JSON)</a></li>
        <li><a href="/datasets/{DATASET_BASENAME}.csv">Dataset (CSV)</a></li>
      </ul>
      <p class="meta">Integrity — JSON sha256: <code>{sha_json}</code> · CSV sha256: <code>{sha_csv}</code></p>
    </section>

    <section>
      <h3>Methodology</h3>
      <p>We parse official lottery sources on a schedule and normalize results into a stable schema (date, game, numbers, jackpot_usd, winners, source_url, fetched_at, version, last_updated).</p>
    </section>

    <section>
      <h3>Developers</h3>
      <pre><code># curl
curl -s {CANONICAL_BASE}/datasets/{DATASET_BASENAME}.json | jq .records[0]

# python
import pandas as pd
df = pd.read_csv("{CANONICAL_BASE}/datasets/{DATASET_BASENAME}.csv")
print(df.head())</code></pre>
    </section>
  </main>
  <footer><a href="/planets.html">Planets</a> · <a href="/license.html">License</a></footer>
</body>
</html>"""

# --- Dataset landing page (/data/lottery-draws.html) ---
dataset_html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Dataset: Latest Lottery Draws - {SITE_TITLE}</title>
  <meta name="description" content="Lottery results dataset (JSON & CSV) for agents and developers." />
  <link rel="stylesheet" href="/assets/styles.css" />
  <link rel="canonical" href="{CANONICAL_BASE}/data/lottery-draws.html" />
  <script type="application/ld+json">{json.dumps(dataset_ld)}</script>
</head>
<body>
  <header><a href="/">Home</a> · <a href="/blog/">Blog</a> · <a href="/developers/">Developers</a></header>
  <main class="container">
    <h1>Dataset: Latest Lottery Draws</h1>
    <p>Download: <a href="/datasets/{DATASET_BASENAME}.json">JSON</a> (sha256: {sha_json}) · <a href="/datasets/{DATASET_BASENAME}.csv">CSV</a> (sha256: {sha_csv})</p>
    <h3>Schema</h3>
    <pre><code>date, game, numbers[], jackpot_usd, winners, source_url, fetched_at, version, last_updated</code></pre>
    <h3>Examples</h3>
    <pre><code># curl
curl -s {CANONICAL_BASE}/datasets/{DATASET_BASENAME}.json | jq .records[0]

# node
const res = await fetch("{CANONICAL_BASE}/datasets/{DATASET_BASENAME}.json");
const data = await res.json();

# python
import pandas as pd
df = pd.read_csv("{CANONICAL_BASE}/datasets/{DATASET_BASENAME}.csv")
print(df.head())</code></pre>
  </main>
  <footer><a href="/planets.html">Planets</a> · <a href="/license.html">License</a></footer>
</body>
</html>"""

# --- Home page (simple latest link) ---
index_html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" /><meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{SITE_TITLE}</title>
  <meta name="description" content="Live lottery draws as articles and datasets." />
  <link rel="stylesheet" href="/assets/styles.css" />
</head>
<body>
  <header class="hero">
    <img src="/assets/logo.svg" alt="logo" class="logo" />
    <h1>{SITE_TITLE}</h1>
    <p>What would you do if you won? We track draws and publish datasets you can use.</p>
    <nav><a href="/blog/">Blog</a> · <a href="/data/">Data</a> · <a href="/developers/">Developers</a></nav>
  </header>
  <main class="container">
    <h2>Latest</h2>
    <ul><li><a href="/blog/{article_slug}">Latest Lottery Draws</a></li></ul>
  </main>
  <footer><a href="/planets.html">Planets</a> · <a href="/license.html">License</a></footer>
</body></html>"""

# --- Blog index (rebuild from feed after we append) ---
def render_blog_index(feed_entries):
    items = "\n".join(
        f'<li><a href="{e.get("url","/")}">{e.get("title","Post")}</a> '
        f'<span class="meta">{e.get("published","")}</span></li>'
        for e in feed_entries[:100]
    )
    return f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8" /><meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Blog - {SITE_TITLE}</title><link rel="stylesheet" href="/assets/styles.css" /></head>
<body>
<header><a href="/">Home</a> · <a href="/data/">Data</a> · <a href="/developers/">Developers</a></header>
<main class="container">
  <h1>Blog</h1>
  <ul>
    {items}
  </ul>
</main>
<footer><a href="/planets.html">Planets</a> · <a href="/license.html">License</a></footer>
</body></html>"""

# --- Data catalog index (reads datasets feed) ---
def render_data_index(feed_entries):
    items = "\n".join(
        f'<li><a href="{e.get("url","/")}">{e.get("title","Dataset")}</a> '
        f'<span class="meta">{e.get("updated","")}</span></li>'
        for e in feed_entries[:100]
    )
    return f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8" /><meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Data Catalog - {SITE_TITLE}</title><link rel="stylesheet" href="/assets/styles.css" /></head>
<body>
<header><a href="/">Home</a> · <a href="/blog/">Blog</a> · <a href="/developers/">Developers</a></header>
<main class="container">
  <h1>Data Catalog</h1>
  <ul>
    {items}
  </ul>
</main>
<footer><a href="/planets.html">Planets</a> · <a href="/license.html">License</a></footer>
</body></html>"""

# --- Developers page (simple, always present) ---
developers_html = f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8" /><meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Developers - {SITE_TITLE}</title><link rel="stylesheet" href="/assets/styles.css" /></head>
<body>
<header><a href="/">Home</a> · <a href="/blog/">Blog</a> · <a href="/data/">Data</a></header>
<main class="container">
  <h1>Developers</h1>
  <p>Public datasets are available without auth. Please cache for 24h for heavy usage.</p>
  <h3>Endpoints</h3>
  <ul>
    <li><code>/datasets/{DATASET_BASENAME}.json</code></li>
    <li><code>/datasets/{DATASET_BASENAME}.csv</code></li>
  </ul>
  <h3>Examples</h3>
  <pre><code># curl
curl -s {CANONICAL_BASE}/datasets/{DATASET_BASENAME}.json | jq .records[0]

# node
const res = await fetch("{CANONICAL_BASE}/datasets/{DATASET_BASENAME}.json");
const data = await res.json();

# python
import pandas as pd
df = pd.read_csv("{CANONICAL_BASE}/datasets/{DATASET_BASENAME}.csv")
print(df.head())</code></pre>
</main>
<footer><a href="/planets.html">Planets</a> · <a href="/license.html">License</a></footer>
</body></html>"""

# --- Write pages ---
write(BLOG_DIR / article_slug, article_html)
write(PUBLIC / "data" / "lottery-draws.html", dataset_html)
write(PUBLIC / "index.html", index_html)
write(DEV_DIR / "index.html", developers_html)

# --- Update feeds (blog + datasets) ---
def append_feed(path: pathlib.Path, entry: dict):
    arr = load_feed(path)
    arr.insert(0, entry)
    write(path, json.dumps(arr, indent=2))

append_feed(FEEDS / "blog.json", {
    "title": "Latest Lottery Draws",
    "url": f"/blog/{article_slug}",
    "published": now.isoformat() + "Z"
})
append_feed(FEEDS / "datasets.json", {
    "title": "Latest Lottery Draws Dataset",
    "url": "/data/lottery-draws.html",
    "updated": dataset_obj.get("last_updated")
})
write(FEEDS / "last-updated.json", json.dumps({"ts": now.isoformat() + "Z"}, indent=2))

# --- Rebuild blog and data indexes from feeds ---
blog_idx = render_blog_index(load_feed(FEEDS / "blog.json"))
data_idx = render_data_index(load_feed(FEEDS / "datasets.json"))
write(PUBLIC / "blog" / "index.html", blog_idx)
write(PUBLIC / "data" / "index.html", data_idx)

# --- Sitemap & robots ---
sitemap = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>{CANONICAL_BASE}/</loc><lastmod>{now.date()}</lastmod></url>
  <url><loc>{CANONICAL_BASE}/blog/{article_slug}</loc><lastmod>{now.date()}</lastmod></url>
  <url><loc>{CANONICAL_BASE}/blog/</loc><lastmod>{now.date()}</lastmod></url>
  <url><loc>{CANONICAL_BASE}/data/lottery-draws.html</loc><lastmod>{now.date()}</lastmod></url>
  <url><loc>{CANONICAL_BASE}/data/</loc><lastmod>{now.date()}</lastmod></url>
  <url><loc>{CANONICAL_BASE}/datasets/{DATASET_BASENAME}.json</loc><lastmod>{now.date()}</lastmod></url>
  <url><loc>{CANONICAL_BASE}/datasets/{DATASET_BASENAME}.csv</loc><lastmod>{now.date()}</lastmod></url>
</urlset>"""
write(PUBLIC / "sitemap.xml", sitemap)

write(PUBLIC / "robots.txt", "User-agent: *\nAllow: /\n")

# --- Static headers hint (Cloudflare/Netlify compatible) ---
# Adjust to your host. This sets CORS for datasets and caching hints.
headers_txt = """
/datasets/*
  Access-Control-Allow-Origin: *
  Cache-Control: public, max-age=86400

/blog/*
  Cache-Control: public, max-age=600

/data/*
  Cache-Control: public, max-age=600

/feeds/*
  Access-Control-Allow-Origin: *
  Cache-Control: public, max-age=300

/sitemap.xml
  Cache-Control: public, max-age=300
"""
write(PUBLIC / "_headers", headers_txt.strip() + "\n")

print(json.dumps({
  "published": True,
  "article": f"/blog/{article_slug}",
  "dataset_json": f"/datasets/{DATASET_BASENAME}.json"
}, indent=2))
