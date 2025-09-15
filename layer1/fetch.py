#!/usr/bin/env python3
"""
Layer 1 — Fetch (robust + verbose)
- Polite pacing (global + per-domain from layer1/pacing.yaml)
- Random jitter, per-host delay; retries with exponential backoff
- Rotating User-Agents + realistic headers; gzip decoding
- Smart Accept header: JSON-first for API-ish URLs (+ X-Requested-With for .asmx)
- Content-type aware file extensions (+ simple sniff fallback)
- Sidecar .meta.json per file (headers, checksum, timing, UA, request headers)
- Rich l1_summary.json (counts, hosts, defaults)
- FAST / LIMIT env toggles for quick local runs
- TARGETS_FILE env selects which list to fetch (defaults to layer1/targets.txt)
- Optional depth-1 same-site expansion via layer1/expand.yaml
- Unbuffered logs when PYTHONUNBUFFERED=1 (set in run.sh)
- Graceful Ctrl+C (partial summary still written)
- Robust target parsing (strips comments/notes)
"""

import os, sys, json, time, random, pathlib, datetime, gzip, io, hashlib, signal, re
import urllib.request, urllib.error
from urllib.parse import urlparse, urljoin

# =========================
# Logging / env toggles
# =========================
VERBOSE = True
ENV_FAST   = os.getenv("FAST") == "1"                 # FAST=1 ./scripts/run.sh
ENV_LIMIT  = int(os.getenv("LIMIT", "0") or "0")      # LIMIT=10 ./scripts/run.sh
TARGETS_FILE = os.getenv("TARGETS") or os.getenv("TARGETS_FILE") or "layer1/targets.txt"

def log(msg: str):
    if VERBOSE:
        print(msg, flush=True)

# =========================
# Defaults (overridden by pacing.yaml)
# =========================
DEFAULTS = {
    "per_host_delay": 1.0,   # seconds between requests to the same host
    "jitter_min": 0.8,       # random post-request sleep min
    "jitter_max": 2.4,       # random post-request sleep max
    "timeout_sec": 30,       # request timeout
    "max_retries": 3,        # retries on 429/5xx/403/URLError
    "backoff_base": 0.7,     # seconds; grows exponentially
    "backoff_cap": 5.0,      # per attempt max additional seconds
    "respect_robots": False, # set True in pacing.yaml if you want robots.txt adherence
    # Optional: per-domain can also define "headers": { "Header-Name": "Value" }
}

# Rotate a few realistic desktop UAs
UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
]

CT_EXT = {
    "application/json": ".json",
    "text/json": ".json",
    "text/html": ".html",
    "application/xml": ".xml",
    "text/xml": ".xml",
    "text/plain": ".txt",
}

# =========================
# Paths
# =========================
RUN_ID = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
OUT_DIR = pathlib.Path("layer1/out") / RUN_ID
LOGS   = pathlib.Path("layer1/logs")
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS.mkdir(parents=True, exist_ok=True)

# Accumulators (even on Ctrl+C)
RESULTS    = []
HOST_STATS = {}
_robots_cache = {}

# =========================
# pacing.yaml loader (optional)
# =========================
def load_pacing():
    cfg = {"defaults": DEFAULTS.copy(), "domains": {}}
    ypath = pathlib.Path("layer1/pacing.yaml")
    if not ypath.exists():
        return cfg
    try:
        import yaml  # type: ignore
    except Exception:
        return cfg
    try:
        data = yaml.safe_load(ypath.read_text(encoding="utf-8")) or {}
        if isinstance(data.get("defaults"), dict):
            for k, v in data["defaults"].items():
                if k in cfg["defaults"]:
                    cfg["defaults"][k] = v
        if isinstance(data.get("domains"), dict):
            cfg["domains"] = data["domains"]
    except Exception:
        pass
    return cfg

PACING = load_pacing()

# =========================
# expand.yaml loader (optional)
# =========================
def load_expand():
    ypath = pathlib.Path("layer1/expand.yaml")
    if not ypath.exists():
        return {}
    try:
        import yaml  # type: ignore
        data = yaml.safe_load(ypath.read_text(encoding="utf-8")) or {}
        # shape: { host: { allow: [regex...], deny: [regex...], max_new: int } }
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

EXPAND = load_expand()

# FAST mode overrides (quick local test)
if ENV_FAST:
    PACING["defaults"]["jitter_min"]    = 0.1
    PACING["defaults"]["jitter_max"]    = 0.3
    PACING["defaults"]["timeout_sec"]   = 12
    PACING["defaults"]["max_retries"]   = 2
    PACING["defaults"]["per_host_delay"]= 0.2
    PACING["defaults"]["backoff_base"]  = 0.3
    PACING["defaults"]["backoff_cap"]   = 1.2

def domain_cfg(host: str):
    d = PACING.get("domains", {}).get(host, {})
    # fallback to eTLD+1-ish if subdomain entry missing
    if not d and host.count(".") >= 2:
        parts = host.split(".")
        candidate = ".".join(parts[-2:])
        d = PACING.get("domains", {}).get(candidate, {})
    merged = PACING["defaults"].copy()
    headers = {}
    if isinstance(d, dict):
        for k, v in d.items():
            if k == "headers" and isinstance(v, dict):
                headers = {str(hk): str(hv) for hk, hv in v.items()}
            elif k in merged:
                merged[k] = v
    merged["headers"] = headers
    return merged

# =========================
# robots.txt (optional, light)
# =========================
def robots_disallows(host: str):
    """Return list of disallow prefixes for '*' from robots.txt, cached."""
    if host in _robots_cache:
        return _robots_cache[host]
    rules = []
    try:
        url = f"https://{host}/robots.txt"
        req = urllib.request.Request(url, headers={"User-Agent": random.choice(UAS)})
        with urllib.request.urlopen(req, timeout=10) as r:
            txt = r.read().decode("utf-8", errors="ignore")
        ua_any = False
        for line in txt.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            low = line.lower()
            if low.startswith("user-agent:"):
                ua = line.split(":", 1)[1].strip().lower()
                ua_any = (ua == "*" or "refineryplanet" in ua)
            elif ua_any and low.startswith("disallow:"):
                path = line.split(":", 1)[1].strip()
                if path:
                    rules.append(path)
            elif low.startswith("user-agent:"):
                ua_any = False
    except Exception:
        rules = []
    _robots_cache[host] = rules
    return rules

def allowed_by_robots(host: str, path: str):
    rules = robots_disallows(host)
    if not rules:
        return True
    for pref in rules:
        try:
            if path.startswith(pref):
                return False
        except Exception:
            continue
    return True

# =========================
# Helpers
# =========================
def jitter_sleep(jmin, jmax):
    time.sleep(random.uniform(max(0.0, jmin), max(jmin, jmax)))

def backoff_sleep(attempt, base, cap):
    delay = min(cap, base * (2 ** (attempt - 1)))
    time.sleep(delay + random.uniform(0, min(0.7, cap)))

def wants_json_for(url: str) -> bool:
    u = url.lower()
    return (
        "_format=json" in u or "/api/" in u or u.endswith(".json") or
        "asmx" in u or "utilservice.asmx" in u
    )

def make_request(url: str, ua: str, per_domain_headers: dict | None = None):
    # JSON-first for API-ish endpoints; HTML-first otherwise
    if wants_json_for(url):
        accept = "application/json, text/plain, */*"
    else:
        accept = "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.9,*/*;q=0.8"

    headers = {
        "User-Agent": ua,
        "Accept": accept,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Referer": url,
        "Connection": "close",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    # Hint for some ASP.NET/ASMX endpoints to return JSON
    if "asmx" in url or "utilservice.asmx" in url:
        headers["X-Requested-With"] = "XMLHttpRequest"

    if per_domain_headers:
        headers.update(per_domain_headers)

    return urllib.request.Request(url, headers=headers), headers

def decode_body(resp, raw: bytes):
    if resp.headers.get("Content-Encoding", "").lower() == "gzip":
        try:
            return gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
        except Exception:
            return raw
    return raw

def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def sniff_ext(ct: str, body: bytes) -> str:
    """Return extension based on CT with a light sniff fallback."""
    ct = (ct or "").lower()
    if ct in CT_EXT:
        return CT_EXT[ct]
    b = body.lstrip()[:1]
    if b in (b"{", b"["):
        return ".json"
    if body.lstrip()[:1] == b"<":
        return ".html"
    return ".bin"

# ---------- Expansion helpers ----------
HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.I)

def extract_links(base_url: str, html_bytes: bytes) -> list[str]:
    try:
        txt = html_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return []
    links = []
    for m in HREF_RE.finditer(txt):
        href = m.group(1).strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        links.append(urljoin(base_url, href))
    return links

def allow_expand(host: str, url: str) -> bool:
    rules = EXPAND.get(host) or EXPAND.get("www."+host) or EXPAND.get(host.split(".")[-2]+"."+host.split(".")[-1]) or {}
    allow = rules.get("allow") or []
    deny  = rules.get("deny") or []
    if deny and any(re.search(p, url, re.I) for p in deny):
        return False
    if allow:
        return any(re.search(p, url, re.I) for p in allow)
    return False

# =========================
# Load targets (strip inline notes) — supports TARGETS/TARGETS_FILE env
# =========================
def load_targets(path_str: str) -> list[str]:
    p = pathlib.Path(path_str)
    if not p.exists():
        raise SystemExit(f"ERROR: targets file not found: {path_str}")
    urls: list[str] = []
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        line = line.split(" #", 1)[0].strip()
        m = re.match(r"^\s*(https?://\S+)", line)
        if not m:
            continue
        urls.append(m.group(1).strip())
    return urls

# Seed queue
queue: list[str] = load_targets(TARGETS_FILE)
if ENV_LIMIT and ENV_LIMIT > 0:
    queue = queue[:ENV_LIMIT]
seen_urls = set(queue)

log(f"🔎 Fetch plan: {len(queue)} URLs (FAST={'on' if ENV_FAST else 'off'}; LIMIT={ENV_LIMIT or 'none'})")
log(f"   • targets file: {TARGETS_FILE}")

# =========================
# Graceful interrupt handling
# =========================
def write_summary_and_exit(code=130):
    try:
        summary = {
            "run_id": RUN_ID,
            "count": len(RESULTS),
            "ok": sum(1 for r in RESULTS if r.get("ok")),
            "skipped": sum(1 for r in RESULTS if r.get("skipped")),
            "ts": datetime.datetime.utcnow().isoformat() + "Z",
            "out_dir": str(OUT_DIR),
            "hosts": HOST_STATS,
            "defaults": PACING.get("defaults", DEFAULTS),
            "interrupted": True,
            "targets_file": TARGETS_FILE,
            "expand_rules": list(EXPAND.keys()),
        }
        (OUT_DIR / "l1_summary.json").write_text(
            json.dumps({"summary": summary, "fetched": RESULTS}, indent=2),
            encoding="utf-8",
        )
    finally:
        sys.exit(code)

def _sigint_handler(signum, frame):
    log("⏹  Received Ctrl+C — writing partial summary and exiting…")
    write_summary_and_exit(130)

signal.signal(signal.SIGINT, _sigint_handler)

# =========================
# Main loop (queue supports optional depth-1 expansion)
# =========================
last_hit = {}  # host -> last timestamp
index = 0

while queue:
    url = queue.pop(0)
    index += 1

    parsed = urlparse(url)
    host = parsed.netloc
    path = parsed.path or "/"

    cfg = domain_cfg(host)
    per_host_delay = float(cfg.get("per_host_delay", DEFAULTS["per_host_delay"]))
    jmin = float(cfg.get("jitter_min", DEFAULTS["jitter_min"]))
    jmax = float(cfg.get("jitter_max", DEFAULTS["jitter_max"]))
    timeout_sec = float(cfg.get("timeout_sec", DEFAULTS["timeout_sec"]))
    max_retries = int(cfg.get("max_retries", DEFAULTS["max_retries"]))
    backoff_base = float(cfg.get("backoff_base", DEFAULTS["backoff_base"]))
    backoff_cap = float(cfg.get("backoff_cap", DEFAULTS["backoff_cap"]))
    respect_robots = bool(cfg.get("respect_robots", DEFAULTS["respect_robots"]))
    header_overrides = cfg.get("headers", {}) or {}

    log(f"[{index}/{len(RESULTS) + len(queue) + 1}] 🌐 {host} → GET {url}")

    # robots posture
    if respect_robots and not allowed_by_robots(host, path):
        RESULTS.append({
            "index": index, "url": url, "ok": False, "skipped": True,
            "reason": "robots_disallow", "host": host,
        })
        log(f"   🚫 skipped by robots.txt")
        continue

    # Per-host pacing
    now_ts = time.time()
    if host in last_hit:
        delta = now_ts - last_hit[host]
        if delta < per_host_delay:
            time.sleep(per_host_delay - delta)

    rec = {
        "index": index,
        "url": url,
        "ok": False,
        "path": None,
        "bytes": 0,
        "content_type": None,
        "status": None,
        "fetched_at": None,
        "elapsed_sec": None,
        "error": None,
        "host": host,
        "final_url": None,
        "sha256": None,
        "headers": None,          # response headers
        "request_headers": None,  # request headers actually sent
    }

    last_err = None
    start_clock = time.time()

    for attempt in range(1, max_retries + 1):
        ua = random.choice(UAS)
        log(f"   ↳ attempt {attempt}/{max_retries} …")
        try:
            req, req_headers = make_request(url, ua, header_overrides)
            start = time.time()
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                status = resp.getcode() or 200
                raw = resp.read()
                body = decode_body(resp, raw)
                ct = resp.info().get_content_type()
                dur = time.time() - start

                ext = sniff_ext(ct, body)
                fname = f"source_{index:03d}{ext}"
                path_fp = OUT_DIR / fname
                with open(path_fp, "wb") as w:
                    w.write(body)

                hdrs = {k.lower(): v for k, v in resp.headers.items()}
                meta = {
                    "url": url,
                    "final_url": resp.geturl(),
                    "status": status,
                    "content_type": ct,
                    "bytes": len(body),
                    "elapsed_sec": round(dur, 3),
                    "fetched_at": datetime.datetime.utcnow().isoformat() + "Z",
                    "sha256": sha256_bytes(body),
                    "headers": hdrs,
                    "user_agent": ua,
                    "request_headers": req_headers,
                }
                (OUT_DIR / f"source_{index:03d}.meta.json").write_text(
                    json.dumps(meta, indent=2), encoding="utf-8"
                )

                rec.update({
                    "ok": True,
                    "path": str(path_fp),
                    "bytes": len(body),
                    "content_type": ct,
                    "status": status,
                    "fetched_at": meta["fetched_at"],
                    "elapsed_sec": round(time.time() - start_clock, 3),
                    "final_url": meta["final_url"],
                    "sha256": meta["sha256"],
                    "headers": meta["headers"],
                    "request_headers": req_headers,
                })

                log(f"   ✅ {status} {ct or 'unknown/ct'} {len(body)} bytes in {dur:.2f}s → {fname}")

                # ---- Optional depth-1 EXPANSION (HTML only) ----
                rules = EXPAND.get(host)
                if rules and ct == "text/html":
                    max_new = int(rules.get("max_new", 0) or 0)
                    if max_new > 0:
                        added = 0
                        for link in extract_links(meta["final_url"], body):
                            # same-site only
                            if urlparse(link).netloc != host:
                                continue
                            if link in seen_urls:
                                continue
                            if not allow_expand(host, link):
                                continue
                            seen_urls.add(link)
                            queue.append(link)
                            added += 1
                            if added >= max_new:
                                break
                        if added:
                            log(f"   ➕ queued {added} same-site links (expand.yaml)")

                break  # success -> exit retry loop

        except urllib.error.HTTPError as e:
            last_err = f"HTTPError {e.code}"
            if e.code in (429, 500, 502, 503, 504, 403) and attempt < max_retries:
                log(f"   ⚠️  {last_err}; backing off and retrying …")
                backoff_sleep(attempt, float(PACING["defaults"]["backoff_base"]), float(PACING["defaults"]["backoff_cap"]))
                continue
            log(f"   ❌ {last_err}")
            rec["error"] = last_err
            break

        except urllib.error.URLError as e:
            reason = getattr(e, "reason", "")
            last_err = f"URLError {reason}"
            if attempt < max_retries:
                log(f"   ⚠️  {last_err}; backing off and retrying …")
                backoff_sleep(attempt, float(PACING["defaults"]["backoff_base"]), float(PACING["defaults"]["backoff_cap"]))
                continue
            log(f"   ❌ {last_err}")
            rec["error"] = last_err
            break

        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            if attempt < max_retries:
                log(f"   ⚠️  {last_err}; backing off and retrying …")
                backoff_sleep(attempt, float(PACING["defaults"]["backoff_base"]), float(PACING["defaults"]["backoff_cap"]))
                continue
            log(f"   ❌ {last_err}")
            rec["error"] = last_err
            break

    if not rec["ok"] and not rec["error"]:
        rec["error"] = last_err or "fetch failed"

    RESULTS.append(rec)

    hs = HOST_STATS.setdefault(host, {"ok": 0, "fail": 0, "bytes": 0})
    if rec["ok"]:
        hs["ok"] += 1
        hs["bytes"] += rec.get("bytes", 0)
        log(f"[{index}] ✔ done ({rec['elapsed_sec']}s total)")
    else:
        hs["fail"] += 1
        log(f"[{index}] ✖ failed: {rec.get('error', 'unknown error')}")

    last_hit[host] = time.time()
    jitter_sleep(jmin, jmax)

# =========================
# Summary
# =========================
summary = {
    "run_id": RUN_ID,
    "count": len(RESULTS),
    "ok": sum(1 for r in RESULTS if r.get("ok")),
    "skipped": sum(1 for r in RESULTS if r.get("skipped")),
    "ts": datetime.datetime.utcnow().isoformat() + "Z",
    "out_dir": str(OUT_DIR),
    "hosts": HOST_STATS,
    "defaults": PACING.get("defaults", DEFAULTS),
    "targets_file": TARGETS_FILE,
    "expand_rules": list(EXPAND.keys()),
}

(OUT_DIR / "l1_summary.json").write_text(
    json.dumps({"summary": summary, "fetched": RESULTS}, indent=2),
    encoding="utf-8",
)
print(json.dumps(summary, indent=2))
