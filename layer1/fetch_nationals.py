#!/usr/bin/env python3
import os, json, time, urllib.request, urllib.error

OUTDIR = "layer1/snaps"
os.makedirs(OUTDIR, exist_ok=True)

MEGA = "https://www.megamillions.com/cmspages/utilservice.asmx/GetLatestDrawData"
PB   = "https://www.powerball.com/api/v1/numbers/powerball/recent?_format=json"
C4L  = "https://data.ny.gov/resource/kwxv-fwze.json"

def save(url, body, ext="json"):
    slug = url.replace("/", "_").replace(":", "_").replace("?", "_").replace("&", "_")
    base = os.path.join(OUTDIR, slug)[:200]
    # clean body file (what the extractor will read first)
    with open(base + ".body.json", "w", encoding="utf-8") as f:
        f.write(body)
    # minimal artifacts so the rest of the pipeline still works
    with open(base + ".network.json", "w", encoding="utf-8") as f:
        json.dump([{"url": url, "status": 200, "contentType": "application/json"}], f)
    with open(base + ".meta.json", "w", encoding="utf-8") as f:
        json.dump({"url": url, "final_url": url, "status": 200, "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")}, f)
    open(base + ".html", "w", encoding="utf-8").write("<html><body>synthetic</body></html>")
    print("FETCH OK", url)

def get(url, method="GET", data=None, headers=None):
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", "ignore")

def fetch_megamillions():
    hdrs = {
        "User-Agent":"Mozilla/5.0",
        "X-Requested-With":"XMLHttpRequest",
        "Content-Type":"application/json; charset=utf-8",
        "Origin":"https://www.megamillions.com",
        "Referer":"https://www.megamillions.com/",
        "Accept":"application/json, text/javascript, */*; q=0.01",
    }
    return get(MEGA, method="POST", data=b"{}", headers=hdrs)

def main():
    try: save(MEGA, fetch_megamillions())
    except Exception as e: print("MEGA ERR", e)
    try: save(PB,   get(PB))
    except Exception as e: print("PB ERR", e)
    try: save(C4L,  get(C4L))
    except Exception as e: print("C4L ERR", e)

if __name__ == "__main__":
    main()
