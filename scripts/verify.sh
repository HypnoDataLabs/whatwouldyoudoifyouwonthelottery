#!/bin/sh
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PUB="${PUB:-$ROOT/public}"
export PUB

# 1) Core files
[ -f "$PUB/sitemap.xml" ] || { echo "FAIL: sitemap.xml missing"; exit 1; }
[ -f "$PUB/datasets/latest-draws.json" ] || { echo "FAIL: datasets JSON missing"; exit 1; }
[ -f "$PUB/datasets/latest-draws.csv" ]  || { echo "FAIL: datasets CSV missing"; exit 1; }
echo "✔ core files exist"

# 2) Parse JSON (dataset + feeds)
PUB="$PUB" python3 - <<'PY'
import os, json, sys
root = os.environ.get("PUB")
with open(os.path.join(root,"datasets","latest-draws.json"), "r", encoding="utf-8") as f:
    d = json.load(f)
assert "records" in d and isinstance(d["records"], list), "dataset.records missing or not a list"
# optional feeds
for p in ("feeds/blog.json","feeds/datasets.json","feeds/last-updated.json"):
    fp = os.path.join(root, p)
    if os.path.exists(fp):
        with open(fp, "r", encoding="utf-8") as f:
            json.load(f)
print("✔ JSON parse ok (dataset + feeds)")
PY

# 3) CSV header quick check
if head -n1 "$PUB/datasets/latest-draws.csv" | grep -Eq '(^|,)date(,|$).*game.*numbers'; then
  echo "✔ CSV header looks good"
else
  echo "WARN: CSV header unexpected (not fatal)"
fi

# 4) Article must link to dataset JSON/CSV (newest post)
ARTICLE="$(ls -1t "$PUB"/blog/*lottery-draws*.html 2>/dev/null | head -n1 || true)"
if [ -z "$ARTICLE" ]; then
  echo "FAIL: no article found in /public/blog/"
  exit 1
fi

if grep -q '/datasets/latest-draws.json' "$ARTICLE"; then
  echo "✔ article links to dataset JSON ($ARTICLE)"
else
  echo "VERIFY FAIL: article missing link to dataset JSON"
  echo "Checked: $ARTICLE"
  exit 1
fi

if grep -q '/datasets/latest-draws.csv' "$ARTICLE"; then
  echo "✔ article links to dataset CSV"
else
  echo "WARN: article missing link to dataset CSV"
fi

echo "Local verify OK"
