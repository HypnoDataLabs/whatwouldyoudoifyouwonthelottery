#!/usr/bin/env bash
set -euo pipefail

INFILE="${1:-layer1/targets.txt}"
OUTDIR="layer1/verify_out"
mkdir -p "$OUTDIR"
JSON_OK="$OUTDIR/targets.json.txt"
HTML_OK="$OUTDIR/targets.html.txt"
NEEDS_VISION="$OUTDIR/targets.needs_vision.txt"
FAILS="$OUTDIR/targets.fail.txt"
META="$OUTDIR/report.ndjson"

: >"$JSON_OK"; : >"$HTML_OK"; : >"$NEEDS_VISION"; : >"$FAILS"; : >"$META"

is_recent_date() {
  # quick-and-dirty: accept Month Day[, Year], or mm/dd/yy, or weekday forms
  # we just ensure at least *some* date-like token is present;
  # Layer 2 will enforce the strict ≤14-day rule.
  grep -Eiq \
    "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[[:space:]]+[0-9]{1,2}([,][[:space:]]*[0-9]{2,4})?|(Sun|Mon|Tue|Wed|Thu|Fri|Sat)|[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}" \
    "$1"
}

contains_numbers_keywords() {
  grep -Eiq "(Powerball|Mega[[:space:]]?Millions|Lucky[[:space:]]for[[:space:]]Life|Cash4?Life|Lotto[[:space:]]America|Winning[[:space:]]Numbers|Results)" "$1" && \
  grep -Eiq "\b([0-9]{1,2})([ ,•\-–—]+){4,}" "$1"
}

while IFS= read -r URL; do
  [[ -z "${URL// }" ]] && continue
  TMPDIR="$(mktemp -d)"
  BODY="$TMPDIR/body.bin"
  HDRS="$TMPDIR/headers.txt"

  CODE_CT_URL="$(curl -sS -L "$URL" -o "$BODY" -D "$HDRS" -w '%{http_code}\t%{content_type}\t%{url_effective}\n' --max-time 25)"
  CODE="$(echo "$CODE_CT_URL" | cut -f1)"
  CTYPE_RAW="$(echo "$CODE_CT_URL" | cut -f2)"
  FINAL_URL="$(echo "$CODE_CT_URL" | cut -f3)"
  CTYPE="$(echo "$CTYPE_RAW" | tr '[:upper:]' '[:lower:]')"

  SIZE="$(wc -c <"$BODY" | tr -d ' ')"

  STATUS="unknown"
  CLASS="unknown"

  if [[ "$CODE" != "200" || "$SIZE" -lt 100 ]]; then
    STATUS="bad_http_or_empty"
    echo "$URL" >>"$FAILS"
  else
    # Peek at body start to detect JSON even if content-type is wrong
    HEAD10="$(head -c 10 "$BODY" | tr -d '\r\n\t ')"
    if [[ "$CTYPE" == application/json* || "$HEAD10" =~ ^[\{\[] ]]; then
      STATUS="ok"
      CLASS="json"
      echo "$URL" >>"$JSON_OK"
    elif [[ "$CTYPE" == text/html* || "$CTYPE" == text/* || "$CTYPE" == */html* ]]; then
      # Convert to UTF-8 text for regex checks
      TEXT="$TMPDIR/body.txt"
      # naive conversion (iconv may fail silently; that's fine)
      iconv -f UTF-8 -t UTF-8 "$BODY" -o "$TEXT" 2>/dev/null || cp "$BODY" "$TEXT"
      if contains_numbers_keywords "$TEXT" && is_recent_date "$TEXT"; then
        STATUS="ok"
        CLASS="html"
        echo "$URL" >>"$HTML_OK"
      else
        STATUS="html_needs_vision_or_deeper_parse"
        CLASS="needs_vision"
        echo "$URL" >>"$NEEDS_VISION"
      fi
    else
      STATUS="unsupported_content_type"
      echo "$URL" >>"$NEEDS_VISION"
      CLASS="needs_vision"
    fi
  fi

  jq -n \
    --arg url "$URL" \
    --arg final_url "$FINAL_URL" \
    --arg code "$CODE" \
    --arg ctype "$CTYPE_RAW" \
    --arg size "$SIZE" \
    --arg status "$STATUS" \
    --arg class "$CLASS" \
    '{url:$url, final_url:$final_url, http_code:$code, content_type:$ctype, bytes:($size|tonumber), status:$status, class:$class, checked_at:(now|tojson)}' \
    >>"$META" 2>/dev/null || echo "{\"url\":\"$URL\",\"final_url\":\"$FINAL_URL\",\"http_code\":\"$CODE\",\"content_type\":\"$CTYPE_RAW\",\"bytes\":$SIZE,\"status\":\"$STATUS\",\"class\":\"$CLASS\"}" >>"$META"

  rm -rf "$TMPDIR"
done <"$INFILE"

echo "JSON ok:   $(wc -l <"$JSON_OK" | tr -d ' ')"
echo "HTML ok:   $(wc -l <"$HTML_OK" | tr -d ' ')"
echo "Needs vis: $(wc -l <"$NEEDS_VISION" | tr -d ' ')"
echo "Fails:     $(wc -l <"$FAILS" | tr -d ' ')"
echo "→ Details: $META"
