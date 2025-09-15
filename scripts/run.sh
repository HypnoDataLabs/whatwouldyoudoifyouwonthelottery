#!/bin/sh
# Portable one-command runner: fetch -> parse -> publish -> verify -> manifest
# POSIX /bin/sh; runs from any CWD; streams live logs from Python.

set -eu

# --- Resolve repo root (run from anywhere) ---
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

# --- Basic env guards ---
if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 not found in PATH" >&2
  exit 1
fi

mkdir -p out logs tmp

START_TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
START_EPOCH="$(date +%s)"
RUN_ID="$(date -u +"%Y%m%d-%H%M%S")"
TMP_DIR="tmp/$RUN_ID"
mkdir -p "$TMP_DIR"

STATUS="ok"   # "ok" | "warn" | "error"

# Always emit a manifest on exit (even if a step fails)
cleanup() {
  END_TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  END_EPOCH="$(date +%s)"
  TOTAL_SEC=$((END_EPOCH - START_EPOCH))
  {
    echo "{"
    echo "  \"start\": \"${START_TS}\","
    echo "  \"end\": \"${END_TS}\","
    echo "  \"run_id\": \"${RUN_ID}\","
    echo "  \"status\": \"${STATUS}\","
    echo "  \"duration_sec\": ${TOTAL_SEC}"
    echo "}"
  } > "out/run.json"
}
trap cleanup EXIT

banner() {
  # usage: banner "emoji" "text"
  printf "%s %s %s\n" "$1" "$2" "$(date -u +"[%Y-%m-%d %H:%M:%SZ]")"
}

# --- Surfacing env toggles for Layer 1 ---
FAST="${FAST:-}"                 # FAST=1 for quicker pacing
LIMIT="${LIMIT:-}"               # LIMIT=5 to cap number of targets

# Choose targets list:
# - If TARGETS is set, use it.
# - Else prefer layer1/targets.txt, falling back to layer1/smoke_targets.txt if present.
if [ -n "${TARGETS:-}" ]; then
  TARGETS_FILE="$TARGETS"
elif [ -f "layer1/targets.txt" ]; then
  TARGETS_FILE="layer1/targets.txt"
elif [ -f "layer1/smoke_targets.txt" ]; then
  TARGETS_FILE="layer1/smoke_targets.txt"
else
  echo "ERROR: No targets file found (expected layer1/targets.txt or layer1/smoke_targets.txt, or set TARGETS=…)" >&2
  exit 1
fi
export TARGETS_FILE

banner "🟡" "[1/5] Layer 1 — Fetch (targets: $TARGETS_FILE)"
# Unbuffered Python so logs stream immediately
if ! FAST="$FAST" LIMIT="$LIMIT" PYTHONUNBUFFERED=1 python3 "$ROOT/layer1/fetch.py"; then
  echo "WARN: Layer 1 completed with errors (continuing)" >&2
  STATUS="warn"
fi

banner "🟠" "[2/5] Layer 2 — Parse & Classify"
if ! python3 "$ROOT/layer2/parse_and_classify.py"; then
  echo "ERROR: Layer 2 failed (stopping run)" >&2
  STATUS="error"
  exit 1
fi

banner "🟣" "[3/5] Layer 3 — Publish"
if ! python3 "$ROOT/layer3/publish.py"; then
  echo "ERROR: Layer 3 failed (stopping run)" >&2
  STATUS="error"
  exit 1
fi

banner "🟦" "[4/5] Verify"
if ! sh "$ROOT/scripts/verify.sh"; then
  echo "WARN: Verify checks failed (continuing)" >&2
  STATUS="warn"
fi

banner "🟢" "[5/5] Atomic ship"
# Static hosting serves /public; publisher already wrote final files.
echo "✅ Done."
