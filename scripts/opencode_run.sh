#!/usr/bin/env bash
set -euo pipefail

PROMPT=${1:-"Say: OK"}
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export XDG_DATA_HOME="${ROOT_DIR}/.opencode-data"
mkdir -p "$XDG_DATA_HOME"

if [[ "${OPENCODE_FULL:-}" == "1" ]]; then
  exec opencode run "$PROMPT"
fi

TMP_OUT=$(mktemp)
opencode run "$PROMPT" >"$TMP_OUT" 2>/dev/null || true
awk 'NF{line=$0} END{if(line) print line}' "$TMP_OUT"
rm -f "$TMP_OUT"
