#!/usr/bin/env bash
set -euo pipefail

PROMPT=${1:-"Say: OK"}
TMP_OUT=$(mktemp)

codex exec --skip-git-repo-check "$PROMPT" >"$TMP_OUT" 2>/dev/null || true
awk 'NF{line=$0} END{if(line) print line}' "$TMP_OUT"
rm -f "$TMP_OUT"
