#!/usr/bin/env bash
set -euo pipefail

MODEL="${GEMINI_MODEL:-auto}"
if [[ $# -gt 0 ]]; then
  PROMPT="$1"
else
  if [[ -t 0 ]]; then
    PROMPT="Say: OK"
  else
    PROMPT="$(cat)"
  fi
fi

exec gemini --model "$MODEL" -p "$PROMPT"
