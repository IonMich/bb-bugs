#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <thread_id> [thread_id ...] | --random"
  exit 1
fi

if [[ "$1" == "--random" ]]; then
  TID=$(scripts/uv_run.sh python - <<'PY'
import sqlite3
import random
from pathlib import Path

conn = sqlite3.connect(Path("data/bbs.sqlite"))
cur = conn.execute(
    "SELECT thread_id FROM posts GROUP BY thread_id HAVING COUNT(*) > 0"
)
rows = [r[0] for r in cur.fetchall()]
if not rows:
    raise SystemExit(1)
print(random.choice(rows))
PY
)
  scripts/uv_run.sh python scripts/llm_judge.py --thread-id "$TID" --max-posts 11
  exit 0
fi

for tid in "$@"; do
  scripts/uv_run.sh python scripts/llm_judge.py --thread-id "$tid" --max-posts 11
done
