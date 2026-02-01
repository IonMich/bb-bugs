from datetime import datetime, timedelta, timezone
import os
import random
import time
import json
import re
import sqlite3
from contextlib import contextmanager
import subprocess
from pathlib import Path
from threading import Lock, Thread
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

DB_PATH = Path("data/bbs.sqlite")
RUNNING_JOBS: dict[str, subprocess.Popen[str]] = {}
RUNNING_JOBS_LOCK = Lock()
MAX_JUDGE_INFLIGHT = int(os.getenv("BB_JUDGE_MAX_INFLIGHT", "8"))
QUEUE_POLL_S = float(os.getenv("BB_JUDGE_QUEUE_POLL_S", "1.0"))
STUCK_JOB_S = float(os.getenv("BB_JUDGE_STUCK_S", "600"))
ALLOWED_MODELS = {
    "auto",
    "pro",
    "flash",
    "flash-lite",
    "gemini-3-pro-preview",
    "gemini-3-flash-preview",
    "gemini-2.5-pro",
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
}

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] ,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class DecisionIn(BaseModel):
    thread_id: str
    status: str
    duplicate_of: Optional[str] = None
    notes: Optional[str] = None


class BulkStatusIn(BaseModel):
    thread_ids: list[str]


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_conn_ctx() -> sqlite3.Connection:
    conn = get_conn()
    try:
        yield conn
    finally:
        conn.close()


def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS triage_decisions (
            thread_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            duplicate_of TEXT,
            notes TEXT,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS llm_judgments (
            thread_id TEXT PRIMARY KEY,
            summary TEXT,
            status_guess TEXT,
            confidence TEXT,
            evidence TEXT,
            duplicates TEXT,
            model TEXT,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS llm_jobs (
            thread_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            dry_run INTEGER DEFAULT 0,
            model TEXT,
            error TEXT,
            started_at TEXT,
            finished_at TEXT,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS llm_job_metrics (
            thread_id TEXT PRIMARY KEY,
            timings_json TEXT,
            created_at TEXT,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS llm_state (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT
        );
        """
    )
    cols = conn.execute("PRAGMA table_info(llm_jobs)").fetchall()
    col_names = {c[1] for c in cols} if cols else set()
    if "dry_run" not in col_names:
        conn.execute("ALTER TABLE llm_jobs ADD COLUMN dry_run INTEGER DEFAULT 0")
    if "model" not in col_names:
        conn.execute("ALTER TABLE llm_jobs ADD COLUMN model TEXT")
    conn.commit()


def _set_job_status(
    conn: sqlite3.Connection,
    thread_id: str,
    status: str,
    *,
    dry_run: bool | None = None,
    model: str | None = None,
    error: str | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO llm_jobs (thread_id, status, dry_run, model, error, started_at, finished_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(thread_id) DO UPDATE SET
          status=excluded.status,
          dry_run=COALESCE(excluded.dry_run, llm_jobs.dry_run),
          model=COALESCE(excluded.model, llm_jobs.model),
          error=excluded.error,
          started_at=COALESCE(excluded.started_at, llm_jobs.started_at),
          finished_at=excluded.finished_at,
          updated_at=excluded.updated_at
        """,
        (
            thread_id,
            status,
            1 if dry_run else (0 if dry_run is False else None),
            model,
            error,
            started_at,
            finished_at,
            datetime.utcnow().isoformat(),
        ),
    )
    conn.commit()


def _count_inflight(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM llm_jobs WHERE status IN ('running', 'starting')"
    ).fetchone()
    return int(row["cnt"]) if row else 0


def _summarize_llm_error(detail: str) -> str:
    text = (detail or "").lower()
    if "terminalquotaerror" in text or "exhausted your capacity" in text or "quota" in text:
        return "LLM quota exhausted; try later"
    if "timed out" in text or "timeout" in text:
        return "LLM timed out; try later"
    if "unexpected eof" in text and "bash" in text:
        return "LLM prompt shell error"
    return detail[:500]


def _is_quota_error(detail: str) -> bool:
    text = (detail or "").lower()
    return "quota" in text or "exhausted your capacity" in text or "terminalquotaerror" in text


def _parse_quota_reset(detail: str) -> str | None:
    match = re.search(r"reset after\s+((\d+h)?(\d+m)?(\d+s)?)", detail, re.IGNORECASE)
    if not match:
        return None
    token = match.group(1)
    hours = re.search(r"(\d+)h", token)
    minutes = re.search(r"(\d+)m", token)
    seconds = re.search(r"(\d+)s", token)
    total = 0
    if hours:
        total += int(hours.group(1)) * 3600
    if minutes:
        total += int(minutes.group(1)) * 60
    if seconds:
        total += int(seconds.group(1))
    if total <= 0:
        return None
    reset_at = datetime.now(timezone.utc) + timedelta(seconds=total)
    return reset_at.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_quota_reset_from_report(detail: str) -> str | None:
    match = re.search(r"Full report available at:\\s*(/\\S+\\.json)", detail)
    path = Path(match.group(1)) if match else None
    if path is None or not path.exists():
        try:
            candidates = list(Path("/tmp").glob("gemini-client-error-*.json"))
            if not candidates:
                return None
            path = max(candidates, key=lambda p: p.stat().st_mtime)
            if (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime) > 180:
                return None
        except Exception:
            return None
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return None
    message = None
    if isinstance(payload, dict):
        if isinstance(payload.get("message"), str):
            message = payload.get("message")
        elif isinstance(payload.get("message"), dict) and isinstance(payload["message"].get("message"), str):
            message = payload["message"]["message"]
        elif isinstance(payload.get("error"), dict) and isinstance(payload["error"].get("message"), str):
            message = payload["error"]["message"]
        elif isinstance(payload.get("error"), str):
            message = payload.get("error")
    if not message:
        return None
    return _parse_quota_reset(message)


def _set_state(conn: sqlite3.Connection, key: str, value: str | None) -> None:
    conn.execute(
        """
        INSERT INTO llm_state (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
        """,
        (key, value, datetime.utcnow().isoformat()),
    )
    conn.commit()


def _set_quota_state(conn: sqlite3.Connection, model: str, message: str, reset_at: str | None) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    _set_state(conn, f"quota_exhausted_at:{model}", now)
    _set_state(conn, f"quota_exhausted_message:{model}", message)
    if reset_at:
        _set_state(conn, f"quota_reset_at:{model}", reset_at)


def _clear_quota_state(conn: sqlite3.Connection, model: str) -> None:
    conn.execute(
        "DELETE FROM llm_state WHERE key IN (?, ?, ?)",
        (f"quota_exhausted_at:{model}", f"quota_exhausted_message:{model}", f"quota_reset_at:{model}"),
    )
    conn.commit()


def _claim_next_job(conn: sqlite3.Connection) -> tuple[str, bool, str | None] | None:
    row = conn.execute(
        """
        SELECT thread_id, dry_run, model
        FROM llm_jobs
        WHERE status = 'queued'
        ORDER BY updated_at ASC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None
    cur = conn.execute(
        """
        UPDATE llm_jobs
        SET status = 'starting', updated_at = ?
        WHERE thread_id = ? AND status = 'queued'
        """,
        (datetime.utcnow().isoformat(), row["thread_id"]),
    )
    conn.commit()
    if cur.rowcount == 0:
        return None
    return (row["thread_id"], bool(row["dry_run"]), row["model"])


def _dispatch_loop() -> None:
    while True:
        conn = None
        try:
            conn = get_conn()
            ensure_tables(conn)
            _cleanup_orphaned_jobs(conn)
            inflight = _count_inflight(conn)
            if inflight >= MAX_JUDGE_INFLIGHT:
                time.sleep(QUEUE_POLL_S)
                continue
            claim = _claim_next_job(conn)
            if not claim:
                time.sleep(QUEUE_POLL_S)
                continue
            thread_id, dry_run, model = claim
            worker = Thread(
                target=_run_judge_job,
                args=(thread_id,),
                kwargs={"dry_run": dry_run, "model": model},
                daemon=True,
            )
            worker.start()
        except Exception:
            time.sleep(QUEUE_POLL_S)
        finally:
            if conn is not None:
                conn.close()


def _cleanup_orphaned_jobs(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT thread_id, status, started_at, updated_at FROM llm_jobs WHERE status IN ('running', 'starting')"
    ).fetchall()
    if not rows:
        return
    now = datetime.utcnow()
    with RUNNING_JOBS_LOCK:
        active = set(RUNNING_JOBS.keys())
    for row in rows:
        thread_id = row["thread_id"]
        if thread_id in active:
            continue
        if row["status"] == "starting":
            updated_at = row["updated_at"]
            if not updated_at:
                continue
            try:
                updated = datetime.fromisoformat(updated_at)
            except ValueError:
                continue
            if (now - updated).total_seconds() >= STUCK_JOB_S:
                _set_job_status(conn, thread_id, "queued")
            continue
        started_at = row["started_at"]
        if not started_at:
            continue
        try:
            started = datetime.fromisoformat(started_at)
        except ValueError:
            continue
        if (now - started).total_seconds() >= STUCK_JOB_S:
            _set_job_status(
                conn,
                thread_id,
                "error",
                error="LLM job orphaned; no active process",
                finished_at=datetime.utcnow().isoformat(),
            )


@app.get("/queue")
def get_queue(
    status: str = "unreviewed",
    limit: int = 50,
    offset: int = 0,
    status_guess: Optional[str] = None,
    confidence: Optional[str] = None,
    q: Optional[str] = None,
    has_llm: Optional[bool] = None,
):
    with get_conn_ctx() as conn:
        ensure_tables(conn)
        q_like = f"%{q}%" if q else None
        q_is_id = q.isdigit() if q else False
        count_sql_base = """
            SELECT COUNT(*) FROM threads t
            LEFT JOIN triage_decisions d ON d.thread_id = t.thread_id
            LEFT JOIN llm_judgments lj ON lj.thread_id = t.thread_id
        """
        llm_clause = ""
        if has_llm is True:
            llm_clause = " AND lj.thread_id IS NOT NULL"
        elif has_llm is False:
            llm_clause = " AND lj.thread_id IS NULL"

        if status == "unreviewed":
            sql = """
                SELECT t.thread_id, t.title, t.url, d.status AS decision_status,
                       lj.status_guess, lj.confidence
                FROM threads t
                LEFT JOIN triage_decisions d ON d.thread_id = t.thread_id
                LEFT JOIN llm_judgments lj ON lj.thread_id = t.thread_id
                WHERE d.thread_id IS NULL
                  AND (? IS NULL OR t.title LIKE ? OR (? = 1 AND t.thread_id = ?))
                  AND (? IS NULL OR lj.status_guess = ?)
                  AND (? IS NULL OR lj.confidence = ?)
            """ + llm_clause + """
                ORDER BY CAST(t.thread_id AS INTEGER) DESC
                LIMIT ? OFFSET ?
            """
            count_sql = count_sql_base + """
                WHERE d.thread_id IS NULL
                  AND (? IS NULL OR t.title LIKE ? OR (? = 1 AND t.thread_id = ?))
                  AND (? IS NULL OR lj.status_guess = ?)
                  AND (? IS NULL OR lj.confidence = ?)
            """ + llm_clause
            rows = conn.execute(
                sql,
                (
                    q,
                    q_like,
                    1 if q_is_id else 0,
                    q,
                    status_guess,
                    status_guess,
                    confidence,
                    confidence,
                    limit,
                    offset,
                ),
            ).fetchall()
            total = conn.execute(
                count_sql,
                (q, q_like, 1 if q_is_id else 0, q, status_guess, status_guess, confidence, confidence),
            ).fetchone()[0]
        elif status == "reviewed":
            sql = """
                SELECT t.thread_id, t.title, t.url, d.status AS decision_status,
                       d.duplicate_of, d.notes, lj.status_guess, lj.confidence
                FROM threads t
                JOIN triage_decisions d ON d.thread_id = t.thread_id
                LEFT JOIN llm_judgments lj ON lj.thread_id = t.thread_id
                WHERE d.status IS NOT NULL
                  AND (? IS NULL OR t.title LIKE ? OR (? = 1 AND t.thread_id = ?))
                  AND (? IS NULL OR lj.status_guess = ?)
                  AND (? IS NULL OR lj.confidence = ?)
            """ + llm_clause + """
                ORDER BY CAST(t.thread_id AS INTEGER) DESC
                LIMIT ? OFFSET ?
            """
            count_sql = """
                SELECT COUNT(*) FROM threads t
                JOIN triage_decisions d ON d.thread_id = t.thread_id
                LEFT JOIN llm_judgments lj ON lj.thread_id = t.thread_id
                WHERE d.status IS NOT NULL
                  AND (? IS NULL OR t.title LIKE ? OR (? = 1 AND t.thread_id = ?))
                  AND (? IS NULL OR lj.status_guess = ?)
                  AND (? IS NULL OR lj.confidence = ?)
            """ + llm_clause
            rows = conn.execute(
                sql,
                (
                    q,
                    q_like,
                    1 if q_is_id else 0,
                    q,
                    status_guess,
                    status_guess,
                    confidence,
                    confidence,
                    limit,
                    offset,
                ),
            ).fetchall()
            total = conn.execute(
                count_sql,
                (q, q_like, 1 if q_is_id else 0, q, status_guess, status_guess, confidence, confidence),
            ).fetchone()[0]
        else:
            sql = """
                SELECT t.thread_id, t.title, t.url, d.status AS decision_status,
                       lj.status_guess, lj.confidence
                FROM threads t
                LEFT JOIN triage_decisions d ON d.thread_id = t.thread_id
                LEFT JOIN llm_judgments lj ON lj.thread_id = t.thread_id
                WHERE (? IS NULL OR t.title LIKE ? OR (? = 1 AND t.thread_id = ?))
                  AND (? IS NULL OR lj.status_guess = ?)
                  AND (? IS NULL OR lj.confidence = ?)
            """ + llm_clause + """
                ORDER BY CAST(t.thread_id AS INTEGER) DESC
                LIMIT ? OFFSET ?
            """
            count_sql = """
                SELECT COUNT(*) FROM threads t
                LEFT JOIN llm_judgments lj ON lj.thread_id = t.thread_id
                WHERE (? IS NULL OR t.title LIKE ? OR (? = 1 AND t.thread_id = ?))
                  AND (? IS NULL OR lj.status_guess = ?)
                  AND (? IS NULL OR lj.confidence = ?)
            """ + llm_clause
            rows = conn.execute(
                sql,
                (
                    q,
                    q_like,
                    1 if q_is_id else 0,
                    q,
                    status_guess,
                    status_guess,
                    confidence,
                    confidence,
                    limit,
                    offset,
                ),
            ).fetchall()
            total = conn.execute(
                count_sql,
                (q, q_like, 1 if q_is_id else 0, q, status_guess, status_guess, confidence, confidence),
            ).fetchone()[0]
        return {"items": [dict(r) for r in rows], "total": total}


@app.get("/thread/{thread_id}")
def get_thread(thread_id: str, max_posts: int = 11):
    with get_conn_ctx() as conn:
        ensure_tables(conn)
        thread = conn.execute(
            "SELECT thread_id, title, url FROM threads WHERE thread_id = ?",
            (thread_id,),
        ).fetchone()
        if not thread:
            raise HTTPException(status_code=404, detail="Thread not found")

        posts = conn.execute(
            """
            SELECT post_id, author, posted_at, body_text
            FROM posts
            WHERE thread_id = ?
            ORDER BY post_id
            LIMIT ?
            """,
            (thread_id, max_posts),
        ).fetchall()

        decision = conn.execute(
            "SELECT * FROM triage_decisions WHERE thread_id = ?", (thread_id,)
        ).fetchone()

        judgment = conn.execute(
            "SELECT * FROM llm_judgments WHERE thread_id = ?", (thread_id,)
        ).fetchone()

        return {
            "thread": dict(thread),
            "posts": [dict(p) for p in posts],
            "decision": dict(decision) if decision else None,
            "judgment": dict(judgment) if judgment else None,
        }


@app.post("/decision")
def upsert_decision(payload: DecisionIn):
    with get_conn_ctx() as conn:
        ensure_tables(conn)
        conn.execute(
            """
            INSERT INTO triage_decisions (thread_id, status, duplicate_of, notes, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(thread_id) DO UPDATE SET
              status=excluded.status,
              duplicate_of=excluded.duplicate_of,
              notes=excluded.notes,
              updated_at=excluded.updated_at
            """,
            (
                payload.thread_id,
                payload.status,
                payload.duplicate_of,
                payload.notes,
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
        return {"ok": True}


def _run_judge_job(thread_id: str, *, dry_run: bool = False, model: str | None = None) -> None:
    conn = get_conn()
    ensure_tables(conn)
    try:
        job = conn.execute(
            "SELECT status FROM llm_jobs WHERE thread_id = ?", (thread_id,)
        ).fetchone()
        if job and job["status"] == "cancelled":
            return
        t_run_start = time.monotonic()
        model = model or "auto"
        _set_job_status(conn, thread_id, "running", started_at=datetime.utcnow().isoformat())
        judge_mode = os.getenv("BB_JUDGE_MODE", "").lower()
        if judge_mode == "mock":
            base_sleep = float(os.getenv("BB_JUDGE_SLEEP_S", "2.0"))
            jitter = float(os.getenv("BB_JUDGE_SLEEP_JITTER_S", "0"))
            if jitter > 0:
                base_sleep += random.random() * jitter
            time.sleep(max(0.0, base_sleep))
            if not dry_run:
                payload = {
                    "thread_id": thread_id,
                    "summary": f"Mock summary for {thread_id}.",
                    "status_guess": "open",
                    "confidence": "low",
                    "evidence": [],
                    "duplicate_candidates": [],
                }
                conn.execute(
                    """
                    INSERT INTO llm_judgments (thread_id, summary, status_guess, confidence, evidence, duplicates, model, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(thread_id) DO UPDATE SET
                      summary=excluded.summary,
                      status_guess=excluded.status_guess,
                      confidence=excluded.confidence,
                      evidence=excluded.evidence,
                      duplicates=excluded.duplicates,
                      model=excluded.model,
                      created_at=excluded.created_at
                    """,
                    (
                        payload.get("thread_id"),
                        payload.get("summary"),
                        payload.get("status_guess"),
                        payload.get("confidence"),
                        json.dumps(payload.get("evidence", [])),
                        json.dumps(payload.get("duplicate_candidates", [])),
                        model,
                        datetime.utcnow().isoformat(),
                    ),
                )
                conn.commit()
            _set_job_status(conn, thread_id, "done", finished_at=datetime.utcnow().isoformat())
            return

        script = Path("scripts/llm_judge.py")
        if not script.exists():
            _set_job_status(conn, thread_id, "error", error="llm_judge.py not found")
            return
        cmd = [
            "bash",
            "-lc",
            f"scripts/uv_run.sh python {script} --thread-id {thread_id} --max-posts 11 --json-only",
        ]
        t_popen = time.monotonic()
        env = os.environ.copy()
        env["GEMINI_MODEL"] = model
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        with RUNNING_JOBS_LOCK:
            RUNNING_JOBS[thread_id] = proc
        stdout, stderr = proc.communicate()
        t_comm_done = time.monotonic()
        with RUNNING_JOBS_LOCK:
            RUNNING_JOBS.pop(thread_id, None)
        current = conn.execute(
            "SELECT status FROM llm_jobs WHERE thread_id = ?", (thread_id,)
        ).fetchone()
        if current and current["status"] == "cancelled":
            _set_job_status(conn, thread_id, "cancelled", finished_at=datetime.utcnow().isoformat())
            return
        if proc.returncode != 0:
            detail = (stderr.strip() or stdout.strip() or "LLM failed")[:2000]
            if _is_quota_error(detail):
                reset_at = _parse_quota_reset(detail) or _parse_quota_reset_from_report(detail)
                _set_quota_state(conn, model, _summarize_llm_error(detail), reset_at)
            _set_job_status(
                conn,
                thread_id,
                "error",
                error=_summarize_llm_error(detail),
                finished_at=datetime.utcnow().isoformat(),
            )
            return

        text = stdout.strip()
        try:
            payload = json.loads(text)
        except Exception:
            match = re.search(r"\{.*\}", text, re.DOTALL)
            if not match:
                detail = f"No JSON returned. Output: {text[:1000]}"
                if _is_quota_error(detail):
                    reset_at = _parse_quota_reset(detail) or _parse_quota_reset_from_report(detail)
                    _set_quota_state(conn, model, _summarize_llm_error(detail), reset_at)
                _set_job_status(
                    conn,
                    thread_id,
                    "error",
                    error=_summarize_llm_error(detail),
                    finished_at=datetime.utcnow().isoformat(),
                )
                return
            payload = json.loads(match.group(0))

        timing = payload.get("timings") if isinstance(payload, dict) else None
        timings = {
            "process_s": round(t_comm_done - t_run_start, 6),
            "spawn_s": round(t_popen - t_run_start, 6),
        }
        if isinstance(timing, dict):
            for k, v in timing.items():
                timings[k] = v
        conn.execute(
            """
            INSERT INTO llm_job_metrics (thread_id, timings_json, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(thread_id) DO UPDATE SET
              timings_json=excluded.timings_json,
              updated_at=excluded.updated_at
            """,
            (
                thread_id,
                json.dumps(timings),
                datetime.utcnow().isoformat(),
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()

        if not dry_run:
            if not payload.get("thread_id"):
                payload["thread_id"] = thread_id
            if payload.get("thread_id") != thread_id:
                payload["thread_id"] = thread_id
            conn.execute(
                """
                INSERT INTO llm_judgments (thread_id, summary, status_guess, confidence, evidence, duplicates, model, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(thread_id) DO UPDATE SET
                  summary=excluded.summary,
                  status_guess=excluded.status_guess,
                  confidence=excluded.confidence,
                  evidence=excluded.evidence,
                  duplicates=excluded.duplicates,
                  model=excluded.model,
                  created_at=excluded.created_at
                """,
                (
                    thread_id,
                    payload.get("summary"),
                    payload.get("status_guess"),
                    payload.get("confidence"),
                    json.dumps(payload.get("evidence", [])),
                    json.dumps(payload.get("duplicate_candidates", [])),
                    model,
                    datetime.utcnow().isoformat(),
                ),
            )
            conn.commit()
        _set_job_status(conn, thread_id, "done", finished_at=datetime.utcnow().isoformat())
        _clear_quota_state(conn, model)
    except Exception as exc:
        if _is_quota_error(str(exc)):
            _set_quota_state(
                conn,
                model or "auto",
                _summarize_llm_error(str(exc)),
                _parse_quota_reset(str(exc)) or _parse_quota_reset_from_report(str(exc)),
            )
        _set_job_status(
            conn,
            thread_id,
            "error",
            error=_summarize_llm_error(f"LLM job crashed: {exc}"),
            finished_at=datetime.utcnow().isoformat(),
        )
    finally:
        conn.close()


@app.post("/judge/{thread_id}", status_code=202)
def judge_thread(thread_id: str, background_tasks: BackgroundTasks, dry_run: bool = False, model: str | None = None):
    with get_conn_ctx() as conn:
        ensure_tables(conn)
        if model and model not in ALLOWED_MODELS:
            raise HTTPException(status_code=400, detail="Unsupported model")
        model = model or "auto"
        has_posts = conn.execute(
            "SELECT COUNT(*) AS cnt FROM posts WHERE thread_id = ?",
            (thread_id,),
        ).fetchone()
        if has_posts and int(has_posts["cnt"]) == 0:
            _set_job_status(
                conn,
                thread_id,
                "skipped",
                dry_run=dry_run,
                model=model,
                error="no posts for thread",
                finished_at=datetime.utcnow().isoformat(),
            )
            return {"thread_id": thread_id, "status": "skipped", "reason": "no_posts"}
        job = conn.execute(
            "SELECT status FROM llm_jobs WHERE thread_id = ?", (thread_id,)
        ).fetchone()
        if job and job["status"] in ("queued", "running"):
            return {
                "thread_id": thread_id,
                "status": job["status"],
                "max_inflight": MAX_JUDGE_INFLIGHT,
                "model": job["model"] if "model" in job.keys() else model,
            }
        inflight = _count_inflight(conn)
        _set_job_status(conn, thread_id, "queued", dry_run=dry_run, model=model)
        reason = "capacity" if inflight >= MAX_JUDGE_INFLIGHT else None
        return {
            "thread_id": thread_id,
            "status": "queued",
            "queued_reason": reason,
            "max_inflight": MAX_JUDGE_INFLIGHT,
            "model": model,
        }


@app.get("/judge/status/{thread_id}")
def judge_status(thread_id: str):
    with get_conn_ctx() as conn:
        ensure_tables(conn)
        job = conn.execute(
            "SELECT status, error, started_at, finished_at, updated_at FROM llm_jobs WHERE thread_id = ?",
            (thread_id,),
        ).fetchone()
        if job:
            return dict(job)
        judgment = conn.execute(
            "SELECT thread_id FROM llm_judgments WHERE thread_id = ?",
            (thread_id,),
        ).fetchone()
        if judgment:
            return {"status": "done"}
        return {"status": "idle"}


@app.post("/judge/status/bulk")
def judge_status_bulk(payload: BulkStatusIn):
    thread_ids = payload.thread_ids
    if not thread_ids:
        return {"items": []}
    if len(thread_ids) > 200:
        raise HTTPException(status_code=413, detail="Too many thread_ids")
    with get_conn_ctx() as conn:
        ensure_tables(conn)
        placeholders = ",".join(["?"] * len(thread_ids))
        rows = conn.execute(
            f"""
            SELECT thread_id, status, error, started_at, finished_at, updated_at
            FROM llm_jobs
            WHERE thread_id IN ({placeholders})
            """,
            thread_ids,
        ).fetchall()
        job_map = {row["thread_id"]: dict(row) for row in rows}
        missing = [tid for tid in thread_ids if tid not in job_map]
        done_set: set[str] = set()
        if missing:
            placeholders = ",".join(["?"] * len(missing))
            done_rows = conn.execute(
                f"SELECT thread_id FROM llm_judgments WHERE thread_id IN ({placeholders})",
                missing,
            ).fetchall()
            done_set = {row["thread_id"] for row in done_rows}
        items = []
        for tid in thread_ids:
            if tid in job_map:
                item = job_map[tid]
                item["thread_id"] = tid
                items.append(item)
            elif tid in done_set:
                items.append({"thread_id": tid, "status": "done"})
            else:
                items.append({"thread_id": tid, "status": "idle"})
        return {"items": items}


@app.get("/judge/metrics/{thread_id}")
def judge_metrics(thread_id: str):
    with get_conn_ctx() as conn:
        ensure_tables(conn)
        row = conn.execute(
            "SELECT timings_json FROM llm_job_metrics WHERE thread_id = ?",
            (thread_id,),
        ).fetchone()
        if not row:
            return {}
        try:
            return json.loads(row["timings_json"] or "{}")
        except Exception:
            return {}


@app.get("/judge/active")
def judge_active():
    with get_conn_ctx() as conn:
        ensure_tables(conn)
        rows = conn.execute(
            """
            SELECT thread_id, status, error, started_at, updated_at
            FROM llm_jobs
            WHERE status IN ('queued', 'running')
            ORDER BY updated_at DESC
            """
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@app.get("/judge/state")
def judge_state(model: str | None = None):
    with get_conn_ctx() as conn:
        ensure_tables(conn)
        if model:
            rows = conn.execute(
                "SELECT key, value FROM llm_state WHERE key IN (?, ?, ?)",
                (
                    f"quota_exhausted_at:{model}",
                    f"quota_exhausted_message:{model}",
                    f"quota_reset_at:{model}",
                ),
            ).fetchall()
        else:
            rows = conn.execute("SELECT key, value FROM llm_state").fetchall()
        return {"state": {row["key"]: row["value"] for row in rows} if rows else {}}


@app.post("/judge/cancel/{thread_id}")
def cancel_judge(thread_id: str):
    with get_conn_ctx() as conn:
        ensure_tables(conn)
        job = conn.execute(
            "SELECT status FROM llm_jobs WHERE thread_id = ?", (thread_id,)
        ).fetchone()
        if not job:
            return {"status": "idle"}
        if job["status"] in ("done", "error", "cancelled"):
            return {"status": job["status"]}
        _set_job_status(conn, thread_id, "cancelled", finished_at=datetime.utcnow().isoformat())
        with RUNNING_JOBS_LOCK:
            proc = RUNNING_JOBS.get(thread_id)
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
        return {"status": "cancelled"}


@app.get("/search")
def search_threads(q: str, limit: int = 20):
    with get_conn_ctx() as conn:
        ensure_tables(conn)
        q_like = f"%{q}%"
        rows = conn.execute(
            """
            SELECT t.thread_id, t.title
            FROM threads t
            WHERE t.title LIKE ?
            ORDER BY CAST(t.thread_id AS INTEGER) DESC
            LIMIT ?
            """,
            (q_like, limit),
        ).fetchall()
        return [dict(r) for r in rows]


_dispatcher = Thread(target=_dispatch_loop, daemon=True)
_dispatcher.start()
