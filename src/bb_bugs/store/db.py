
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass
class DbConfig:
    path: Path


def connect_db(config: DbConfig) -> sqlite3.Connection:
    conn = sqlite3.connect(config.path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS threads (
            thread_id TEXT PRIMARY KEY,
            folder_id INTEGER NOT NULL,
            title TEXT,
            author TEXT,
            url TEXT,
            created_at TEXT,
            last_seen_at TEXT
        );

        CREATE TABLE IF NOT EXISTS posts (
            post_id TEXT PRIMARY KEY,
            thread_id TEXT NOT NULL,
            author TEXT,
            posted_at TEXT,
            body_html TEXT,
            body_text TEXT,
            is_first INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (thread_id) REFERENCES threads(thread_id)
        );

        CREATE TABLE IF NOT EXISTS fetch_state (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """
    )
    _ensure_columns(conn, "threads", {"url": "TEXT"})
    conn.commit()


def _normalize_thread_url(url: str | None) -> str | None:
    if not url:
        return url
    if "m=" not in url:
        return url
    from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

    parsed = urlparse(url)
    q = parse_qs(parsed.query)
    q["m"] = ["1"]
    return urlunparse(parsed._replace(query=urlencode(q, doseq=True)))


def upsert_threads(conn: sqlite3.Connection, rows: Iterable[dict]) -> None:
    normalized_rows = []
    for row in rows:
        if "url" in row:
            row = dict(row)
            row["url"] = _normalize_thread_url(row.get("url"))
        normalized_rows.append(row)
    conn.executemany(
        """
        INSERT INTO threads (thread_id, folder_id, title, author, url, created_at, last_seen_at)
        VALUES (:thread_id, :folder_id, :title, :author, :url, :created_at, :last_seen_at)
        ON CONFLICT(thread_id) DO UPDATE SET
            title=excluded.title,
            author=excluded.author,
            url=excluded.url,
            created_at=excluded.created_at,
            last_seen_at=excluded.last_seen_at
        """,
        normalized_rows,
    )
    conn.commit()


def upsert_post(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute(
        """
        INSERT INTO posts (post_id, thread_id, author, posted_at, body_html, body_text, is_first)
        VALUES (:post_id, :thread_id, :author, :posted_at, :body_html, :body_text, :is_first)
        ON CONFLICT(post_id) DO UPDATE SET
            author=excluded.author,
            posted_at=excluded.posted_at,
            body_html=excluded.body_html,
            body_text=excluded.body_text,
            is_first=excluded.is_first
        """,
        row,
    )
    conn.commit()


def get_fetch_state(conn: sqlite3.Connection, key: str) -> str | None:
    cur = conn.execute("SELECT value FROM fetch_state WHERE key = ?", (key,))
    row = cur.fetchone()
    return row["value"] if row else None


def set_fetch_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO fetch_state (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    conn.commit()


def list_threads_missing_first_post(conn: sqlite3.Connection, limit: int | None = None) -> list[sqlite3.Row]:
    sql = """
        SELECT t.thread_id, t.url
        FROM threads t
        WHERE t.url IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM posts p WHERE p.thread_id = t.thread_id AND p.is_first = 1
          )
        ORDER BY CAST(t.thread_id AS INTEGER) DESC
    """
    if limit is not None:
        sql += " LIMIT ?"
        cur = conn.execute(sql, (limit,))
    else:
        cur = conn.execute(sql)
    return list(cur.fetchall())


def list_threads_with_urls(conn: sqlite3.Connection, limit: int | None = None) -> list[sqlite3.Row]:
    sql = """
        SELECT t.thread_id, t.url
        FROM threads t
        WHERE t.url IS NOT NULL
        ORDER BY CAST(t.thread_id AS INTEGER) DESC
    """
    if limit is not None:
        sql += " LIMIT ?"
        cur = conn.execute(sql, (limit,))
    else:
        cur = conn.execute(sql)
    return list(cur.fetchall())


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    for name, col_type in columns.items():
        if name in existing:
            continue
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {col_type}")
