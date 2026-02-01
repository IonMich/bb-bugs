import argparse
import sqlite3
from pathlib import Path

from dotenv import load_dotenv

from bb_bugs.fetch.auth import get_login_creds, login_web
from bb_bugs.fetch.session import FetchConfig, PoliteSession
from bb_bugs.forum.thread import fetch_thread_posts
from bb_bugs.store import db as db_store


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=Path("data/bbs.sqlite"))
    parser.add_argument("--thread-id", action="append", required=True)
    parser.add_argument("--login", action="store_true")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    fetch_cfg = FetchConfig()
    session = PoliteSession(fetch_cfg)
    conn = db_store.connect_db(db_store.DbConfig(path=args.db))

    if args.login:
        username, password = get_login_creds()
        if not username or not password:
            raise RuntimeError("Missing BB_USERNAME and BB_PASSWORD/BB_SECURITY_CODE for login")
        ok = login_web(session.session, "https://www2.buzzerbeater.com", username, password)
        if not ok:
            raise RuntimeError("Login failed")

    for thread_id in args.thread_id:
        cur = conn.execute("SELECT url FROM threads WHERE thread_id = ?", (thread_id,))
        row = cur.fetchone()
        if not row or not row[0]:
            continue
        thread_url = row[0]
        thread_page = fetch_thread_posts(session, thread_url)
        if not thread_page.posts:
            continue
        if thread_page.posts and not thread_page.posts[0].get("post_id"):
            thread_page.posts[0]["post_id"] = f"{thread_id}.1"
        for index, post in enumerate(thread_page.posts):
            if not post.get("post_id"):
                continue
            post_row = {
                "post_id": post.get("post_id"),
                "thread_id": thread_id,
                "author": post.get("author"),
                "posted_at": post.get("posted_at"),
                "body_html": post.get("body_html"),
                "body_text": post.get("body_text"),
                "is_first": 1 if index == 0 else 0,
            }
            db_store.upsert_post(conn, post_row)


if __name__ == "__main__":
    main()
