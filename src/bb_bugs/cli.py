import argparse
from pathlib import Path

from dotenv import load_dotenv

from bb_bugs.fetch.session import FetchConfig, PoliteSession
from bb_bugs.fetch.auth import get_login_creds, login_web
from bb_bugs.jobs.fetch_folder import FolderFetchConfig, fetch_folder
from bb_bugs.jobs.fetch_threads import fetch_missing_first_posts
from bb_bugs.store.db import DbConfig, connect_db, init_db


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch BuzzerBeater bugs forum folder.")
    parser.add_argument("--folder", type=int, default=2)
    parser.add_argument("--db", type=Path, default=Path("data/bbs.sqlite"))
    parser.add_argument(
        "--phase",
        choices=["discover", "fetch"],
        default="discover",
        help="discover=collect thread IDs, fetch=fetch first posts",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-fetch threads even if posts already exist",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="fetch parallelism; use 2 to split odd/even thread_ids",
    )
    parser.add_argument(
        "--login",
        action="store_true",
        help="use BB_USERNAME/BB_PASSWORD to login before fetch phase",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="resume discovery from last stored thread URL",
    )
    parser.add_argument("--min-delay", type=float, default=2.5)
    parser.add_argument("--jitter", type=float, default=2.5)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--max-threads", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    fetch_cfg = FetchConfig(
        min_delay_s=args.min_delay,
        jitter_s=args.jitter,
        max_retries=args.max_retries,
        timeout_s=args.timeout,
    )
    session = PoliteSession(fetch_cfg)

    db_cfg = DbConfig(path=args.db)
    conn = connect_db(db_cfg)
    init_db(conn)

    if args.phase == "discover":
        folder_cfg = FolderFetchConfig(folder_id=args.folder, max_threads=args.max_threads)
        fetch_folder(session, conn, folder_cfg, resume=args.resume)
    else:
        if args.login:
            username, password = get_login_creds()
            if not username or not password:
                raise RuntimeError("Missing BB_USERNAME and BB_PASSWORD/BB_SECURITY_CODE for login")
            ok = login_web(session.session, "https://www2.buzzerbeater.com", username, password)
            if not ok:
                raise RuntimeError("Login failed")
        fetch_missing_first_posts(
            session,
            conn,
            max_threads=args.max_threads,
            force=args.force,
            concurrency=args.concurrency,
        )


if __name__ == "__main__":
    main()
