from bb_bugs.fetch.session import PoliteSession
from bb_bugs.forum.thread import fetch_thread_posts
import queue
import threading
from typing import Iterable

from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from bb_bugs.store import db as db_store


def fetch_missing_first_posts(
    session: PoliteSession,
    conn,
    max_threads: int | None = None,
    *,
    force: bool = False,
    concurrency: int = 1,
) -> None:
    if force:
        rows = db_store.list_threads_with_urls(conn, limit=max_threads)
    else:
        rows = db_store.list_threads_missing_first_post(conn, limit=max_threads)
    total = len(rows)
    if concurrency < 1:
        concurrency = 1

    def _make_worker_session() -> PoliteSession:
        worker = PoliteSession(session.config, limiter=session.limiter)
        worker.session.cookies.update(session.session.cookies)
        return worker

    def _parity_bucket(thread_id: str) -> int:
        try:
            return int(thread_id) % 2
        except ValueError:
            return hash(thread_id) % 2

    def _fetch_posts(worker_session: PoliteSession, row: dict) -> tuple[str, list[dict]]:
        thread_url = row["url"]
        if not thread_url:
            return row["thread_id"], []
        thread_page = fetch_thread_posts(worker_session, thread_url)
        if not thread_page.posts:
            return row["thread_id"], []
        if thread_page.posts and not thread_page.posts[0].get("post_id"):
            thread_page.posts[0]["post_id"] = f"{row['thread_id']}.1"
        return row["thread_id"], thread_page.posts

    def _worker(rows_subset: Iterable[dict], result_queue: queue.Queue) -> None:
        worker_session = _make_worker_session()
        for row in rows_subset:
            result_queue.put(_fetch_posts(worker_session, row))
        result_queue.put(None)

    result_queue: queue.Queue = queue.Queue()
    if concurrency == 1:
        _worker(rows, result_queue)
    else:
        bucketed = {0: [], 1: []}
        for row in rows:
            bucketed[_parity_bucket(row["thread_id"])].append(row)
        threads = []
        for bucket in (0, 1):
            t = threading.Thread(target=_worker, args=(bucketed[bucket], result_queue))
            t.daemon = True
            t.start()
            threads.append(t)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}", justify="right"),
        TimeElapsedColumn(),
    ) as progress:
        task = progress.add_task("threads", total=total)
        done_workers = 0
        while done_workers < (1 if concurrency == 1 else 2):
            item = result_queue.get()
            if item is None:
                done_workers += 1
                continue
            thread_id, posts = item
            for index, post in enumerate(posts):
                post_id = post.get("post_id")
                if not post_id:
                    continue
                post_row = {
                    "post_id": post_id,
                    "thread_id": thread_id,
                    "author": post.get("author"),
                    "posted_at": post.get("posted_at"),
                    "body_html": post.get("body_html"),
                    "body_text": post.get("body_text"),
                    "is_first": 1 if index == 0 else 0,
                }
                db_store.upsert_post(conn, post_row)
            progress.update(
                task,
                advance=1,
                description=f"threads (last={thread_id} posts={len(posts)})",
            )
