
from dataclasses import dataclass
from typing import Iterable

from rich.console import Group
from rich.live import Live
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from bb_bugs.fetch.session import PoliteSession
from bb_bugs.forum.folder import fetch_folder_page, fetch_folder_page_postback
from bb_bugs.store import db as db_store


@dataclass
class FolderFetchConfig:
    folder_id: int = 2
    base_url: str = "https://www2.buzzerbeater.com"
    folder_url_template: str = "https://www2.buzzerbeater.com/community/forum/read.aspx?folder={folder_id}"
    max_threads: int | None = None


def fetch_folder(session: PoliteSession, conn, config: FolderFetchConfig, *, resume: bool = False) -> None:
    folder_url = config.folder_url_template.format(folder_id=config.folder_id)
    seen = set()
    if resume:
        resume_url = db_store.get_fetch_state(conn, f"discover:last_thread_url:{config.folder_id}")
        if resume_url:
            page = fetch_folder_page(session, resume_url)
        else:
            page = fetch_folder_page(session, folder_url)
    else:
        page = fetch_folder_page(session, folder_url)

    page_index = 0
    pages_progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}", justify="right"),
        TimeElapsedColumn(),
    )
    threads_progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}", justify="right"),
        TimeElapsedColumn(),
    )
    pages_task = pages_progress.add_task("pages", total=None)
    threads_task = threads_progress.add_task("threads", total=config.max_threads)
    with Live(Group(pages_progress, threads_progress), refresh_per_second=8):
        while True:
            page_index += 1
            threads = [t for t in page.threads if t.get("thread_id") not in seen]
            if config.max_threads is not None:
                remaining = config.max_threads - len(seen)
                if remaining <= 0:
                    break
                threads = threads[:remaining]
            for t in threads:
                if t.get("thread_id"):
                    seen.add(t["thread_id"])

            if threads:
                db_store.upsert_threads(
                    conn,
                    [
                        {
                            "thread_id": t.get("thread_id"),
                            "folder_id": config.folder_id,
                            "title": t.get("title"),
                            "author": t.get("author"),
                            "url": t.get("url"),
                            "created_at": t.get("created_at"),
                            "last_seen_at": t.get("last_seen_at"),
                        }
                        for t in threads
                    ],
                )
            pages_progress.advance(pages_task, 1)
            threads_progress.update(threads_task, advance=len(threads), total=config.max_threads)
            pages_progress.update(
                pages_task, description=f"pages (current={page_index}, total_threads={len(seen)})"
            )

            if config.max_threads is not None and len(seen) >= config.max_threads:
                break

            ctx = page.pagination_context
            if not ctx.get("has_next"):
                break

            if page.threads:
                last_url = page.threads[-1].get("url")
                if last_url:
                    db_store.set_fetch_state(
                        conn, f"discover:last_thread_url:{config.folder_id}", last_url
                    )

            data = dict(ctx.get("hidden_fields", {}))
            data["__EVENTTARGET"] = ctx.get("event_target") or ""
            data["__EVENTARGUMENT"] = ctx.get("event_argument") or ""
            page = fetch_folder_page_postback(session, ctx.get("action_url") or folder_url, data)
