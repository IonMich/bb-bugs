import sqlite3
from pathlib import Path


def main() -> None:
    conn = sqlite3.connect(Path("data/bbs.sqlite"))
    conn.row_factory = sqlite3.Row

    total_threads = conn.execute("SELECT COUNT(*) FROM threads").fetchone()[0]
    total_posts = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    threads_with_posts = conn.execute(
        "SELECT COUNT(DISTINCT thread_id) FROM posts"
    ).fetchone()[0]

    newest_threads = conn.execute(
        """
        SELECT thread_id, title
        FROM threads
        ORDER BY rowid ASC
        LIMIT 5
        """
    ).fetchall()

    latest_posts = conn.execute(
        """
        SELECT p.thread_id, t.title, p.post_id, p.author, p.posted_at
        FROM posts p
        LEFT JOIN threads t ON t.thread_id = p.thread_id
        ORDER BY p.rowid DESC
        LIMIT 5
        """
    ).fetchall()

    print("threads_total", total_threads)
    print("posts_total", total_posts)
    print("threads_with_posts", threads_with_posts)
    print("newest_threads")
    for row in newest_threads:
        print(f"- {row['thread_id']} | {row['title']}")
    print("latest_posts")
    for row in latest_posts:
        print(
            f"- {row['thread_id']} | {row['title']} | {row['post_id']} | {row['author']} | {row['posted_at']}"
        )


if __name__ == "__main__":
    main()
