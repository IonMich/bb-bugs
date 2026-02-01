import argparse
import sqlite3
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=Path("data/bbs.sqlite"))
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT p.thread_id, t.title, p.post_id, p.author, p.posted_at
        FROM posts p
        LEFT JOIN threads t ON t.thread_id = p.thread_id
        ORDER BY p.rowid ASC
        LIMIT ?
        """,
        (args.limit,),
    )
    rows = cur.fetchall()
    for row in rows:
        title = row["title"] or ""
        print(f"{row['thread_id']} | {title} | {row['post_id']} | {row['author']} | {row['posted_at']}")


if __name__ == "__main__":
    main()
