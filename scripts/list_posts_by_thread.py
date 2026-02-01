import argparse
import sqlite3
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=Path("data/bbs.sqlite"))
    parser.add_argument("--thread-id", required=True)
    parser.add_argument(
        "--verbosity",
        type=int,
        choices=[1, 2, 3],
        default=2,
        help="1=summary, 2=with body text, 3=with body text + html",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT p.thread_id, t.title, p.post_id, p.author, p.posted_at, p.body_text, p.body_html
        FROM posts p
        LEFT JOIN threads t ON t.thread_id = p.thread_id
        WHERE p.thread_id = ?
        ORDER BY p.post_id
        """,
        (args.thread_id,),
    )
    rows = cur.fetchall()
    for row in rows:
        body_text = row["body_text"] or ""
        body_html = row["body_html"] or ""
        if args.verbosity == 1:
            print(
                f"{row['thread_id']} | {row['title']} | {row['post_id']} | {row['author']} | {row['posted_at']}"
            )
            continue

        print(f"thread_id: {row['thread_id']}")
        print(f"title: {row['title']}")
        print(f"post_id: {row['post_id']}")
        print(f"author: {row['author']}")
        print(f"posted_at: {row['posted_at']}")
        print(f"body_text_len: {len(body_text)}")
        if args.verbosity >= 2:
            print("body_text:")
            print(body_text)
        if args.verbosity >= 3:
            print(f"body_html_len: {len(body_html)}")
            print("body_html:")
            print(body_html)
        print("-" * 60)


if __name__ == "__main__":
    main()
