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
        SELECT thread_id, title, author, url
        FROM threads
        ORDER BY rowid ASC
        LIMIT ?
        """,
        (args.limit,),
    )
    rows = cur.fetchall()
    for row in rows:
        author = row["author"] or ""
        print(f"{row['thread_id']} | {row['title']} | {author} | {row['url']}")


if __name__ == "__main__":
    main()
