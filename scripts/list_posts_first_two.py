import sqlite3
from pathlib import Path


def main() -> None:
    conn = sqlite3.connect(Path("data/bbs.sqlite"))
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT thread_id, title FROM threads ORDER BY rowid ASC LIMIT 2")
    threads = cur.fetchall()
    if not threads:
        return
    thread_ids = [t["thread_id"] for t in threads]
    titles = {t["thread_id"]: t["title"] for t in threads}
    q_marks = ",".join("?" for _ in thread_ids)
    cur = conn.execute(
        f"""
        SELECT thread_id, post_id, author, posted_at
        FROM posts
        WHERE thread_id IN ({q_marks})
        ORDER BY thread_id, post_id
        """,
        thread_ids,
    )
    for row in cur.fetchall():
        title = titles.get(row["thread_id"], "")
        print(f"{row['thread_id']} | {title} | {row['post_id']} | {row['author']} | {row['posted_at']}")


if __name__ == "__main__":
    main()
