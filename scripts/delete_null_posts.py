import sqlite3
from pathlib import Path


def main() -> None:
    conn = sqlite3.connect(Path("data/bbs.sqlite"))
    cur = conn.execute("DELETE FROM posts WHERE post_id IS NULL")
    conn.commit()
    print(cur.rowcount)


if __name__ == "__main__":
    main()
