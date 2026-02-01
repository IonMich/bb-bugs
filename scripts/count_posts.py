import sqlite3
from pathlib import Path


def main() -> None:
    conn = sqlite3.connect(Path("data/bbs.sqlite"))
    cur = conn.execute("SELECT COUNT(*) FROM posts")
    print(cur.fetchone()[0])


if __name__ == "__main__":
    main()
