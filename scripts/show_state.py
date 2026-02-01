import sqlite3
from pathlib import Path


def main() -> None:
    conn = sqlite3.connect(Path("data/bbs.sqlite"))
    cur = conn.execute("SELECT key, value FROM fetch_state")
    for row in cur.fetchall():
        print(row)


if __name__ == "__main__":
    main()
