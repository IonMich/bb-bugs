import sqlite3
from pathlib import Path


def main() -> None:
    db = Path("data/bbs.sqlite")
    conn = sqlite3.connect(db)
    cur = conn.execute("SELECT COUNT(*) FROM threads")
    print(cur.fetchone()[0])


if __name__ == "__main__":
    main()
