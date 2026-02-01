from pathlib import Path
from bs4 import BeautifulSoup
from bb_bugs.parse.thread_list import parse_thread_list


def main() -> None:
    html = Path("data/folder2.html").read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "lxml")
    link = soup.find("a", id="cphContent_lbNextPage")
    print("link", link)
    if link:
        print("href", link.get("href"))
    threads, ctx = parse_thread_list(html, "https://www2.buzzerbeater.com/community/forum/read.aspx?thread=205072&m=1")
    print("threads", len(threads))
    print("has_next", ctx.get("has_next"))
    print("event_target", ctx.get("event_target"))


if __name__ == "__main__":
    main()
