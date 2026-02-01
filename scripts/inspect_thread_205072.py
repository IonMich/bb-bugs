import requests
from bs4 import BeautifulSoup
import re

URL = "https://www2.buzzerbeater.com/community/forum/read.aspx?thread=205072&m=1"
HEADERS = {"User-Agent": "bb-bugs-fetcher/0.1 (+polite; contact=local)"}


def main() -> None:
    resp = requests.get(URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    boxes = soup.find_all("div", id="messagebox")
    print("messagebox_count", len(boxes))
    for i, box in enumerate(boxes):
        header = box.find("div", class_="boxheader") or box
        text = header.get_text(" ", strip=True)
        author = None
        author_link = header.find("a", href=re.compile(r"/community/forum/read\.aspx\?teamid="))
        if author_link:
            author = author_link.get_text(strip=True)
        post_id = None
        for link in header.find_all("a", href=re.compile(r"read\.aspx\?thread=")):
            t = link.get_text(strip=True)
            if re.match(r"\d+\.\d+$", t):
                post_id = t
                break
        print(i, "author", author, "post_id", post_id, "header", text)


if __name__ == "__main__":
    main()
