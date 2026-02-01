import requests
from bs4 import BeautifulSoup

URL = "https://www2.buzzerbeater.com/community/forum/read.aspx?thread=330024&m=1"
HEADERS = {"User-Agent": "bb-bugs-fetcher/0.1 (+polite; contact=local)"}


def main() -> None:
    resp = requests.get(URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    boxes = soup.find_all("div", id="messagebox")
    print("messagebox_count", len(boxes))
    if not boxes:
        return
    box = boxes[0]
    header = box.find("div", class_="boxheader")
    print("header_text", header.get_text(" ", strip=True) if header else None)
    if header:
        for link in header.find_all("a"):
            print("link", link.get_text(strip=True), link.get("href"))

    # also show any text nodes containing 'Date:'
    for s in header.stripped_strings if header else []:
        if "Date" in s:
            print("date_token", s)


if __name__ == "__main__":
    main()
