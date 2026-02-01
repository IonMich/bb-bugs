import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

URL = "https://www2.buzzerbeater.com/community/forum/read.aspx?folder=2"
HEADERS = {"User-Agent": "bb-bugs-fetcher/0.1 (+polite; contact=local)"}


def extract(html: str, page_url: str):
    soup = BeautifulSoup(html, "lxml")
    boxes = soup.select("div.threadBox, div.threadBoxGold")
    ids = []
    for box in boxes:
        link = box.find("a", href=True)
        if not link:
            continue
        ids.append(link["href"])
    form = soup.find("form", id="form1") or soup.find("form")
    hidden = {}
    action = page_url
    if form:
        action = urljoin(page_url, form.get("action", ""))
        for inp in form.select("input[type=hidden][name]"):
            hidden[inp["name"]] = inp.get("value", "")
    next_link = soup.find("a", id="cphContent_lbNextPage")
    target = arg = None
    if next_link:
        m = re.search(r"__doPostBack\('([^']*)','([^']*)'\)", next_link.get("href", ""))
        if m:
            target, arg = m.group(1), m.group(2)
    return ids, action, hidden, target, arg


def main() -> None:
    resp = requests.get(URL, headers=HEADERS, timeout=20, allow_redirects=True)
    resp.raise_for_status()
    ids, action, hidden, target, arg = extract(resp.text, resp.url)
    print("page1_count", len(ids))
    print("page1_first", ids[:3])
    data = dict(hidden)
    data["__EVENTTARGET"] = target or ""
    data["__EVENTARGUMENT"] = arg or ""
    resp2 = requests.post(action, headers=HEADERS, data=data, timeout=20)
    resp2.raise_for_status()
    ids2, *_ = extract(resp2.text, resp2.url)
    print("page2_count", len(ids2))
    print("page2_first", ids2[:3])


if __name__ == "__main__":
    main()
