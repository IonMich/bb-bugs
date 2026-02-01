import requests
from bs4 import BeautifulSoup

URL = "https://www2.buzzerbeater.com/login.aspx"
HEADERS = {"User-Agent": "bb-bugs-fetcher/0.1 (+polite; contact=local)"}


def main() -> None:
    resp = requests.get(URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    form = soup.find("form")
    if not form:
        print("no form")
        return
    print("action", form.get("action"))
    inputs = form.find_all("input")
    for inp in inputs:
        name = inp.get("name")
        typ = inp.get("type")
        if not name:
            continue
        if typ == "hidden" or "pass" in name.lower() or "login" in name.lower() or "user" in name.lower():
            print("input", name, typ, (inp.get("value") or ""))


if __name__ == "__main__":
    main()
