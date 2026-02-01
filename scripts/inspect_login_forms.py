import requests
from bs4 import BeautifulSoup

URL = "https://www2.buzzerbeater.com/login.aspx"


def main() -> None:
    resp = requests.get(URL, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    for i, form in enumerate(soup.find_all("form")):
        print("form", i, "action", form.get("action"))
        for inp in form.find_all("input"):
            name = inp.get("name")
            if not name:
                continue
            if any(k in name for k in ["User", "Pass", "Login", "VIEWSTATE", "EVENTVALIDATION"]):
                print(" ", name, inp.get("type"), inp.get("value", ""))


if __name__ == "__main__":
    main()
