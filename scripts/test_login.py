from dotenv import load_dotenv
import os
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup


def pick_form(soup):
    form = None
    for candidate in soup.find_all("form"):
        names = {inp.get("name", "") for inp in candidate.find_all("input")}
        if any("txtUserName" in n for n in names) and any("txtPassword" in n for n in names):
            form = candidate
            break
    if form is None:
        form = soup.find("form")
    return form


def main() -> None:
    load_dotenv()
    username = os.getenv("BB_USERNAME")
    password = os.getenv("BB_PASSWORD") or os.getenv("BB_SECURITY_CODE")
    if not username or not password:
        print("missing creds")
        return
    base = "https://www2.buzzerbeater.com"
    login_url = urljoin(base, "/login.aspx")
    session = requests.Session()

    r1 = session.get(login_url, timeout=20)
    r1.raise_for_status()
    soup = BeautifulSoup(r1.text, "lxml")
    form = pick_form(soup)
    if not form:
        print("no form")
        return
    data = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        data[name] = inp.get("value", "")
    for key in list(data.keys()):
        if key.endswith("txtUserName") or key.endswith("txtLoginUserName"):
            data[key] = username
        if key.endswith("txtPassword") or key.endswith("txtLoginPassword"):
            data[key] = password
    data.setdefault("ctl00$cphContent$btnLoginUser", "Login")

    action = form.get("action", "/login.aspx")
    post_url = urljoin(login_url, action)
    r2 = session.post(post_url, data=data, timeout=20)
    r2.raise_for_status()

    print("cookies", [c.name for c in session.cookies])

    # Fetch a thread to verify visibility
    thread_url = urljoin(base, "/community/forum/read.aspx?thread=205072&m=1")
    r3 = session.get(thread_url, timeout=20)
    r3.raise_for_status()
    soup3 = BeautifulSoup(r3.text, "lxml")
    boxes = soup3.find_all("div", id="messagebox")
    print("messagebox_count", len(boxes))


if __name__ == "__main__":
    main()
