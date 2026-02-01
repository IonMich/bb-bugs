import os
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


def login_web(session: requests.Session, base_url: str, username: str, password: str) -> bool:
    login_url = urljoin(base_url, "/login.aspx")
    resp = session.get(login_url, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    form = None
    for candidate in soup.find_all("form"):
        names = {inp.get("name", "") for inp in candidate.find_all("input")}
        if any("txtUserName" in n for n in names) and any("txtPassword" in n for n in names):
            form = candidate
            break
    if form is None:
        form = soup.find("form")
    if not form:
        raise RuntimeError("Login form not found")

    data: dict[str, str] = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        data[name] = inp.get("value", "")

    # Fill in whichever login fields are present in the selected form.
    for key in list(data.keys()):
        if key.endswith("txtUserName") or key.endswith("txtLoginUserName"):
            data[key] = username
        if key.endswith("txtPassword") or key.endswith("txtLoginPassword"):
            data[key] = password
    data.setdefault("ctl00$cphContent$btnLoginUser", "Login")

    action = form.get("action", "/login.aspx")
    post_url = urljoin(login_url, action)
    resp2 = session.post(post_url, data=data, timeout=20)
    resp2.raise_for_status()

    # Check auth cookies first (site uses ASP.NET auth cookies).
    cookie_names = {c.name for c in session.cookies}
    if any(name.startswith(".ASPXAUTH") for name in cookie_names) or "BBUser" in cookie_names:
        return True

    if "logout.aspx" in resp2.text.lower() or "log out" in resp2.text.lower():
        return True
    # Verify by requesting the home page for a logout link.
    verify = session.get(urljoin(base_url, "/default.aspx"), timeout=20)
    if "logout.aspx" in verify.text.lower() or "log out" in verify.text.lower():
        return True
    return False


def get_login_creds() -> tuple[str | None, str | None]:
    username = os.environ.get("BB_USERNAME")
    password = os.environ.get("BB_PASSWORD") or os.environ.get("BB_SECURITY_CODE")
    return username, password
