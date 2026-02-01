
import re
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup


def parse_thread_list(html: str, page_url: str) -> tuple[list[dict], dict]:
    """
    Return (threads, pagination_context).

    threads: list of dicts with keys: thread_id, title, author, replies, url
    pagination_context: dict with hidden form fields and next-page payload hints.
    """
    soup = BeautifulSoup(html, "lxml")

    threads: list[dict] = []
    for box in soup.select("div.threadBox, div.threadBoxGold"):
        link = box.find("a", href=True)
        if not link:
            continue
        full_url = urljoin(page_url, link["href"])
        parsed = urlparse(full_url)
        query = parse_qs(parsed.query)
        thread_id = query.get("thread", [None])[0]
        # Normalize thread links to the first message.
        query["m"] = ["1"]
        full_url = urlunparse(parsed._replace(query=urlencode(query, doseq=True)))
        title = link.get_text(strip=True)
        title_attr = link.get("title")
        author = None

        author_link = box.find("a", href=re.compile(r"/community/forum/read\.aspx\?teamid="))
        if author_link:
            author = author_link.get_text(strip=True)

        full_title = title_attr.strip() if title_attr else None
        if full_title and len(full_title) > len(title):
            title = full_title

        if not author and full_title and " by " in full_title:
            possible_title, possible_author = full_title.rsplit(" by ", 1)
            if len(possible_title.strip()) >= len(title):
                author = possible_author.strip()
        replies = None
        count = box.find("span", class_="allread")
        if count:
            try:
                replies = int(count.get_text(strip=True))
            except ValueError:
                replies = None
        threads.append(
            {
                "thread_id": thread_id,
                "title": title,
                "author": author,
                "replies": replies,
                "url": full_url,
            }
        )

    form = soup.find("form", id="form1") or soup.find("form")
    hidden_fields: dict[str, str] = {}
    action_url = page_url
    if form:
        action_url = urljoin(page_url, form.get("action", ""))
        for inp in form.select("input[type=hidden][name]"):
            hidden_fields[inp["name"]] = inp.get("value", "")

    next_link = soup.find("a", id="cphContent_lbNextPage")
    event_target = None
    event_argument = None
    has_next = False
    if next_link:
        href = next_link.get("href", "")
        match = re.search(r"__doPostBack\('([^']*)','([^']*)'\)", href)
        if match:
            event_target, event_argument = match.group(1), match.group(2)
            has_next = True

    pagination_context = {
        "action_url": action_url,
        "hidden_fields": hidden_fields,
        "has_next": has_next,
        "event_target": event_target,
        "event_argument": event_argument,
    }

    return threads, pagination_context
