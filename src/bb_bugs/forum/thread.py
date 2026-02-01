
from dataclasses import dataclass
import re

from bb_bugs.fetch.session import PoliteSession
from bb_bugs.parse.thread_page import parse_posts


@dataclass
class ThreadPage:
    posts: list[dict]
    raw_html: str


def fetch_thread_posts(session: PoliteSession, url: str) -> ThreadPage:
    resp = session.get(url)
    resp.encoding = resp.apparent_encoding
    posts = parse_posts(resp.text)
    if posts:
        return ThreadPage(posts=posts, raw_html=resp.text)

    # Some thread list URLs point to the last message (m=2, m=10, etc.). If that
    # page is empty (e.g., deleted posts), retry from the first message.
    if "m=" in url:
        fallback_url = re.sub(r"m=\d+", "m=1", url)
        if fallback_url != url:
            resp2 = session.get(fallback_url)
            resp2.encoding = resp2.apparent_encoding
            posts2 = parse_posts(resp2.text)
            return ThreadPage(posts=posts2, raw_html=resp2.text)

    return ThreadPage(posts=posts, raw_html=resp.text)
